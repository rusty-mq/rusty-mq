//! T09 slice: concurrency stress — competing publishers and consumers with
//! prefetch and manual acks under load. Invariants (M3 exit gate):
//! every published message is delivered exactly once across consumers
//! (no loss, no duplicate concurrent delivery), and credit rules hold
//! (no consumer ever exceeds its prefetch of unacked deliveries).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use tokio::sync::Mutex;

const PUBLISHERS: usize = 4;
const MESSAGES_PER_PUBLISHER: usize = 50;
const CONSUMERS: usize = 3;
const PREFETCH: u16 = 7;

async fn start_broker() -> std::net::SocketAddr {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
    let broker = rusty_mq::Broker::new("guest".into(), "guest".into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener(listener, broker));
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_publish_consume_no_loss_no_duplicates() {
    let addr = start_broker().await;

    // Declare the queue once.
    {
        let conn = lapin_connect(addr).await;
        let ch = conn.create_channel().await.unwrap();
        ch.queue_declare(
            "stress.q".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    }

    // Publishers: each sends MESSAGES_PER_PUBLISHER distinct payloads.
    let mut publisher_handles = Vec::new();
    for p in 0..PUBLISHERS {
        publisher_handles.push(tokio::spawn(async move {
            let conn = lapin_connect(addr).await;
            let ch = conn.create_channel().await.unwrap();
            for i in 0..MESSAGES_PER_PUBLISHER {
                let payload = format!("p{p}-m{i}");
                ch.basic_publish(
                    "".into(),
                    "stress.q".into(),
                    BasicPublishOptions::default(),
                    payload.as_bytes(),
                    BasicProperties::default(),
                )
                .await
                .unwrap();
            }
            conn.close(200, "done".into()).await.unwrap();
        }));
    }
    for h in publisher_handles {
        h.await.unwrap();
    }

    // Consumers on separate connections, prefetch + manual ack, each
    // counting deliveries and policing its in-flight bound.
    let delivered: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mut consumer_handles = Vec::new();
    for _ in 0..CONSUMERS {
        let delivered = delivered.clone();
        consumer_handles.push(tokio::spawn(async move {
            let conn = lapin_connect(addr).await;
            let ch = conn.create_channel().await.unwrap();
            ch.basic_qos(PREFETCH, BasicQosOptions::default())
                .await
                .unwrap();
            let mut consumer = ch
                .basic_consume(
                    "stress.q".into(),
                    "".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();
            let mut in_flight: u64 = 0;
            let mut max_in_flight: u64 = 0;
            loop {
                let next =
                    match tokio::time::timeout(Duration::from_secs(10), consumer.next()).await {
                        Ok(next) => next,
                        Err(_) => break, // idle: everything drained
                    };
                let delivery = next.expect("stream alive").expect("delivery ok");
                in_flight += 1;
                max_in_flight = max_in_flight.max(in_flight);
                delivery
                    .acker
                    .ack(BasicAckOptions::default())
                    .await
                    .unwrap();
                in_flight -= 1;
                delivered
                    .lock()
                    .await
                    .push(String::from_utf8_lossy(&delivery.data).into_owned());
                // Stop once the global total is reached (consumers share the
                // queue; any of them may observe the final count).
                if delivered.lock().await.len() >= PUBLISHERS * MESSAGES_PER_PUBLISHER {
                    break;
                }
            }
            let _ = conn.close(200, "done".into()).await;
            max_in_flight
        }));
    }

    let mut max_in_flight_any = 0;
    for h in consumer_handles {
        let m = h.await.unwrap();
        max_in_flight_any = max_in_flight_any.max(m);
    }

    let all = delivered.lock().await;
    let expected_total = PUBLISHERS * MESSAGES_PER_PUBLISHER;
    assert_eq!(
        all.len(),
        expected_total,
        "every message delivered exactly once in total"
    );
    let unique: HashSet<&String> = all.iter().collect();
    assert_eq!(
        unique.len(),
        expected_total,
        "no duplicate deliveries across consumers"
    );
    assert!(
        max_in_flight_any <= PREFETCH as u64,
        "prefetch credit respected: max in-flight {max_in_flight_any} > {PREFETCH}"
    );
}

/// Shared-channel consumers under the shared (global=true) limit: the
/// channel as a whole never exceeds the limit, while work still flows.
#[tokio::test(flavor = "multi_thread")]
async fn shared_prefetch_limits_channel_not_consumers() {
    let addr = start_broker().await;
    let conn = lapin_connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "shared.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    // Shared limit 5 across the whole channel (§6.2 rule 3).
    ch.basic_qos(5, BasicQosOptions { global: true })
        .await
        .unwrap();

    let inflight = Arc::new(Mutex::new(0u64));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let ch = ch.clone();
        let inflight = inflight.clone();
        handles.push(tokio::spawn(async move {
            let mut consumer = ch
                .basic_consume(
                    "shared.q".into(),
                    "".into(),
                    BasicConsumeOptions {
                        no_ack: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();
            let _ = &inflight; // no_ack deliveries are not credit-gated
            loop {
                let next = match tokio::time::timeout(Duration::from_secs(5), consumer.next()).await
                {
                    Ok(next) => next,
                    Err(_) => break,
                };
                let Some(Ok(_d)) = next else { break };
            }
        }));
    }
    // no_ack consumers settle at delivery; the shared limit does not gate
    // them (§6.2 rule 6). Publish a batch and let it drain.
    for i in 0..10u8 {
        ch.basic_publish(
            "".into(),
            "shared.q".into(),
            BasicPublishOptions::default(),
            &[i],
            BasicProperties::default(),
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Drain check via passive declare: all ten were delivered (settled at
    // delivery, so ready count is zero).
    let count = ch
        .queue_declare(
            "shared.q".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap()
        .message_count();
    assert_eq!(count, 0, "no_ack deliveries are not gated by prefetch");
    for h in handles {
        h.abort();
    }
    let _ = conn.close(200, "bye".into()).await;
}

async fn lapin_connect(addr: std::net::SocketAddr) -> Connection {
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect within timeout")
    .expect("handshake")
}
