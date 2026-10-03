//! T24 slow-consumer posture: a stalled consumer's outstanding deliveries
//! are bounded by its prefetch credit (bounded memory), while OTHER
//! connections on the same broker keep making progress — confirms keep
//! flowing, control operations succeed, nothing is lost, and accounting
//! stays exact through the stall (mixed-use connections).

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

async fn connect(addr: std::net::SocketAddr) -> Connection {
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect within timeout")
    .expect("handshake")
}

async fn next_delivery(consumer: &mut lapin::Consumer) -> Option<lapin::message::Delivery> {
    match tokio::time::timeout(Duration::from_secs(5), consumer.next()).await {
        Ok(Some(Ok(d))) => Some(d),
        Ok(Some(Err(e))) => panic!("delivery error: {e}"),
        Ok(None) => panic!("consumer stream ended"),
        Err(_) => None, // no delivery within the window
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stalled_consumer_bounds_credit_and_never_blocks_others() {
    let addr = start_broker().await;
    let slow = connect(addr).await;
    let mixed = connect(addr).await;

    let slow_ch = slow.create_channel().await.unwrap();
    // Durable profile: shared transient queues are rejected by the frozen
    // profile (ADR-0005); memory mode makes no persistence claim.
    slow_ch
        .queue_declare(
            "t24.slow".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    slow_ch
        .basic_qos(10, BasicQosOptions::default())
        .await
        .unwrap();
    let mut consumer = slow_ch
        .basic_consume(
            "t24.slow".into(),
            "stalled".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Seed 20 messages BEFORE the consumer stalls at its credit bound.
    let mixed_ch = mixed.create_channel().await.unwrap();
    mixed_ch
        .queue_declare(
            "t24.fast".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    mixed_ch
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    for i in 0..20u8 {
        let confirm = mixed_ch
            .basic_publish(
                "".into(),
                "t24.slow".into(),
                BasicPublishOptions::default(),
                &[i],
                BasicProperties::default(),
            )
            .await
            .unwrap();
        confirm.await.expect("confirm while consumer stalls");
    }

    // The stalled consumer receives EXACTLY its credit (10) and no more —
    // outstanding deliveries are bounded by prefetch, never by backlog.
    let mut stalled: Vec<lapin::message::Delivery> = Vec::new();
    while let Some(d) = next_delivery(&mut consumer).await {
        stalled.push(d);
    }
    assert_eq!(stalled.len(), 10, "credit bound holds during the stall");

    // Mixed-use progress while the consumer holds all 10 unacked: another
    // 20 confirmed publishes to a DIFFERENT queue plus a control op must
    // complete promptly (no head-of-line blocking across connections).
    for i in 0..20u8 {
        let confirm = mixed_ch
            .basic_publish(
                "".into(),
                "t24.fast".into(),
                BasicPublishOptions::default(),
                &[i],
                BasicProperties::default(),
            )
            .await
            .unwrap();
        confirm.await.expect("fast-path confirm while stalled");
    }
    mixed_ch
        .queue_declare(
            "t24.slow".into(),
            lapin::options::QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("control op succeeds while a consumer stalls");

    // Exact accounting at the stall: 10 unacked + 10 ready.
    let q = mixed_ch
        .queue_declare(
            "t24.slow".into(),
            lapin::options::QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(q.message_count(), 10, "ready = backlog minus credit");
    let qf = mixed_ch
        .queue_declare(
            "t24.fast".into(),
            lapin::options::QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(qf.message_count(), 20);

    // Drain the stalled consumer: acking releases credit; every message
    // eventually arrives — nothing was dropped or duplicated by the stall.
    let mut seen: Vec<u8> = Vec::new();
    for d in &stalled {
        seen.push(d.data[0]);
        d.acker.ack(BasicAckOptions::default()).await.unwrap();
    }
    while let Some(d) = next_delivery(&mut consumer).await {
        seen.push(d.data[0]);
        d.acker.ack(BasicAckOptions::default()).await.unwrap();
    }
    seen.sort_unstable();
    assert_eq!(
        seen,
        (0..20).collect::<Vec<u8>>(),
        "exactly-once through the stall"
    );

    let _ = slow.close(200, "bye".into()).await;
    let _ = mixed.close(200, "bye".into()).await;
}
