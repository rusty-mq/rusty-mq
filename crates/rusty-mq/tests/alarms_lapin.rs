//! §10/T24 slice: resource alarms — memory alarm stops message admissions
//! (506, never a false success), the budget-hit transition notifies
//! capable clients with connection.blocked, journal commits quiesce under
//! a disk alarm (§6.4), and readiness flips while liveness stays healthy.

use std::sync::Arc;
use std::time::Duration;

use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-alarm-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn serve(
    data_dir: std::path::PathBuf,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    Arc<rusty_mq::Broker>,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &data_dir,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(rusty_mq::server::serve_listener_shared(
        listener,
        broker.clone(),
    ));
    (addr, handle, broker)
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

/// Shrink the store budget effect by pre-filling the in-memory store until
/// the alarm raises: publish big transient messages through a real client.
#[tokio::test(flavor = "multi_thread")]
async fn memory_alarm_stops_admissions_and_notifies_blocked() {
    let data = dir("mem");
    let (addr, server, broker) = serve(data.clone()).await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "big.q".into(),
        QueueDeclareOptions {
            durable: false,
            exclusive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();

    // Force the transition directly: fill the store to its budget with an
    // in-process enqueue (deterministic; the broker's budget is 64 MiB and
    // pushing that over the wire is slow).
    // Resolve the queue id FIRST (no lock nesting), then fill under the
    // store lock alone — the server task takes topology→store orderings
    // and a store→topology nesting here deadlocks against it.
    let big_q = {
        let topo = broker.topology.lock().unwrap();
        let vhost = topo.find_vhost("/").unwrap();
        topo.find_queue(vhost, "big.q").unwrap()
    };
    {
        let mut store = broker.store.lock().unwrap();
        let filler = rusty_mq_core::StoredMessage {
            property_bytes: vec![],
            body: vec![0u8; 1024 * 1024], // 1 MiB
            exchange: String::new(),
            routing_key: "big.q".into(),
            persistent: false,
            redelivered: false,
        };
        // Fill to the 64 MiB budget: enqueue until full (the budget error
        // itself marks the boundary).
        while store.enqueue(big_q, filler.clone()).is_ok() {}
    }
    broker.evaluate_alarms_and_notify();
    assert!(broker.memory_alarm(), "budget hit raises the memory alarm");

    // The next publish is refused with 506 — never a fabricated success.
    // The WRITE itself is racy under load (the refusal may reach lapin
    // before the publish flush completes — ConnectionAborted seen once in
    // ~8 full-suite runs); either outcome is the refusal surfacing, so
    // accept Ok or Err here and assert on the CHANNEL state via a probe.
    let _ = ch
        .basic_publish(
            "".into(),
            "big.q".into(),
            BasicPublishOptions::default(),
            b"over-budget".as_ref(),
            BasicProperties::default(),
        )
        .await;
    // The refusal closes the channel: the probe fails within a deadline.
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        ch.queue_declare(
            "probe.q".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ),
    )
    .await
    .expect("probe completes within the deadline")
    .expect_err("admissions paused under the memory alarm");
    let text = err.to_string().to_lowercase();
    assert!(
        text.contains("resource") || text.contains("memory alarm") || text.contains("closed"),
        "got: {err}"
    );

    // Unblock: drain the filler in-process, settle-transition evaluate.
    {
        let mut store = broker.store.lock().unwrap();
        store.purge(big_q);
    }
    broker.evaluate_alarms_and_notify();
    assert!(!broker.memory_alarm(), "below the clear watermark unblocks");

    server.abort();
    let _ = std::fs::remove_dir_all(&data);
}

#[tokio::test(flavor = "multi_thread")]
async fn disk_alarm_quiesces_journal_and_flips_readiness() {
    let data = dir("disk");
    let (addr, server, broker) = serve(data.clone()).await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();

    // Declare a durable queue while healthy.
    ch.queue_declare(
        "disk.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();

    // Inject a below-floor free-space reading: the disk alarm raises on
    // the next evaluation and journal commits refuse (§6.4).
    {
        let mut alarms = broker.alarms.lock().unwrap();
        alarms.inject_volume_bytes(10_000_000_000);
        alarms.inject_free_bytes(Some(1024 * 1024)); // 1 MiB < floors
    }
    broker.evaluate_alarms_and_notify();
    assert!(broker.disk_alarm());

    // Durable declare now fails with 506 (journal quiesced).
    let err = ch
        .queue_declare(
            "disk2.q".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("durable writes quiesce under the disk alarm");
    let text = err.to_string().to_lowercase();
    assert!(
        text.contains("resource") || text.contains("closed") || text.contains("journal"),
        "got: {err}"
    );

    // Readiness fails while liveness stays healthy (§12.1).
    use rusty_mq_management::broker_facade::BrokerHandle as _;
    assert!(!broker.is_ready(), "disk alarm -> not ready");

    // Recovery: free space restored -> alarm clears, commits resume.
    broker
        .alarms
        .lock()
        .unwrap()
        .inject_free_bytes(Some(5_000_000_000));
    broker.evaluate_alarms_and_notify();
    assert!(!broker.disk_alarm());
    assert!(broker.is_ready());

    server.abort();
    let _ = std::fs::remove_dir_all(&data);
}

#[tokio::test(flavor = "multi_thread")]
async fn blocked_notification_reaches_capable_client() {
    // Frame-level check that connection.blocked is emitted on the
    // transition (lapin's high-level event surface differs across
    // versions; the wire frame is the contract).
    use amq_protocol::frame::AMQPFrame;
    use amq_protocol::protocol::{connection as conn7, AMQPClass};
    use tokio::io::AsyncWriteExt;

    let data = dir("blocked");
    let (addr, server, broker) = serve(data.clone()).await;

    // A raw client that declares connection.blocked.
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(&rusty_mq_protocol::PROTOCOL_HEADER_0_9_1)
        .await
        .unwrap();
    let start_ok = AMQPFrame::Method(
        0,
        AMQPClass::Connection(conn7::AMQPMethod::StartOk(conn7::StartOk {
            client_properties: {
                let mut t = amq_protocol::types::FieldTable::default();
                let mut caps = amq_protocol::types::FieldTable::default();
                caps.insert(
                    "connection.blocked".into(),
                    amq_protocol::types::AMQPValue::Boolean(true),
                );
                t.insert(
                    "capabilities".into(),
                    amq_protocol::types::AMQPValue::FieldTable(caps),
                );
                t
            },
            mechanism: "PLAIN".into(),
            response: amq_protocol::types::LongString::from(vec![
                0, b'g', b'u', b'e', b's', b't', 0, b'g', b'u', b'e', b's', b't',
            ]),
            locale: "en_US".into(),
        })),
    );
    sock.write_all(&rusty_mq_protocol::encode_frame(&start_ok))
        .await
        .unwrap();

    let limits = rusty_mq_protocol::ProtocolLimits::default();
    let negotiated = limits
        .negotiate(limits.max_channel_max, limits.max_frame_max, 0)
        .unwrap();
    let mut reader = rusty_mq_protocol::FrameReader::new_post_header(&negotiated);
    let mut buf = vec![0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_blocked = false;
    let mut opened = false;

    // Raise the memory alarm in-process; the capable client must see
    // connection.blocked on channel 0.
    {
        let mut store = broker.store.lock().unwrap();
        let filler = rusty_mq_core::StoredMessage {
            property_bytes: vec![],
            body: vec![0u8; 1024 * 1024],
            exchange: String::new(),
            routing_key: String::new(),
            persistent: false,
            redelivered: false,
        };
        // A throwaway queue id: the store budget is global.
        let q = rusty_mq_core::QueueId::for_test(999_001);
        while store.enqueue(q, filler.clone()).is_ok() {}
    }
    broker.evaluate_alarms_and_notify();

    while tokio::time::Instant::now() < deadline && !(saw_blocked && opened) {
        let n = match sock.try_read(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            Err(e) => panic!("socket error: {e}"),
        };
        reader.feed(&buf[..n]).unwrap();
        while let Ok(Some(frame)) = reader.next_frame() {
            match frame {
                AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::Blocked(b))) => {
                    assert!(!b.reason.as_str().is_empty(), "reason carried");
                    saw_blocked = true;
                }
                AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::Tune(t))) => {
                    let tune_ok = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::TuneOk(conn7::TuneOk {
                            channel_max: t.channel_max,
                            frame_max: t.frame_max,
                            heartbeat: t.heartbeat,
                        })),
                    );
                    sock.write_all(&rusty_mq_protocol::encode_frame(&tune_ok))
                        .await
                        .unwrap();
                    let open = AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(conn7::AMQPMethod::Open(conn7::Open {
                            virtual_host: "/".into(),
                        })),
                    );
                    sock.write_all(&rusty_mq_protocol::encode_frame(&open))
                        .await
                        .unwrap();
                }
                AMQPFrame::Method(0, AMQPClass::Connection(conn7::AMQPMethod::OpenOk(_))) => {
                    opened = true;
                }
                _other => {}
            }
        }
    }
    assert!(opened, "handshake completed");
    assert!(saw_blocked, "capable client received connection.blocked");

    server.abort();
    let _ = std::fs::remove_dir_all(&data);
}
