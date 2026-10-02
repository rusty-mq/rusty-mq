//! M4 exit gate slice: persistent round trip — durable topology and
//! persistent messages survive a broker kill and restart on the same data
//! directory; settled entries never resurrect (INV-02).
//!
//! The "kill" is aborting the server task (dropping the broker and its
//! journal writer without an end marker), which exercises the crash path:
//! recovery replays committed fences only.

use std::time::Duration;

use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};

fn data_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-durability-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn serve_persistent(
    dir: &std::path::Path,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let broker = rusty_mq::Broker::open_persistent("guest".into(), "guest".into(), dir);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(rusty_mq::server::serve_listener(listener, broker));
    (addr, handle)
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

fn durable() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..Default::default()
    }
}

async fn publish_persistent(ch: &lapin::Channel, exchange: &str, key: &str, body: &[u8]) {
    ch.basic_publish(
        exchange.into(),
        key.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default().with_delivery_mode(2),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn persistent_messages_and_topology_survive_restart() {
    let dir = data_dir("restart");

    // --- Phase 1: topology, three persistent messages, one acked, one
    // held unacked at the kill point.
    let (addr, server) = serve_persistent(&dir).await;
    {
        let conn = connect(addr).await;
        let ch = conn.create_channel().await.unwrap();
        ch.queue_declare("jobs".into(), durable(), FieldTable::default())
            .await
            .unwrap();
        ch.exchange_declare(
            "work.direct".into(),
            ExchangeKind::Direct,
            lapin::options::ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        ch.queue_bind(
            "jobs".into(),
            "work.direct".into(),
            "tasks".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

        for body in ["one".as_ref(), "two".as_ref(), "three".as_ref()] {
            publish_persistent(&ch, "work.direct", "tasks", body).await;
        }

        // Ack "one" terminally; hold "two" unacked across the crash.
        let m1 = ch
            .basic_get("jobs".into(), BasicGetOptions { no_ack: false })
            .await
            .unwrap()
            .expect("one");
        assert_eq!(m1.delivery.data, b"one".to_vec());
        m1.delivery
            .acker
            .ack(BasicAckOptions::default())
            .await
            .unwrap();
        let m2 = ch
            .basic_get("jobs".into(), BasicGetOptions { no_ack: false })
            .await
            .unwrap()
            .expect("two");
        assert_eq!(m2.delivery.data, b"two".to_vec());
        let _ = m2; // never acked: must come back after restart

        // "three" stays ready. No graceful close: the broker is killed.
    }
    server.abort();

    // --- Phase 2: same data directory, fresh broker (recovery + replay).
    let (addr2, server2) = serve_persistent(&dir).await;
    {
        let conn = connect(addr2).await;
        let ch = conn.create_channel().await.unwrap();

        // Durable topology restored: passive declare + the binding still
        // routes (publish via the exchange lands in the queue).
        ch.queue_declare(
            "jobs".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("durable queue survived the restart");
        let q = ch
            .queue_declare(
                "jobs".into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            q.message_count(),
            2,
            "acked 'one' is gone; 'two' and 'three' survive"
        );

        ch.exchange_declare(
            "work.direct".into(),
            ExchangeKind::Direct,
            lapin::options::ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("durable exchange survived (equivalent redeclare)");

        publish_persistent(&ch, "work.direct", "tasks", b"four").await;
        let q = ch
            .queue_declare(
                "jobs".into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            q.message_count(),
            3,
            "restored binding routes new publishes"
        );

        // The recovered set: two, three, four — one never returns (INV-02).
        let mut seen = Vec::new();
        while let Some(m) = ch
            .basic_get("jobs".into(), BasicGetOptions { no_ack: false })
            .await
            .unwrap()
        {
            seen.push(String::from_utf8_lossy(&m.delivery.data).into_owned());
            m.delivery
                .acker
                .ack(BasicAckOptions::default())
                .await
                .unwrap();
        }
        let mut expected = vec!["two".to_string(), "three".to_string(), "four".to_string()];
        seen.sort();
        expected.sort();
        assert_eq!(seen, expected, "exactly the surviving set, in order");
        assert!(
            !seen.contains(&"one".to_string()),
            "settled entry never resurrects"
        );
        let _ = conn.close(200, "bye".into()).await;
    }
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_on_second_data_dir_starts_empty() {
    let dir = data_dir("fresh");
    let (addr, server) = serve_persistent(&dir).await;
    {
        let conn = connect(addr).await;
        let ch = conn.create_channel().await.unwrap();
        ch.queue_declare("solo".into(), durable(), FieldTable::default())
            .await
            .unwrap();
        publish_persistent(&ch, "", "solo", b"x").await;
    }
    server.abort();

    // A different directory is a different broker: nothing carries over.
    let dir2 = data_dir("fresh-other");
    let (addr2, server2) = serve_persistent(&dir2).await;
    {
        let conn = connect(addr2).await;
        let ch = conn.create_channel().await.unwrap();
        let err = ch
            .queue_declare(
                "solo".into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .expect_err("other data dir starts empty");
        assert!(err.to_string().to_lowercase().contains("not_found"));
    }
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}
