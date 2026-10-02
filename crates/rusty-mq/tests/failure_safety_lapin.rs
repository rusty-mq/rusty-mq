//! T13/T14 + §9.6 slices: failure safety on the durable paths.
//!
//! - A persistent entry delivered manual-ack but unacked at the kill point
//!   comes back after restart with the conservative redelivered hint
//!   (Delivered markers, §9.6).
//! - A no-ack consumer delivery of a persistent entry journals its terminal
//!   dequeue BEFORE exposure: after a kill mid-stream, the entry never
//!   comes back (no-ack is outside at-least-once by design).
//! - With the journal fsync failing (failpoint), a confirm-mode persistent
//!   publish NEVER receives a positive confirm (INV-01 / T13): the channel
//!   closes with 506 instead.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

fn data_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-fsafety-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn serve_persistent(
    dir: &std::path::Path,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    Arc<rusty_mq::Broker>,
) {
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        dir,
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

async fn declare_durable(ch: &lapin::Channel, queue: &str) {
    ch.queue_declare(
        queue.into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
}

async fn publish_persistent(ch: &lapin::Channel, queue: &str, body: &[u8]) {
    ch.basic_publish(
        "".into(),
        queue.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default().with_delivery_mode(2),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_ack_delivery_comes_back_redelivered_after_kill() {
    let dir = data_dir("marker");
    let (addr, server, _broker) = serve_persistent(&dir).await;
    {
        let conn = connect(addr).await;
        let ch = conn.create_channel().await.unwrap();
        declare_durable(&ch, "marked.q").await;
        publish_persistent(&ch, "marked.q", b"once-delivered").await;

        let mut consumer = ch
            .basic_consume(
                "marked.q".into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("delivery")
            .expect("stream alive")
            .expect("ok");
        assert_eq!(d.data, b"once-delivered".to_vec());
        assert!(!d.redelivered, "first delivery carries no hint");
        // Delivered but never acked — kill the broker here (§9.6 marker).
    }
    server.abort();

    let (addr2, server2, _b2) = serve_persistent(&dir).await;
    {
        let conn = connect(addr2).await;
        let ch = conn.create_channel().await.unwrap();
        let m = ch
            .basic_get(
                "marked.q".into(),
                lapin::options::BasicGetOptions { no_ack: false },
            )
            .await
            .unwrap()
            .expect("entry recovered after kill");
        assert_eq!(m.delivery.data, b"once-delivered".to_vec());
        assert!(
            m.delivery.redelivered,
            "Delivered marker restores the conservative redelivery hint"
        );
        m.delivery
            .acker
            .ack(lapin::options::BasicAckOptions::default())
            .await
            .unwrap();
    }
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_ack_delivery_settles_before_exposure() {
    let dir = data_dir("noack-boundary");
    let (addr, server, _broker) = serve_persistent(&dir).await;
    {
        let conn = connect(addr).await;
        let ch = conn.create_channel().await.unwrap();
        declare_durable(&ch, "na.q").await;
        publish_persistent(&ch, "na.q", b"seen").await;

        let mut consumer = ch
            .basic_consume(
                "na.q".into(),
                "".into(),
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("delivery")
            .expect("stream alive")
            .expect("ok");
        assert_eq!(d.data, b"seen".to_vec());
        // Kill immediately after the delivery reached the client: the
        // terminal dequeue was journaled BEFORE exposure (§9.6), so this
        // entry must never reappear.
    }
    server.abort();

    let (addr2, server2, _b2) = serve_persistent(&dir).await;
    {
        let conn = connect(addr2).await;
        let ch = conn.create_channel().await.unwrap();
        let count = ch
            .queue_declare(
                "na.q".into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap()
            .message_count();
        assert_eq!(
            count, 0,
            "no-ack delivery is terminally settled pre-exposure"
        );
    }
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_positive_confirm_when_journal_fsync_fails() {
    let dir = data_dir("t13");
    let (addr, server, broker) = serve_persistent(&dir).await;

    // Set up topology while the journal is healthy.
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    declare_durable(&ch, "t13.q").await;
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    // Fail every journal commit at the before-sync point: INV-01 says no
    // positive confirm may precede a successful sync.
    let tripped = Arc::new(AtomicBool::new(false));
    let tripped2 = tripped.clone();
    let fp: rusty_mq_storage::journal::Failpoint = Box::new(move |name| {
        if name == "before-sync" {
            tripped2.store(true, Ordering::SeqCst);
            Err("injected fsync failure".into())
        } else {
            Ok(())
        }
    });
    broker.set_journal_failpoint(Some(Arc::new(fp)));

    let publish = ch
        .basic_publish(
            "".into(),
            "t13.q".into(),
            BasicPublishOptions::default(),
            b"never-confirmed".as_ref(),
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .expect("publish write accepted");

    // The confirm future must NOT resolve positively. The channel closes
    // with 506 (storage failure with uncertain persistence — §6.4), which
    // lapin surfaces as an error on the pending confirm.
    let outcome = tokio::time::timeout(Duration::from_secs(5), publish).await;
    match outcome {
        Err(_) => panic!("confirm future hung instead of failing"),
        Ok(Err(_)) => { /* channel-close error: the correct outcome */ }
        Ok(Ok(lapin::Confirmation::Ack(_))) => {
            panic!("INV-01 violated: positive confirm without a successful fsync")
        }
        Ok(Ok(other)) => panic!("expected close-error, got confirmation {other:?}"),
    }
    assert!(
        tripped.load(Ordering::SeqCst),
        "the failpoint actually fired"
    );
    let _ = conn.close(200, "bye".into()).await;
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
