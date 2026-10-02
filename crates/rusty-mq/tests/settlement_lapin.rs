//! T08 slices: negative settlement — basic.reject (terminal discard and
//! requeue), basic.nack single/multiple, basic.recover(requeue=true), and
//! the frozen rejection of recover(requeue=false) with 540.

use std::time::Duration;

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicNackOptions,
        BasicPublishOptions, BasicRecoverOptions, BasicRejectOptions, QueueDeclareOptions,
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

fn durable_queue() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..Default::default()
    }
}

async fn setup(ch: &lapin::Channel, queue: &str) {
    ch.queue_declare(queue.into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
}

async fn publish(ch: &lapin::Channel, queue: &str, body: &[u8]) {
    ch.basic_publish(
        "".into(),
        queue.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default(),
    )
    .await
    .unwrap();
}

async fn get(ch: &lapin::Channel, queue: &str) -> Option<lapin::message::BasicGetMessage> {
    ch.basic_get(queue.into(), BasicGetOptions { no_ack: false })
        .await
        .unwrap()
}

async fn ready_count(ch: &lapin::Channel, queue: &str) -> u32 {
    ch.queue_declare(
        queue.into(),
        QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap()
    .message_count()
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_without_requeue_is_terminal_discard() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    setup(&ch, "discard.q").await;
    publish(&ch, "discard.q", b"one").await;
    publish(&ch, "discard.q", b"two").await;

    let m1 = get(&ch, "discard.q").await.expect("first");
    let m2 = get(&ch, "discard.q").await.expect("second");
    // requeue=false discards: no DLX in V1, so the entries are gone.
    m1.delivery
        .acker
        .reject(BasicRejectOptions { requeue: false })
        .await
        .unwrap();
    m2.delivery
        .acker
        .reject(BasicRejectOptions { requeue: false })
        .await
        .unwrap();

    assert_eq!(ready_count(&ch, "discard.q").await, 0);
    assert!(get(&ch, "discard.q").await.is_none());
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_with_requeue_restores_position_and_flag() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    setup(&ch, "requeue.q").await;
    publish(&ch, "requeue.q", b"first").await;
    publish(&ch, "requeue.q", b"second").await;

    let m1 = get(&ch, "requeue.q").await.expect("first");
    assert_eq!(m1.delivery.data, b"first".to_vec());
    m1.delivery
        .acker
        .reject(BasicRejectOptions { requeue: true })
        .await
        .unwrap();

    // Original relative position preserved; redelivered hint set.
    let again = get(&ch, "requeue.q").await.expect("requeued");
    assert_eq!(again.delivery.data, b"first".to_vec());
    assert!(again.delivery.redelivered, "redelivered hint after requeue");
    let m2 = get(&ch, "requeue.q").await.expect("second still there");
    assert_eq!(m2.delivery.data, b"second".to_vec());
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_multiple_requeues_all_outstanding() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    setup(&ch, "nack.q").await;
    for i in 0..3u8 {
        publish(&ch, "nack.q", &[i]).await;
    }
    let m1 = get(&ch, "nack.q").await.expect("1");
    let m2 = get(&ch, "nack.q").await.expect("2");
    let m3 = get(&ch, "nack.q").await.expect("3");
    assert_eq!(ready_count(&ch, "nack.q").await, 0);

    // nack multiple on the highest tag requeues everything ≤ it.
    m3.delivery
        .acker
        .nack(BasicNackOptions {
            multiple: true,
            requeue: true,
        })
        .await
        .unwrap();
    let _ = m1;
    let _ = m2;

    assert_eq!(ready_count(&ch, "nack.q").await, 3);
    for expected in 0..3u8 {
        let m = get(&ch, "nack.q").await.expect("requeued entry");
        assert_eq!(m.delivery.data, vec![expected]);
        assert!(m.delivery.redelivered);
    }
    assert!(get(&ch, "nack.q").await.is_none());
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_requeues_channel_deliveries_to_consumer() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    setup(&ch, "recover.q").await;
    publish(&ch, "recover.q", b"payload").await;

    let mut consumer = ch
        .basic_consume(
            "recover.q".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("delivery within timeout")
        .expect("stream alive")
        .expect("delivery ok");
    assert_eq!(d.data, b"payload".to_vec());
    // Delivered but unacked; recover(requeue=true) must requeue it and let
    // it be redelivered (FR-C08).
    ch.basic_recover(BasicRecoverOptions { requeue: true })
        .await
        .expect("recover-ok");

    let d2 = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("redelivery within timeout")
        .expect("stream alive")
        .expect("redelivery ok");
    assert_eq!(d2.data, b"payload".to_vec());
    assert!(d2.redelivered, "recovered delivery carries the hint");
    d2.acker.ack(BasicAckOptions::default()).await.unwrap();
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_without_requeue_is_540() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    setup(&ch, "recover-no.q").await;
    publish(&ch, "recover-no.q", b"x").await;
    let _m = get(&ch, "recover-no.q").await.expect("delivery");

    let err = ch
        .basic_recover(BasicRecoverOptions { requeue: false })
        .await
        .expect_err("recover(requeue=false) must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("not_implemented"),
        "expected NOT_IMPLEMENTED, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_delivery_tag_settlement_is_406() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    setup(&ch, "bogus-tag.q").await;
    publish(&ch, "bogus-tag.q", b"held").await;
    let held = get(&ch, "bogus-tag.q")
        .await
        .expect("delivery held unacked");

    // A fabricated tag nobody delivered is a protocol error (§6.1). ack
    // has no reply, so the 406 close surfaces on the next operation.
    assert_ne!(held.delivery.delivery_tag, 9999);
    ch.basic_ack(9999, BasicAckOptions::default())
        .await
        .expect("write accepted");
    let err = ch
        .queue_declare("bogus-tag.q".into(), durable_queue(), FieldTable::default())
        .await
        .expect_err("channel must be closed by the 406");
    assert!(
        err.to_string().to_lowercase().contains("precondition")
            || err.to_string().to_lowercase().contains("not open"),
        "expected PRECONDITION_FAILED (or local closed state), got: {err}"
    );
    // Substantive outcome: the bogus tag settled nothing — the real held
    // entry came back via the channel-close requeue, redelivered.
    let ch2 = conn.create_channel().await.unwrap();
    assert_eq!(ready_count(&ch2, "bogus-tag.q").await, 1);
    let m = get(&ch2, "bogus-tag.q").await.expect("entry survived");
    assert_eq!(m.delivery.data, b"held".to_vec());
    assert!(m.delivery.redelivered);
    let _ = conn.close(200, "bye".into()).await;
}
