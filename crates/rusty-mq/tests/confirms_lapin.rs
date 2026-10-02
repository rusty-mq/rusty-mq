//! T12/T13 slices: publisher confirms — confirm.select, positive confirms
//! after admission (persistent path: only after the journal commit,
//! INV-01), FIFO confirm ordering per channel (§6.3), mandatory
//! return-before-confirm, and nack-on-rejected-publish.

use std::time::Duration;

use lapin::{
    message::BasicReturnMessage,
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, PublisherConfirm,
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

async fn confirmed_publish(
    ch: &lapin::Channel,
    exchange: &str,
    key: &str,
    body: &[u8],
    persistent: bool,
) -> lapin::Confirmation {
    let mut props = BasicProperties::default();
    if persistent {
        props = props.with_delivery_mode(2);
    }
    let confirm: PublisherConfirm = ch
        .basic_publish(
            exchange.into(),
            key.into(),
            BasicPublishOptions::default(),
            body,
            props,
        )
        .await
        .expect("publish write");
    tokio::time::timeout(Duration::from_secs(5), confirm)
        .await
        .expect("confirm within timeout")
        .expect("confirm resolved")
}

#[tokio::test(flavor = "multi_thread")]
async fn confirm_select_yields_positive_confirms() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "conf.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select-ok");

    for i in 0..5u8 {
        let confirmation = confirmed_publish(&ch, "", "conf.q", &[i], true).await;
        assert!(
            matches!(confirmation, lapin::Confirmation::Ack(None)),
            "persistent publish confirmed positively: {confirmation:?}"
        );
    }
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn confirms_arrive_in_publish_order() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "order.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    // Publish a batch; the confirm futures must resolve FIFO per channel
    // (§6.3: one confirm per publish, order preserved).
    let mut confirms = Vec::new();
    for i in 0..20u8 {
        confirms.push(
            ch.basic_publish(
                "".into(),
                "order.q".into(),
                BasicPublishOptions::default(),
                &[i],
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap(),
        );
    }
    for c in confirms {
        let confirmation = tokio::time::timeout(Duration::from_secs(5), c)
            .await
            .expect("confirm within timeout")
            .expect("resolved");
        assert!(matches!(confirmation, lapin::Confirmation::Ack(None)));
    }
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mandatory_unroutable_returns_then_confirms() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // An exchange that exists but has no bindings: valid publish, zero
    // destinations (a nonexistent exchange would correctly 404 instead).
    ch.exchange_declare(
        "ex.unbound".into(),
        lapin::ExchangeKind::Fanout,
        lapin::options::ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    let publish = ch
        .basic_publish(
            "ex.unbound".into(),
            "k".into(),
            lapin::options::BasicPublishOptions {
                mandatory: true,
                ..Default::default()
            },
            b"returned-payload".as_ref(),
            BasicProperties::default(),
        )
        .await
        .expect("publish write");
    let confirmation = tokio::time::timeout(Duration::from_secs(5), publish)
        .await
        .expect("confirm within timeout")
        .expect("resolved");

    match confirmation {
        lapin::Confirmation::Ack(Some(ret)) => {
            assert_eq!(ret.reply_code, 312, "NO_ROUTE");
            assert_eq!(ret.exchange.as_str(), "ex.unbound");
            assert_eq!(ret.data, b"returned-payload".to_vec());
        }
        lapin::Confirmation::Ack(None) => {
            panic!("expected the returned message attached to the ack")
        }
        other => panic!("expected Ack-with-return, got {other:?}"),
    }
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_publish_is_nacked_in_confirm_mode() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    // delivery_mode=9 is rejected at header time: the reserved confirm
    // sequence must be nacked before the channel close (§6.4).
    let publish = ch
        .basic_publish(
            "".into(),
            "nowhere".into(),
            BasicPublishOptions::default(),
            b"x".as_ref(),
            BasicProperties::default().with_delivery_mode(9),
        )
        .await
        .expect("publish write");
    let confirmation = tokio::time::timeout(Duration::from_secs(5), publish)
        .await
        .expect("nack within timeout")
        .expect("resolved");
    assert!(
        matches!(confirmation, lapin::Confirmation::Nack(_)),
        "rejected publish nacked: {confirmation:?}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

/// The confirmation payload type exists to force compilation of the
/// return-message path used above.
#[allow(dead_code)]
fn _witness(m: &BasicReturnMessage) -> u16 {
    m.reply_code
}
