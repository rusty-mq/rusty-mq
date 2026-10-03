//! T04/T05/T22 slices: topology methods (exchange/queue declare, bind,
//! unbind, delete) verified through a real client (lapin), including the
//! strict error profile (404/405/406/540) and exclusive-queue lifecycle.

use std::time::Duration;

use lapin::{
    options::{
        ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
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

#[tokio::test(flavor = "multi_thread")]
async fn declare_bind_unbind_delete_roundtrip() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();

    // Durable work queue + direct exchange; equivalent redeclare is
    // idempotent (FR-Q03).
    let q = ch
        .queue_declare("jobs".into(), durable_queue(), FieldTable::default())
        .await
        .expect("queue declare");
    assert_eq!(q.name().as_str(), "jobs");
    assert_eq!(q.message_count(), 0);
    assert_eq!(q.consumer_count(), 0);

    ch.queue_declare("jobs".into(), durable_queue(), FieldTable::default())
        .await
        .expect("equivalent redeclare succeeds");

    ch.exchange_declare(
        "work.direct".into(),
        ExchangeKind::Direct,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("exchange declare");

    ch.queue_bind(
        "jobs".into(),
        "work.direct".into(),
        "tasks".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("bind");
    // Duplicate binding is idempotent (FR-E04).
    ch.queue_bind(
        "jobs".into(),
        "work.direct".into(),
        "tasks".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .expect("duplicate bind idempotent");

    ch.queue_unbind(
        "jobs".into(),
        "work.direct".into(),
        "tasks".into(),
        FieldTable::default(),
    )
    .await
    .expect("unbind");

    let deleted = ch
        .queue_delete("jobs".into(), QueueDeleteOptions::default())
        .await
        .expect("queue delete");
    assert_eq!(deleted, 0);

    ch.exchange_delete("work.direct".into(), ExchangeDeleteOptions::default())
        .await
        .expect("exchange delete");

    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn server_generated_queue_name() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    let q = ch
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare with generated name");
    assert!(
        !q.name().as_str().is_empty(),
        "server must return the generated name"
    );
    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicting_redeclare_is_406() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("conflict".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();

    let err = ch
        .queue_declare(
            "conflict".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("inequivalent redeclare must fail");
    assert!(
        err.to_string().to_lowercase().contains("precondition"),
        "expected PRECONDITION_FAILED, got: {err}"
    );

    // The channel is closed by the error, but the connection survives and a
    // fresh channel works (error scope, FR-P03).
    let closed = ch
        .queue_declare("other".into(), durable_queue(), FieldTable::default())
        .await;
    assert!(
        closed.is_err(),
        "closed channel must reject further methods"
    );

    let ch2 = conn.create_channel().await.expect("new channel opens");
    ch2.queue_declare("other".into(), durable_queue(), FieldTable::default())
        .await
        .expect("fresh channel works after a channel-scoped error");

    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn passive_declare_missing_is_404() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    let err = ch
        .queue_declare(
            "nope".into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("passive declare of missing queue must fail");
    assert!(
        err.to_string().to_lowercase().contains("not_found"),
        "expected NOT_FOUND, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_arguments_are_540() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();

    // x-message-ttl: deferred feature must be rejected, not accepted (T22).
    let mut ttl_args = FieldTable::default();
    ttl_args.insert(
        "x-message-ttl".into(),
        lapin::types::AMQPValue::LongInt(60_000),
    );
    let err = ch
        .queue_declare("ttl-q".into(), QueueDeclareOptions::default(), ttl_args)
        .await
        .expect_err("x-message-ttl must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("not_implemented"),
        "expected NOT_IMPLEMENTED, got: {err}"
    );

    // Durable+exclusive profile is rejected in V1.
    let ch2 = conn.create_channel().await.unwrap();
    let err = ch2
        .queue_declare(
            "bad-profile".into(),
            QueueDeclareOptions {
                durable: true,
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("durable+exclusive must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("precondition"),
        "expected PRECONDITION_FAILED, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exclusive_queue_dies_with_connection() {
    let addr = start_broker().await;
    let conn_a = connect(addr).await;
    let ch_a = conn_a.create_channel().await.unwrap();
    let q = ch_a
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("exclusive queue declared");
    let name = q.name().as_str().to_string();

    // Passive declare from the same connection sees it.
    ch_a.queue_declare(
        name.clone().into(),
        QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("owner can passively inspect");

    // A different connection cannot even inspect it (405, FR-Q04).
    let conn_b = connect(addr).await;
    let ch_b = conn_b.create_channel().await.unwrap();
    let err = ch_b
        .queue_declare(
            name.clone().into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("other connection must be locked out");
    assert!(
        err.to_string().to_lowercase().contains("lock"),
        "expected RESOURCE_LOCKED, got: {err}"
    );
    let _ = conn_b.close(200, "bye".into()).await;

    // Closing the owner reclaims the queue (FR-P09).
    conn_a.close(200, "bye".into()).await.unwrap();
    // Reclaim races the close-ok the client already saw (the admin_close
    // CI flake class): poll to a deadline instead of sleeping a guess.
    let conn_c = connect(addr).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let ch_c = conn_c.create_channel().await.unwrap();
        let err = ch_c
            .queue_declare(
                name.clone().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await;
        match err {
            Err(e) => {
                assert!(
                    e.to_string().to_lowercase().contains("not_found"),
                    "expected NOT_FOUND after reclaim, got: {e}"
                );
                break;
            }
            Ok(_) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "exclusive queue not reclaimed after owner close"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let _ = conn_c.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reserved_amq_exchange_creation_refused() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    let err = ch
        .exchange_declare(
            "amq.custom".into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("amq. names are reserved");
    assert!(
        err.to_string().to_lowercase().contains("refused")
            || err.to_string().to_lowercase().contains("not permitted"),
        "expected ACCESS_REFUSED, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn builtin_amq_direct_is_declared_passively() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    // The predeclared exchange exists and an equivalent declare is OK.
    ch.exchange_declare(
        "amq.direct".into(),
        ExchangeKind::Direct,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("amq.direct exists with durable=true");
    let _ = conn.close(200, "bye".into()).await;
}
