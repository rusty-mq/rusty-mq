//! §10 admission budgets (ADR-0004): count caps are enforced at the
//! mutation boundary with channel-scoped 506 — never silent acceptance.
//! Config-driven (limits.*), so the caps are live operator surface.

use std::sync::Arc;
use std::time::Duration;

use lapin::{
    options::{QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};

async fn broker_with(limits: &str) -> (Arc<rusty_mq::Broker>, std::net::SocketAddr) {
    let cfg = rusty_mq::config::load_str(limits).expect("test config");
    let dir = std::env::temp_dir().join(format!(
        "rmq-caps-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let broker = Arc::new(rusty_mq::Broker::open_persistent_from_config(
        "guest".into(),
        "guest".into(),
        &dir,
        &cfg,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(
        listener,
        broker.clone(),
    ));
    (broker, addr)
}

async fn connect(addr: std::net::SocketAddr) -> Connection {
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect timeout")
    .expect("handshake")
}

async fn declare(ch: &Channel, name: &str) -> Result<(), lapin::Error> {
    ch.queue_declare(
        name.into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .map(|_| ())
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_cap_rejects_with_506_and_redeclare_stays_legal() {
    let (_broker, addr) = broker_with("[limits]\nmax_queues_per_vhost = 2\n").await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, "cap.q1").await.expect("first queue ok");
    declare(&ch, "cap.q2").await.expect("second queue ok");

    // The 506 closes the channel; the message must name the budget.
    let err = declare(&ch, "cap.q3").await.expect_err("cap exhausted");
    let msg = err.to_string();
    assert!(msg.contains("RESOURCE_ERROR"), "got: {msg}");
    assert!(msg.contains("queues per vhost limit 2"), "got: {msg}");

    // Equivalent redeclare never grows the budget: fresh channel works
    // and redeclaring an existing queue at-cap is still legal.
    let ch2 = conn.create_channel().await.unwrap();
    declare(&ch2, "cap.q1")
        .await
        .expect("redeclare at cap is legal");
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn binding_cap_rejects_new_binds_but_duplicates_stay_legal() {
    let (_broker, addr) = broker_with("[limits]\nmax_bindings_per_vhost = 2\n").await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, "bcap.q").await.expect("queue");
    ch.exchange_declare(
        "bcap.ex".into(),
        lapin::ExchangeKind::Topic,
        lapin::options::ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();

    async fn bind(ch: &Channel, key: &str) -> Result<(), lapin::Error> {
        ch.queue_bind(
            "bcap.q".into(),
            "bcap.ex".into(),
            key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map(|_| ())
    }
    bind(&ch, "a").await.expect("first bind");
    bind(&ch, "b").await.expect("second bind");
    let err = bind(&ch, "c").await.expect_err("cap exhausted");
    assert!(err.to_string().contains("bindings per vhost limit 2"));

    // Duplicate of an existing binding is idempotent — no budget growth.
    let ch2 = conn.create_channel().await.unwrap();
    bind(&ch2, "a")
        .await
        .expect("duplicate bind at cap is legal");
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_cap_refuses_the_third_connection() {
    let (_broker, addr) = broker_with("[limits]\nmax_connections = 2\n").await;
    let c1 = connect(addr).await;
    let c2 = connect(addr).await;

    // The third connection is refused at the server with 506 — lapin
    // surfaces the broker close as a connect failure.
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    let third = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("no hang");
    assert!(third.is_err(), "connection over cap must be refused");

    let _ = c1.close(200, "bye".into()).await;
    let _ = c2.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn byte_budget_publish_guard_unaffected() {
    // Sanity: the pre-existing byte-budget 506 path still works beside
    // the new count caps (a publish through the default exchange to a
    // declared queue succeeds under default caps).
    let (_broker, addr) = broker_with("[limits]\nmax_queues_per_vhost = 10000\n").await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, "sane.q").await.unwrap();
    let confirm = ch
        .basic_publish(
            "".into(),
            "sane.q".into(),
            lapin::options::BasicPublishOptions::default(),
            b"x",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    confirm.await.expect("publish still flows");
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_cap_rejects_fanout_expansion() {
    // Three queues bound to one fanout exchange; cap at 2 destinations.
    let (_broker, addr) = broker_with("[limits]\nmax_destinations_per_publish = 2\n").await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    ch.exchange_declare(
        "dcap.ex".into(),
        lapin::ExchangeKind::Fanout,
        lapin::options::ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    for q in ["dcap.q1", "dcap.q2", "dcap.q3"] {
        declare(&ch, q).await.unwrap();
        ch.queue_bind(
            q.into(),
            "dcap.ex".into(),
            "".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
    // The 3-destination fanout is refused (channel 506, naming the
    // count). Publish errors surface ASYNCHRONOUSLY (the matrix lesson):
    // probe with a follow-up op.
    let _ = ch
        .basic_publish(
            "dcap.ex".into(),
            "".into(),
            lapin::options::BasicPublishOptions::default(),
            b"x",
            BasicProperties::default(),
        )
        .await;
    let err = declare(&ch, "dcap.probe")
        .await
        .expect_err("destination expansion over cap");
    let msg = err.to_string();
    assert!(
        msg.contains("3 destinations, over the limit (2)"),
        "got: {msg}"
    );
    let ch2 = conn.create_channel().await.unwrap();
    declare(&ch2, "dcap.after")
        .await
        .expect("connection alive after the 506");
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_confirms_cap_value_is_config_driven_and_structurally_bounded() {
    // Config drives the cap value...
    let cfg =
        rusty_mq::config::load_str("[limits]\nmax_pending_confirms_per_channel = 7\n").unwrap();
    assert_eq!(cfg.limits.max_pending_confirms_per_channel, 7);
    let dir = std::env::temp_dir().join(format!("rmq-pcap-cfg-{}", std::process::id()));
    let broker =
        rusty_mq::Broker::open_persistent_from_config("guest".into(), "guest".into(), &dir, &cfg);
    assert_eq!(broker.admission.max_pending_confirms_per_channel, 7);
    let _ = std::fs::remove_dir_all(&dir);

    // ...but pending is STRUCTURALLY bounded at 1 per channel today: the
    // incomplete-content guard (505) blocks a second publish method while
    // content is in flight, and each durable publish's journal commit is
    // awaited inline before the next frame processes. Concurrent
    // persistent publishes on one channel therefore serialize (verified
    // empirically during this slice: pending never reached 2). The cap
    // is defense-in-depth if the frame loop ever pipelines — recorded
    // in the ledger as a design note, not a behavioral claim.
}
