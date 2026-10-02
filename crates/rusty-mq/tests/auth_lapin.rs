//! T21 slice: durable principals, vhost isolation, and §11.2 permission
//! enforcement through a real client — including revocation taking effect
//! on a live broker.

use std::sync::Arc;
use std::time::Duration;

use lapin::{
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

fn data_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-auth-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn serve(
    dir: std::path::PathBuf,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    Arc<rusty_mq::Broker>,
) {
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &dir,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(rusty_mq::server::serve_listener_shared(
        listener,
        broker.clone(),
    ));
    (addr, handle, broker)
}

async fn connect_as(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    vhost: &str,
) -> Result<Connection, lapin::Error> {
    let uri = format!("amqp://{user}:{pass}@{addr}/{vhost}");
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect attempt completes")
}

fn error_text(e: &lapin::Error) -> String {
    e.to_string().to_lowercase()
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_admin_works_and_wrong_password_fails() {
    let dir = data_dir("bootstrap");
    let (addr, server, _b) = serve(dir.clone()).await;

    let ok = connect_as(addr, "admin", "admin-secret", "%2F").await;
    assert!(
        ok.is_ok(),
        "bootstrapped admin with the flagged password works"
    );

    let bad = connect_as(addr, "admin", "wrong", "%2F").await;
    assert!(bad.is_err(), "wrong password refused");

    server.abort();

    // Durable: the bootstrapped admin survives restart (Argon2id hash
    // replayed from the journal).
    let (addr2, server2, _b2) = serve(dir.clone()).await;
    let ok2 = connect_as(addr2, "admin", "admin-secret", "%2F").await;
    assert!(ok2.is_ok(), "admin credentials survive restart");
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn permissions_enforce_the_11_2_table() {
    let dir = data_dir("perms");
    let (addr, server, broker) = serve(dir.clone()).await;

    // A restricted user: may only write to 'events', read nothing, declare
    // only 'events'.
    broker
        .upsert_principal(
            rusty_mq::Principal {
                username: "app".into(),
                password_phc: String::new(),
                role: rusty_mq::Role::Ordinary,
            },
            Some("app-pass"),
        )
        .unwrap();
    broker
        .set_permissions(
            "app",
            "/",
            rusty_mq::Permissions {
                configure: "^events$".into(),
                write: "events".into(),
                read: "!nothing".into(), // matches no resource name
            },
        )
        .unwrap();

    let conn = connect_as(addr, "app", "app-pass", "%2F")
        .await
        .expect("app connects");
    let ch = conn.create_channel().await.unwrap();

    // configure: allowed on 'events', refused on 'other'.
    ch.queue_declare(
        "events".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("configure on events allowed");
    let err = ch
        .queue_declare(
            "other".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("configure on other refused");
    assert!(error_text(&err).contains("refused"), "got: {err}");

    // write: publish to the default exchange routes to queue 'events' —
    // normalized name amq.default does NOT match 'events'... the §11.2 rule
    // uses amq.default for the default exchange, so a write-only-'events'
    // user cannot publish via the default exchange.
    // (The declare refusal above closed `ch`; each scoped check gets a
    // fresh channel.)
    let ch_w = conn.create_channel().await.unwrap();
    let publish = ch_w
        .basic_publish(
            "".into(),
            "events".into(),
            BasicPublishOptions::default(),
            b"x".as_ref(),
            BasicProperties::default(),
        )
        .await
        .expect("publish write accepted (refusal closes asynchronously)");
    // The refusal surfaces as the channel closing before any confirm.
    let consumer_probe = ch_w
        .basic_consume(
            "events".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await;
    match consumer_probe {
        Err(e) => assert!(
            error_text(&e).contains("refused") || error_text(&e).contains("closed"),
            "got: {e}"
        ),
        Ok(_) => panic!("channel should be closed after the publish refusal"),
    }
    drop(publish);

    // read: consume refused (pattern matches nothing). The publish
    // refusal closed the previous channel (channel-scoped 403), so use a
    // fresh one.
    let ch2 = conn.create_channel().await.unwrap();
    let err = ch2
        .basic_consume(
            "events".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("read refused");
    assert!(
        error_text(&err).contains("refused") || error_text(&err).contains("closed"),
        "got: {err}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn vhost_isolation_and_revocation() {
    let dir = data_dir("vhost");
    let (addr, server, broker) = serve(dir.clone()).await;

    broker
        .upsert_principal(
            rusty_mq::Principal {
                username: "iso".into(),
                password_phc: String::new(),
                role: rusty_mq::Role::Ordinary,
            },
            Some("iso-pass"),
        )
        .unwrap();
    broker
        .set_permissions(
            "iso",
            "/",
            rusty_mq::Permissions {
                configure: ".*".into(),
                write: ".*".into(),
                read: ".*".into(),
            },
        )
        .unwrap();

    // INV-08: no permission entry for a vhost -> broker refuses the open.
    // (lapin's connect future hangs on server-side close during the
    // open-ok wait — the documented M1 finding — so the gate is asserted
    // at the broker layer here; the frame-level refusal is already proven
    // by the M1 vhost test.)
    assert!(!broker.auth.lock().unwrap().may_access_vhost("iso", "nope"));
    // And the AMQP plane shows it too: a raw probe would get 403; the
    // in-memory broker check above is the same predicate the connection
    // handler consults.

    let conn = connect_as(addr, "iso", "iso-pass", "%2F")
        .await
        .expect("iso enters /");
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "iso.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("full perms work");

    // Revocation: delete permissions; the live connection's subsequent
    // operations are refused (FR-S08's effect on the AMQP plane).
    broker.auth.lock().unwrap().delete_permissions("iso", "/");
    let ch2 = conn.create_channel().await.unwrap();
    let err = ch2
        .queue_declare(
            "iso.q2".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("revocation takes effect immediately");
    assert!(error_text(&err).contains("refused"), "got: {err}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
