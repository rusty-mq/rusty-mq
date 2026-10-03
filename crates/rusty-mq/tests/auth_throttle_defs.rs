//! Gate-1 closers: auth throttling (FR-S05) end-to-end and
//! users/permissions in the definitions surface.

use std::sync::Arc;
use std::time::Duration;

use lapin::{Connection as LapinConnection, ConnectionProperties};

use rusty_mq::definitions::{export, import};

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-throttle-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn spawn(tag: &str) -> (std::net::SocketAddr, Arc<rusty_mq::Broker>) {
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &dir(tag),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = broker.clone();
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener, b));
    (addr, broker)
}

async fn try_login(addr: std::net::SocketAddr, pass: &str) -> Result<(), lapin::Error> {
    let uri = format!("amqp://admin:{pass}@{addr}/%2F");
    let conn = LapinConnection::connect(&uri, ConnectionProperties::default()).await?;
    let _ = conn.close(200, "bye".into()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_failures_throttle_then_recover() {
    let (addr, broker) = spawn("throttle").await;
    // The dev limiter: 10 per-peer failures / 10s window. The failures
    // below share the loopback peer string.
    let mut refused = 0;
    for _ in 0..12 {
        if try_login(addr, "wrong").await.is_err() {
            refused += 1;
        }
    }
    assert!(
        refused >= 10,
        "each wrong password must refuse (got {refused})"
    );
    assert_eq!(
        broker.auth_throttle.check("127.0.0.1"),
        rusty_mq::throttle::Decision::Throttled,
        "peer is throttled after the failure burst"
    );
    // And the throttled peer cannot even start a handshake: the connect
    // attempt fails (connection closed before start).
    assert!(try_login(addr, "admin-secret").await.is_err());

    // Recovery: slide the window manually (test hook on the same limiter
    // instance is not available via public API; instead prove the decision
    // type flips once the window empties by constructing a fresh limiter
    // with a tiny window and waiting it out).
    let quick = rusty_mq::throttle::AuthThrottle::new(Duration::from_millis(50), 1, 100);
    quick.record_failure("p");
    assert_eq!(quick.check("p"), rusty_mq::throttle::Decision::Throttled);
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(quick.check("p"), rusty_mq::throttle::Decision::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn definitions_round_trip_users_and_permissions() {
    let (addr, broker) = spawn("defs").await;
    // Create a user + grants through the management surface.
    use rusty_mq_management::broker_facade::BrokerHandle as _;
    broker
        .create_user("app", "app-pass", rusty_mq::Role::Ordinary)
        .unwrap();
    broker
        .set_permissions(
            "app",
            "/",
            rusty_mq::Permissions {
                configure: "^t-".into(),
                write: ".*".into(),
                read: "^t-".into(),
            },
        )
        .unwrap();
    let _ = addr;

    // Export: users (no hashes) + grants present.
    let out = export(&broker);
    let users = out["users"].as_array().unwrap().clone();
    assert!(users
        .iter()
        .any(|u| u["username"] == "app" && u["role"] == "ordinary"));
    assert!(
        !out.to_string().contains("app-pass"),
        "no credential material"
    );
    assert!(!out.to_string().contains("$argon2"), "no hash material");
    let perms = out["permissions"].as_array().unwrap().clone();
    assert!(perms
        .iter()
        .any(|p| p["username"] == "app" && p["configure"] == "^t-"));

    // Import the same grants into a second broker WITHOUT the user:
    // invalid finding (credentials never ride in definitions).
    let dir2 = dir("defs-b");
    let broker2 = Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &dir2,
    ));
    let report = import(&broker2, &out, false).unwrap();
    assert!(report
        .results
        .iter()
        .any(|r| r.resource.contains("permission:app@/")
            && matches!(r.outcome, rusty_mq::definitions::Outcome::Invalid)));

    // Create the user, re-import: grants apply and verify.
    broker2
        .create_user("app", "app-pass", rusty_mq::Role::Ordinary)
        .unwrap();
    let report = import(&broker2, &out, false).unwrap();
    assert!(report
        .results
        .iter()
        .any(|r| r.resource.contains("permission:app@/")
            && matches!(r.outcome, rusty_mq::definitions::Outcome::Created)));
    let got = broker2.get_permissions("app", "/");
    assert_eq!(got.map(|p| p.configure), Some("^t-".to_string()));
}
