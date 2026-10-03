//! M7-5 slice: connection registry surface + admin CLI. Live connections
//! list with usernames, operator close (server-initiated
//! connection.close), revocation closing the user's live connections
//! (FR-S08's missing half), permissions listing, and the CLI admin client
//! exercised against a real spawned server.

use std::sync::Arc;
use std::time::Duration;

use lapin::{Connection as LapinConnection, ConnectionProperties};

use rusty_mq::admin_client::{request, AdminCredentials};
use rusty_mq_management::broker_facade::BrokerHandle as _;

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-admin-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn spawn() -> (
    std::net::SocketAddr,
    Arc<rusty_mq::Broker>,
    tokio::task::JoinHandle<()>,
) {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &dir(&format!("srv{n}")),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = broker.clone();
    let task = tokio::spawn(rusty_mq::server::serve_listener_shared(listener, b));
    (addr, broker, task)
}

/// Channel creation on a connection the server is closing asynchronously:
/// poll to a deadline instead of assuming the close has already landed
/// (loaded CI runners exposed the race — ci#11+).
async fn channel_eventually_rejected(conn: &LapinConnection) {
    let deadline = tokio::time::Instant::now() + timeout_secs();
    loop {
        if conn.create_channel().await.is_err() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "channel still accepted after the close deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Registry usernames are enriched post-auth; poll until both are present.
async fn registry_shows(broker: &Arc<rusty_mq::Broker>, want: &[&str]) {
    let deadline = tokio::time::Instant::now() + timeout_secs();
    loop {
        let conns = rusty_mq::Broker::list_connections(broker);
        let users: Vec<String> = conns.iter().map(|(_, u)| u.clone()).collect();
        if want.iter().all(|w| users.iter().any(|u| u == w)) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "registry never showed {want:?} (last: {users:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]

async fn connections_list_operator_close_and_revocation_close() {
    let (addr, broker, _server) = spawn().await;

    // Two users, two live connections.
    broker
        .create_user("alice", "alice-pass", rusty_mq::Role::Ordinary)
        .unwrap();
    broker
        .set_permissions(
            "alice",
            "/",
            rusty_mq::Permissions {
                configure: ".*".into(),
                write: ".*".into(),
                read: ".*".into(),
            },
        )
        .unwrap();

    let alice_uri = format!("amqp://alice:alice-pass@{addr}/%2F");
    let alice = LapinConnection::connect(&alice_uri, ConnectionProperties::default())
        .await
        .unwrap();
    let admin_uri = format!("amqp://admin:admin-secret@{addr}/%2F");
    let admin = LapinConnection::connect(&admin_uri, ConnectionProperties::default())
        .await
        .unwrap();

    // Registry carries real usernames post-auth (enrichment races the
    // open-ok the client saw — poll).
    registry_shows(&broker, &["alice", "admin"]).await;

    // Operator close of alice's connection: her channel future fails.
    let alice_id = rusty_mq::Broker::list_connections(&broker)
        .into_iter()
        .find(|(_, u)| u == "alice")
        .map(|(id, _)| id)
        .unwrap();
    assert!(rusty_mq::Broker::close_connection(
        &broker,
        alice_id,
        "operator test",
    ));
    channel_eventually_rejected(&alice).await;

    // Revocation closes admin's remaining connection (FR-S08).
    broker.delete_user("admin").unwrap();
    channel_eventually_rejected(&admin).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_close_endpoint_and_permissions_listing() {
    use axum::body::Body;
    use http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let (addr, broker, _server) = spawn().await;
    // A live connection to close via HTTP.
    let uri = format!("amqp://admin:admin-secret@{addr}/%2F");
    let conn = LapinConnection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let make_app = || rusty_mq_management::router(broker.clone());

    // Permissions listing reflects bootstrap grants.
    let resp = make_app()
        .oneshot(
            Request::builder()
                .uri("/v1/permissions")
                .header("authorization", basic("admin", "admin-secret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let rows = body["permissions"].as_array().unwrap();
    assert!(rows
        .iter()
        .any(|r| r["username"] == "admin" && r["vhost"] == "/"));

    // Close the live connection via the endpoint.
    let id = rusty_mq::Broker::list_connections(&broker)[0]
        .0
        .to_raw()
        .to_string();
    let path = format!("/v1/connections/{id}/close");
    let resp = make_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", basic("admin", "admin-secret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    channel_eventually_rejected(&conn).await;

    // Unknown id -> 404.
    let resp = make_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/connections/999999/close")
                .header("authorization", basic("admin", "admin-secret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_admin_client_round_trip() {
    let (_addr, broker, _server) = spawn().await;
    // Spawn the management API on a real port for the CLI client.
    let mgmt = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgmt_addr = mgmt.local_addr().unwrap();
    let b = broker.clone();
    tokio::spawn(async move {
        let app = rusty_mq_management::router(b);
        let _ = axum::serve(mgmt, app).await;
    });

    let creds = AdminCredentials {
        user: "admin".into(),
        password: "admin-secret".into(),
    };
    let base = format!("http://{mgmt_addr}");

    // status
    let resp = request(&base, "GET", "/v1/status", &creds, None)
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.json()["version"], env!("CARGO_PKG_VERSION"));

    // create user -> listed -> credentials rotated -> permissions set/listed
    let resp = request(
        &base,
        "POST",
        "/v1/users",
        &creds,
        Some(&serde_json::json!({
            "username": "cli-user", "password": "cli-pass", "role": "monitor"
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp.status, 201);
    let resp = request(&base, "GET", "/v1/users", &creds, None)
        .await
        .unwrap();
    let users = resp.json()["users"].as_array().unwrap().clone();
    assert!(users.iter().any(|u| u["username"] == "cli-user"));

    let resp = request(
        &base,
        "PUT",
        "/v1/users/cli-user/credentials",
        &creds,
        Some(&serde_json::json!({ "password": "rotated" })),
    )
    .await
    .unwrap();
    assert_eq!(resp.status, 204);
    assert!(!broker.authenticate("cli-user", "cli-pass"));
    assert!(broker.authenticate("cli-user", "rotated"));

    let resp = request(
        &base,
        "PUT",
        "/v1/permissions/cli-user/%2F",
        &creds,
        Some(&serde_json::json!({
            "configure": "^cli-", "write": ".*", "read": ".*"
        })),
    )
    .await
    .unwrap();
    assert_eq!(resp.status, 204);
    let resp = request(&base, "GET", "/v1/permissions", &creds, None)
        .await
        .unwrap();
    let rows = resp.json()["permissions"].as_array().unwrap().clone();
    assert!(rows
        .iter()
        .any(|r| r["username"] == "cli-user" && r["configure"] == "^cli-"));

    // queues listing with a URL-encoded vhost.
    let resp = request(&base, "GET", "/v1/vhosts/%2F/queues", &creds, None)
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert!(resp.json()["queues"].is_array());
}

fn basic(user: &str, pass: &str) -> String {
    use base64::Engine;
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
    )
}

#[allow(dead_code)]
fn timeout_secs() -> Duration {
    Duration::from_secs(10)
}
