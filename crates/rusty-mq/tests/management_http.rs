//! §12.1 slice: native HTTP management API — health, status, capabilities,
//! metrics, queue inspection with real counts, users/credentials/permissions
//! CRUD with role-gated Basic auth (FR-S04). Tested via tower oneshot (no
//! sockets).

use std::sync::Arc;

use axum::body::Body;
use http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

use rusty_mq_management::broker_facade::BrokerHandle as _;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-http-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

static DIR_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn broker() -> Arc<rusty_mq::Broker> {
    let n = DIR_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &unique_dir(&format!("b{n}")),
    ))
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    auth: Option<(&str, &str)>,
    body: Option<serde_json::Value>,
) -> (http::StatusCode, serde_json::Value, String) {
    let mut req = Request::builder().method(method).uri(path);
    if let Some((u, p)) = auth {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
        req = req.header("authorization", format!("Basic {encoded}"));
    }
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let body = body
        .map(|b| Body::from(serde_json::to_string(&b).unwrap()))
        .unwrap_or_else(Body::empty);
    let response = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    let _ = headers;
    (status, json, text)
}

#[tokio::test]
async fn health_and_capabilities_are_public() {
    let broker = broker();
    let app = rusty_mq_management::router(broker);

    let (status, body, _) = call(&app, "GET", "/health/live", None, None).await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "live");

    let (status, body, _) = call(&app, "GET", "/health/ready", None, None).await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ready");

    let (status, body, _) = call(&app, "GET", "/v1/capabilities", None, None).await;
    assert_eq!(status, 200);
    assert_eq!(body["product"], "rusty-mq");
    assert_eq!(body["features"]["publisher_confirms"], true);
    assert_eq!(
        body["features"]["quorum_queues"], false,
        "never advertise unsupported"
    );
}

#[tokio::test]
async fn reads_require_monitor_role() {
    let broker = broker();
    // An ordinary user cannot read /v1/status or queues.
    broker
        .create_user("ord", "ord-pass", rusty_mq::Role::Ordinary)
        .unwrap();
    let app = rusty_mq_management::router(broker.clone());

    let (status, _, _) = call(&app, "GET", "/v1/status", None, None).await;
    assert_eq!(status, 401, "no credentials -> unauthorized");

    let (status, body, _) = call(&app, "GET", "/v1/status", Some(("ord", "ord-pass")), None).await;
    assert_eq!(status, 403, "ordinary role refused");
    assert_eq!(body["error"]["code"], "forbidden");

    let (status, body, _) = call(
        &app,
        "GET",
        "/v1/status",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn user_lifecycle_credentials_and_permissions() {
    let broker = broker();
    let app = rusty_mq_management::router(broker.clone());

    // Create a monitor user.
    let (status, _, _) = call(
        &app,
        "POST",
        "/v1/users",
        Some(("admin", "admin-secret")),
        Some(serde_json::json!({
            "username": "mon",
            "password": "mon-pass",
            "role": "monitor"
        })),
    )
    .await;
    assert_eq!(status, 201);

    // Bad role rejected.
    let (status, body, _) = call(
        &app,
        "POST",
        "/v1/users",
        Some(("admin", "admin-secret")),
        Some(serde_json::json!({
            "username": "x", "password": "y", "role": "superuser"
        })),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(body["error"]["code"], "bad_request");

    // Listing never exposes password material.
    let (status, body, _) = call(
        &app,
        "GET",
        "/v1/users",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 200);
    let users = body["users"].as_array().unwrap();
    assert!(users.iter().any(|u| u["username"] == "mon"));
    assert!(!body.to_string().contains("mon-pass"));
    assert!(!body.to_string().contains("$argon2"));

    // Grants: set + read + verify through the AMQP auth path.
    let (status, _, _) = call(
        &app,
        "PUT",
        "/v1/permissions/mon/%2F",
        Some(("admin", "admin-secret")),
        Some(serde_json::json!({
            "configure": "^t-",
            "write": ".*",
            "read": ".*"
        })),
    )
    .await;
    assert_eq!(status, 204);
    let (status, body, _) = call(
        &app,
        "GET",
        "/v1/permissions/mon/%2F",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["configure"], "^t-");

    // Invalid regex refused at set time.
    let (status, body, _) = call(
        &app,
        "PUT",
        "/v1/permissions/mon/%2F",
        Some(("admin", "admin-secret")),
        Some(serde_json::json!({
            "configure": "([", "write": ".*", "read": ".*"
        })),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(body["error"]["code"], "bad_request");

    // Credential rotation: old fails, new works.
    let (status, _, _) = call(
        &app,
        "PUT",
        "/v1/users/mon/credentials",
        Some(("admin", "admin-secret")),
        Some(serde_json::json!({ "password": "rotated" })),
    )
    .await;
    assert_eq!(status, 204);
    assert!(!broker.authenticate("mon", "mon-pass"));
    assert!(broker.authenticate("mon", "rotated"));

    // Delete grants then user; 404s afterwards.
    let (status, _, _) = call(
        &app,
        "DELETE",
        "/v1/permissions/mon/%2F",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _, _) = call(
        &app,
        "DELETE",
        "/v1/users/mon",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _, _) = call(
        &app,
        "GET",
        "/v1/permissions/mon/%2F",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn queues_listing_and_purge_with_real_counts() {
    let dir = std::env::temp_dir().join(format!("rmq-http-q-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &dir,
    ));
    let app = rusty_mq_management::router(broker.clone());

    // Durable queue + three persistent messages + one consumer.
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let b2 = broker.clone();
        tokio::spawn(rusty_mq::server::serve_listener_shared(listener, b2));
        let uri = format!("amqp://admin:admin-secret@{addr}/%2F");
        let conn = lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
            .await
            .unwrap();
        let ch = conn.create_channel().await.unwrap();
        ch.queue_declare(
            "http.q".into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
        for i in 0..3u8 {
            ch.basic_publish(
                "".into(),
                "http.q".into(),
                lapin::options::BasicPublishOptions::default(),
                &[i],
                lapin::BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap();
        }
        // Wait for admission (no confirm mode: poll the API).
        for _ in 0..50 {
            let (_, body, _) = call(
                &app,
                "GET",
                "/v1/vhosts/%2F/queues",
                Some(("admin", "admin-secret")),
                None,
            )
            .await;
            if body["queues"][0]["ready_messages"] == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    let (status, body, _) = call(
        &app,
        "GET",
        "/v1/vhosts/%2F/queues",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 200);
    let queues = body["queues"].as_array().unwrap();
    let row = queues
        .iter()
        .find(|q| q["name"] == "http.q")
        .expect("queue listed");
    assert_eq!(row["ready_messages"], 3);
    assert_eq!(row["durable"], true);

    // Purge via the API, re-check the count.
    let (status, body, _) = call(
        &app,
        "POST",
        "/v1/vhosts/%2F/queues/http.q/purge",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["purged"], 3);
    let (_, body, _) = call(
        &app,
        "GET",
        "/v1/vhosts/%2F/queues",
        Some(("admin", "admin-secret")),
        None,
    )
    .await;
    let row = body["queues"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["name"] == "http.q")
        .unwrap();
    assert_eq!(row["ready_messages"], 0);
}

#[tokio::test]
async fn metrics_text_endpoint() {
    let broker = broker();
    let app = rusty_mq_management::router(broker);
    let (status, _, text) = call(&app, "GET", "/metrics", None, None).await;
    assert_eq!(status, 200);
    assert!(text.contains("rusty_mq_messages_published_total"));
    assert!(text.contains("# TYPE rusty_mq_ready_messages gauge"));
}
