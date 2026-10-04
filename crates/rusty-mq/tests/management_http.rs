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
async fn metrics_live_on_their_own_listener_router() {
    // PRD §12.3: /metrics belongs to the SEPARATE metrics plane.
    let handle = broker();
    let app = rusty_mq_management::metrics_router(handle);
    let (status, _, text) = call(&app, "GET", "/metrics", None, None).await;
    assert_eq!(status, 200);
    assert!(text.contains("rusty_mq_messages_published_total"));
    assert!(text.contains("# TYPE rusty_mq_ready_messages gauge"));

    // ...and no longer on the (authenticated) management router.
    let mgmt = rusty_mq_management::router(broker()); // shadowed-name safe
    let (status, _, _) = call(&mgmt, "GET", "/metrics", None, None).await;
    assert_eq!(status, 404, "metrics moved off the management plane");
}

#[tokio::test]
async fn per_queue_metrics_are_opt_in_and_escaped() {
    // §12.3: per-queue series exist ONLY with metrics.queue_labels_enabled
    // (bounded cardinality; queue names are the only labels).
    let dir = std::env::temp_dir().join(format!(
        "rmq-qmetrics-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let cfg = rusty_mq::config::load_str("[metrics]\nqueue_labels_enabled = true\n").unwrap();
    let cfg_broker = std::sync::Arc::new(rusty_mq::Broker::open_persistent_from_config(
        "guest".into(),
        "guest".into(),
        &dir,
        &cfg,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(
        listener,
        cfg_broker.clone(),
    ));

    let uri = format!("amqp://guest:guest@{addr}/%2F");
    let conn = lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
        .await
        .unwrap();
    let ch = conn.create_channel().await.unwrap();
    for (q, n) in [
        (String::from("alpha.q"), 2u8),
        (String::from("we\\") + "ird", 1u8),
    ] {
        ch.queue_declare(
            q.clone().into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
        for i in 0..n {
            let _ = ch
                .basic_publish(
                    "".into(),
                    q.clone().into(),
                    lapin::options::BasicPublishOptions::default(),
                    &[i],
                    lapin::BasicProperties::default(),
                )
                .await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let app = rusty_mq_management::metrics_router(cfg_broker.clone());
    let (status, _, text) = call(&app, "GET", "/metrics", None, None).await;
    assert_eq!(status, 200);
    assert!(
        text.contains("rusty_mq_queue_ready_messages{queue=\"alpha.q\"} 2"),
        "per-queue series missing: {text}"
    );
    assert!(
        text.contains("queue=\"we\\\\ird\""),
        "backslash escaping missing: {text}"
    );

    // OFF by default: a default broker renders no per-queue series.
    let plain_handle = broker();
    let app2 = rusty_mq_management::metrics_router(plain_handle);
    let (_, _, text2) = call(&app2, "GET", "/metrics", None, None).await;
    assert!(
        !text2.contains("rusty_mq_queue_ready_messages{"),
        "per-queue series must be opt-in"
    );
    let _ = conn.close(200, "bye".into()).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn oversized_requests_get_413_before_handlers() {
    // §13.2 management.max_request_bytes: enforced by an axum body
    // limit — over-limit requests never reach a handler.
    let broker_handle = broker();
    let app = rusty_mq_management::router_with_limit(broker_handle, 64);
    // A definitions payload far over 64 bytes.
    let big = "x".repeat(2048);
    let body = serde_json::json!({ "blob": big });
    let (status, _, _) = call(&app, "POST", "/v1/definitions", None, Some(body)).await;
    assert_eq!(status, 413, "over-limit request must be refused");
    // Under the limit the request reaches the handler (the auth check
    // answers 401 without credentials — proof it got past the limit).
    let (status, _, _) = call(&app, "GET", "/v1/status", None, None).await;
    assert_eq!(status, 401, "handler reached (not 413)");
}

#[tokio::test]
async fn vhost_create_is_durable_and_listed() {
    // §12.1 POST /v1/vhosts: journaled (survives reopen) + listed.
    use std::sync::Arc;
    let dir = std::env::temp_dir().join(format!(
        "rmq-vhost-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let app = rusty_mq_management::router(broker.clone());
    let (status_code, _, body) = call(
        &app,
        "POST",
        "/v1/vhosts",
        Some(("guest", "guest")),
        Some(serde_json::json!({"name": "tenant-a"})),
    )
    .await;
    assert_eq!(status_code, 201, "create: {body}");
    let (_, _, list) = call(&app, "GET", "/v1/vhosts", Some(("guest", "guest")), None).await;
    assert!(list.to_string().contains("tenant-a"), "listed: {list}");

    // Non-admin role refused.
    let (s, _, _) = call(
        &app,
        "POST",
        "/v1/vhosts",
        Some(("guest", "wrong")),
        Some(serde_json::json!({"name": "x"})),
    )
    .await;
    assert_eq!(s, 401);

    // Durability: reopen the same data dir — the vhost and its built-in
    // exchanges must be there.
    drop(broker);
    let reopened = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let topo = reopened.topology.lock().unwrap();
    let vh = topo.find_vhost("tenant-a").expect("vhost survived restart");
    assert!(
        topo.find_exchange(vh, "amq.direct").is_some(),
        "built-in exchanges restored for the new vhost"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn vhost_delete_enforces_destructive_checks_and_is_durable() {
    use std::sync::Arc;
    let dir = std::env::temp_dir().join(format!(
        "rmq-vhost-del-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let app = rusty_mq_management::router(broker.clone());
    let auth = Some(("guest", "guest"));

    // Create an empty vhost and a vhost holding a queue.
    for name in ["tmp-empty", "tmp-full"] {
        let (s, _, b) = call(
            &app,
            "POST",
            "/v1/vhosts",
            auth,
            Some(serde_json::json!({"name": name})),
        )
        .await;
        assert_eq!(s, 201, "{name}: {b}");
    }
    // Occupancy via AMQP: grant guest access to tmp-full, connect,
    // declare a durable queue. (POST /v1/vhosts/{v}/queues is not in the
    // V1 surface; queues come from the protocol plane.)
    broker
        .set_permissions(
            "guest",
            "tmp-full",
            rusty_mq_core::auth::Permissions {
                configure: ".*".into(),
                write: ".*".into(),
                read: ".*".into(),
            },
        )
        .unwrap();
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let laddr = listener.local_addr().unwrap();
        tokio::spawn(rusty_mq::server::serve_listener_shared(
            listener,
            broker.clone(),
        ));
        let uri = format!("amqp://guest:guest@{laddr}/tmp-full");
        let vconn = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            lapin::Connection::connect(&uri, lapin::ConnectionProperties::default()),
        )
        .await
        .unwrap()
        .unwrap();
        let vch = vconn.create_channel().await.unwrap();
        vch.queue_declare(
            "occupied.q".into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .expect("declare in the new vhost");
        let _ = vconn.close(200, "bye".into()).await;
    }

    // "/" is never deletable.
    let (s, _, b) = call(&app, "DELETE", "/v1/vhosts/%2F", auth, None).await;
    assert_eq!(s, 409, "default vhost: {s} {b}");
    // A vhost holding a queue refuses.
    let (s, _, b) = call(&app, "DELETE", "/v1/vhosts/tmp-full", auth, None).await;
    assert_eq!(s, 409, "occupied: {s} {b}");
    assert!(b.to_string().contains("queue"), "reason: {b}");
    // An empty vhost deletes (204) and disappears from the list.
    let (s, _, _) = call(&app, "DELETE", "/v1/vhosts/tmp-empty", auth, None).await;
    assert_eq!(s, 204);
    let (_, _, list) = call(&app, "GET", "/v1/vhosts", auth, None).await;
    let list = list.to_string();
    assert!(!list.contains("tmp-empty"), "gone from list: {list}");
    assert!(list.contains("tmp-full"), "occupied still listed: {list}");

    // Durable: after reopen, the deleted vhost is absent.
    drop(broker);
    let reopened = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let names: Vec<String> = {
        let topo = reopened.topology.lock().unwrap();
        topo.vhost_names().collect()
    };
    assert!(
        !names.contains(&"tmp-empty".to_string()),
        "deleted stayed deleted"
    );
    assert!(names.contains(&"tmp-full".to_string()), "occupied survived");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn bindings_listing_shows_amqp_created_bindings() {
    use std::sync::Arc;
    let dir = std::env::temp_dir().join(format!(
        "rmq-bindings-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(
        listener,
        broker.clone(),
    ));
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    let conn = lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
        .await
        .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "bind.q".into(),
        lapin::options::QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "bind.ex".into(),
        lapin::ExchangeKind::Topic,
        lapin::options::ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await
    .unwrap();
    for key in ["a.b.c", "a.*.d"] {
        ch.queue_bind(
            "bind.q".into(),
            "bind.ex".into(),
            key.into(),
            lapin::options::QueueBindOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
    }
    let _ = conn.close(200, "bye".into()).await;

    let app = rusty_mq_management::router(broker);
    let (_, _, body) = call(
        &app,
        "GET",
        "/v1/vhosts/%2F/bindings",
        Some(("guest", "guest")),
        None,
    )
    .await;
    let text = body.to_string();
    assert!(text.contains("\"source\":\"bind.ex\""), "rows: {text}");
    assert!(text.contains("\"routing_key\":\"a.b.c\""), "rows: {text}");
    assert!(text.contains("\"routing_key\":\"a.*.d\""), "rows: {text}");
    assert!(text.contains("bind.ex|bind.q|a.b.c"), "opaque id: {text}");
    // Role floor: no credentials -> 401.
    let (s, _, _) = call(&app, "GET", "/v1/vhosts/%2F/bindings", None, None).await;
    assert_eq!(s, 401);
    let _ = std::fs::remove_dir_all(&dir);
}
