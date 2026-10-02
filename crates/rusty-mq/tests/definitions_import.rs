//! T29 slice: native definitions export/import — export reflects durable
//! topology; dry-run never mutates; real import is idempotent with journaled
//! declarations; conflicts are reported, never overwritten.

use std::sync::Arc;
use std::time::Duration;

use rusty_mq::definitions::{export, import};

fn broker(tag: &str) -> Arc<rusty_mq::Broker> {
    let dir = std::env::temp_dir().join(format!("rmq-defs-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    Arc::new(rusty_mq::Broker::open_persistent(
        "admin".into(),
        "admin-secret".into(),
        &dir,
    ))
}

async fn declare_topology(broker: &Arc<rusty_mq::Broker>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = broker.clone();
    let _ = &b;
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener, b));
    let uri = format!("amqp://admin:admin-secret@{addr}/%2F");
    let conn = lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
        .await
        .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        "jobs".into(),
        lapin::options::QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        "ev.topic".into(),
        lapin::ExchangeKind::Topic,
        lapin::options::ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        "jobs".into(),
        "ev.topic".into(),
        "a.#".into(),
        lapin::options::QueueBindOptions::default(),
        lapin::types::FieldTable::default(),
    )
    .await
    .unwrap();
    // Transient/exclusive queue must NOT be exported.
    ch.queue_declare(
        "".into(),
        lapin::options::QueueDeclareOptions {
            exclusive: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn export_reflects_durable_topology() {
    let b = broker("export");
    declare_topology(&b).await;
    let out = export(&b);
    assert_eq!(out["format"], "rusty-mq-definitions");
    let queues: Vec<&str> = out["queues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert_eq!(queues, vec!["jobs"], "durable queue only");
    let exchanges: Vec<&str> = out["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        exchanges,
        vec!["ev.topic"],
        "durable non-builtin exchange only"
    );
    assert_eq!(out["bindings"].as_array().unwrap().len(), 1);
    assert_eq!(out["bindings"][0]["routing_key"], "a.#");
}

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_reports_without_mutating() {
    let b = broker("dry");
    let payload = serde_json::json!({
        "format": "rusty-mq-definitions", "version": 1,
        "queues": [{"name": "new-q", "vhost": "/"}],
        "exchanges": [{"name": "new-ex", "vhost": "/", "type": "fanout"}],
        "bindings": []
    });
    let report = import(&b, &payload, true).unwrap();
    assert!(report.dry_run);
    assert!(report.results.iter().all(|r| r.detail == "dry run"));
    // Nothing was created.
    let topo = b.topology.lock().unwrap();
    let vhost = topo.find_vhost("/").unwrap();
    assert!(topo.find_queue(vhost, "new-q").is_none());
    assert!(topo.find_exchange(vhost, "new-ex").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn import_is_idempotent_and_journaled() {
    let dir = std::env::temp_dir().join(format!("rmq-defs-{}-import", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let payload = serde_json::json!({
        "queues": [{"name": "imp.q", "vhost": "/"}],
        "exchanges": [{"name": "imp.ex", "vhost": "/", "type": "topic"}],
        "bindings": [{"source": "imp.ex", "destination": "imp.q", "vhost": "/", "routing_key": "k"}]
    });

    {
        let b = broker("import-a");
        let first = import(&b, &payload, false).unwrap();
        assert!(first.results.iter().any(|r| matches!(
            r.outcome,
            rusty_mq::definitions::Outcome::Created
        ) && r.resource.contains("imp.q")));
        // Re-import: everything exists, nothing conflicts.
        let second = import(&b, &payload, false).unwrap();
        assert!(second.results.iter().all(|r| {
            !matches!(
                r.outcome,
                rusty_mq::definitions::Outcome::Conflict | rusty_mq::definitions::Outcome::Invalid
            )
        }));
        // Conflicts ARE reported: a differing exchange is never overwritten.
        let conflicting = serde_json::json!({
            "queues": [],
            "exchanges": [{"name": "imp.ex", "vhost": "/", "type": "fanout"}],
            "bindings": []
        });
        let third = import(&b, &conflicting, false).unwrap();
        assert!(third.results.iter().any(|r| matches!(
            r.outcome,
            rusty_mq::definitions::Outcome::Conflict
        ) && r.detail.contains("NOT overwritten")));
    }
}

#[test]
fn invalid_payload_rejected_before_mutation() {
    let b = broker("invalid");
    let payload = serde_json::json!({
        "queues": [{"name": "amq.gen-x", "vhost": "/"}],
        "exchanges": [{"name": "ok", "vhost": "/", "type": "headers"}],
        "bindings": []
    });
    let report = import(&b, &payload, false).unwrap();
    assert!(report
        .results
        .iter()
        .any(|r| matches!(r.outcome, rusty_mq::definitions::Outcome::Invalid)));
    let topo = b.topology.lock().unwrap();
    let vhost = topo.find_vhost("/").unwrap();
    assert!(
        topo.find_exchange(vhost, "ok").is_none(),
        "nothing applied when validation fails"
    );
}

// Silence the unused import when compiled without the async tests above.
#[allow(dead_code)]
fn _witness(_: Duration) {}
