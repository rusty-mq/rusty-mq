//! §13.2 wiring: a config file must change live server behavior — not
//! just validate. The compat switch flip is the end-to-end proof (a
//! shared transient queue is 406 under defaults, 200 with the switch),
//! plus the translation layer itself.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lapin::{options::QueueDeclareOptions, types::FieldTable, Connection, ConnectionProperties};

fn write_config(body: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rmq-cfg-apply-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("apply.toml");
    std::fs::write(&path, body).unwrap();
    path
}

#[test]
fn translations_match_the_file() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .canonicalize()
        .unwrap();
    let cfg = rusty_mq::config::load_file(&root.join("deploy/config.example.toml")).unwrap();
    assert_eq!(cfg.protocol_limits().max_frame_max, 131_072);
    assert_eq!(cfg.protocol_limits().max_channel_max, 256);
    assert_eq!(cfg.protocol_limits().heartbeat_seconds, 60);
    assert_eq!(cfg.protocol_limits().max_message_bytes, 16 * 1024 * 1024);
    assert_eq!(cfg.journal_config().segment_bytes, 268_435_456);
    assert_eq!(cfg.journal_config().commit_batch_delay_ms, 2);
    assert!(!cfg.compatibility().allow_transient_nonexclusive_queues);
    assert!(cfg.compatibility().reject_unknown_arguments);
    let (disk_bytes, ratio) = cfg.alarm_settings();
    assert_eq!(disk_bytes, 1_073_741_824);
    assert!((ratio - 0.10).abs() < 1e-9);
}
#[tokio::test(flavor = "multi_thread")]
async fn compatibility_switch_flips_live_server_behavior() {
    let cfg_path = write_config(
        "[compatibility]\nallow_transient_nonexclusive_queues = true\n[storage]\nsegment_bytes = 8388608\n",
    );
    let cfg = rusty_mq::config::load_file(&cfg_path).unwrap();
    let data_dir = cfg_path.parent().unwrap().join("data");
    let broker = Arc::new(rusty_mq::Broker::open_persistent_from_config(
        "guest".into(),
        "guest".into(),
        &data_dir,
        &cfg,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener, broker));

    let uri = format!("amqp://guest:guest@{addr}/%2F");
    let conn = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .unwrap()
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    // 406 under the default profile; the config switch must make it pass.
    ch.queue_declare(
        "shared-transient.via-config".into(),
        QueueDeclareOptions::default(), // transient + non-exclusive
        FieldTable::default(),
    )
    .await
    .expect("config switch must permit shared transient queues");
    let _ = conn.close(200, "bye".into()).await;

    // And the inverse: a default broker still refuses it (the switch is
    // per-broker state, not global).
    let plain = Arc::new(rusty_mq::Broker::new("guest".into(), "guest".into()));
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener2, plain));
    let uri2 = format!("amqp://guest:guest@{addr2}/%2F");
    let conn2 = Connection::connect(&uri2, ConnectionProperties::default())
        .await
        .unwrap();
    let ch2 = conn2.create_channel().await.unwrap();
    let err = ch2
        .queue_declare(
            "shared-transient.default".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("default profile still refuses shared transient queues");
    assert!(
        err.to_string().to_lowercase().contains("precondition"),
        "got: {err}"
    );
    let _ = conn2.close(200, "bye".into()).await;
    let _ = std::fs::remove_dir_all(cfg_path.parent().unwrap());
}
