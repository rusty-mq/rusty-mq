//! T28 regression: compaction must reclaim the ACTIVE segment. Found by
//! the 24-hour soak's fail-fast check (attempt 1 failed at cycle 2000
//! with an 18 MB journal): with `segment_bytes` large enough that the
//! writer never rotates, everything lives in one growing segment and
//! reclaim — which keeps the current segment — dropped nothing. The fix
//! seals a fully-covered active segment at compact time so it becomes
//! reclaimable. This test drives that exact shape fast.

use std::time::Duration;

use lapin::{
    options::{BasicAckOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use rusty_mq_storage::journal::JournalConfig;

fn data_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-compaction-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_seals_and_reclaims_the_active_segment() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
    let dir = data_dir("seal");
    // No segment rotation ever: the pre-fix failure shape.
    let journal = JournalConfig {
        segment_bytes: usize::MAX,
        ..Default::default()
    };
    let broker = std::sync::Arc::new(rusty_mq::Broker::open_persistent_with_journal(
        "guest".into(),
        "guest".into(),
        &dir,
        journal,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(
        listener,
        broker.clone(),
    ));

    let uri = format!("amqp://guest:guest@{addr}/%2F");
    let conn = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect timeout")
    .expect("handshake");
    let ch = conn.create_channel().await.unwrap();

    // Enough confirmed churn that a single un-reclaimed segment would be
    // far over the bound: 30 queues x 20 persistent publishes + settles.
    for cycle in 0..30u32 {
        let qname = format!("seal.{cycle}");
        ch.queue_declare(
            qname.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        let payload = vec![7u8; 256]; // soak-shaped bodies
        for _ in 0..20u32 {
            let confirm = ch
                .basic_publish(
                    "".into(),
                    qname.clone().into(),
                    BasicPublishOptions::default(),
                    payload.as_slice(),
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await
                .unwrap();
            confirm.await.expect("confirmed persistent publish");
        }
        loop {
            let got = ch
                .basic_get(
                    qname.clone().into(),
                    lapin::options::BasicGetOptions { no_ack: false },
                )
                .await
                .unwrap();
            match got {
                Some(m) => {
                    m.acker.ack(BasicAckOptions::default()).await.unwrap();
                }
                None => break,
            }
        }
        ch.queue_delete(
            qname.clone().into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await
        .unwrap();
    }

    // Pre-fix: the one active segment holds everything (~hundreds of KB)
    // and survives a forced compact. Post-fix: the covered active segment
    // is sealed, reclaim drops it, and the live journal is a fresh empty
    // segment.
    let before = rusty_mq_storage::snapshot::journal_bytes(&dir);
    assert!(
        before > 128 * 1024,
        "churn must build a meaningful journal first (got {before} bytes)"
    );
    broker.compact().unwrap();
    let after = rusty_mq_storage::snapshot::journal_bytes(&dir);
    assert!(
        after < 64 * 1024,
        "compaction must seal+reclaim the active segment: {before} -> {after} bytes"
    );

    // The compacted directory still recovers identically (sealed segment
    // gone, snapshot + fresh suffix are the recovery root).
    drop(conn);
    let summary = rusty_mq_storage::rebuild::rebuild(&dir, usize::MAX / 2).unwrap();
    assert_eq!(summary.replayed, 0, "snapshot covers everything");

    let _ = std::fs::remove_dir_all(&dir);
}
