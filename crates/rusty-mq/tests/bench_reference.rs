//! §14 reference benchmark + T28 soak slice. Env-gated so the normal
//! suite stays fast:
//!
//! - RMQ_BENCH_SECS=<n>  run the §14.1 profile for n seconds per phase
//!   (default skip). 4 publishers / 4 consumers, one durable queue +
//!   direct exchange, 1 KiB payloads, persistent mode with confirms,
//!   prefetch 100, manual acks; bounded confirm window (§14.1). Reports
//!   aggregate throughput and confirm-latency percentiles as JSON.
//! - RMQ_SOAK_CYCLES=<n> run the churn soak for n cycles (default skip):
//!   publish → consume-with-ack cycles plus queue churn under a tiny
//!   compaction threshold; asserts the journal stays bounded.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, PublisherConfirm,
};

const PAYLOAD_LEN: usize = 1024;
const PREFETCH: u16 = 100;
const CONFIRM_WINDOW: usize = 1000;

fn env_num(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn bench_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-bench-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn spawn_persistent(
    dir: std::path::PathBuf,
) -> (std::net::SocketAddr, Arc<rusty_mq::Broker>) {
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
    (addr, broker)
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::test(flavor = "multi_thread")]
async fn reference_profile_benchmark() {
    let Some(secs) = env_num("RMQ_BENCH_SECS") else {
        eprintln!("SKIP bench (set RMQ_BENCH_SECS=<seconds> to run)");
        return;
    };
    let dir = bench_dir("ref");
    let (addr, _broker) = spawn_persistent(dir.clone()).await;
    let uri = format!("amqp://guest:guest@{addr}/%2F");

    // Shared topology (§14.1: one direct exchange, one durable queue).
    let setup = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let sch = setup.create_channel().await.unwrap();
    sch.queue_declare(
        "bench.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    sch.exchange_declare(
        "bench.direct".into(),
        lapin::ExchangeKind::Direct,
        lapin::options::ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    sch.queue_bind(
        "bench.q".into(),
        "bench.direct".into(),
        "rk".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    drop(setup);

    let confirmed = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let latencies_us = Arc::new(std::sync::Mutex::new(Vec::<u128>::new()));

    // 4 consumers: prefetch 100, manual ack.
    let mut consumer_tasks = Vec::new();
    for _ in 0..4 {
        let uri = uri.clone();
        let consumed = consumed.clone();
        consumer_tasks.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();
            ch.basic_qos(PREFETCH, lapin::options::BasicQosOptions::default())
                .await
                .unwrap();
            let mut consumer = ch
                .basic_consume(
                    "bench.q".into(),
                    "".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();
            while let Some(Ok(d)) = consumer.next().await {
                consumed.fetch_add(1, Ordering::Relaxed);
                d.acker.ack(BasicAckOptions::default()).await.unwrap();
            }
        }));
    }

    // Warm-up (§14.1: 60s full, scaled here for short runs).
    let warmup = Duration::from_secs((secs / 6).max(1));
    run_publishers(&uri, warmup, &confirmed, None).await;
    let warm_confirmed = confirmed.load(Ordering::Relaxed);

    // Measured phase.
    let start = Instant::now();
    let lat_sink = latencies_us.clone();
    run_publishers(&uri, Duration::from_secs(secs), &confirmed, Some(&lat_sink)).await;
    let elapsed = start.elapsed();

    // Drain: consumers keep acking; wait briefly for the tail.
    let measured_confirmed = confirmed.load(Ordering::Relaxed) - warm_confirmed;
    let deadline = Instant::now() + Duration::from_secs(10);
    while consumed.load(Ordering::Relaxed) < confirmed.load(Ordering::Relaxed)
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for t in consumer_tasks {
        t.abort();
    }

    let mut latencies_us = latencies_us.lock().unwrap().clone();
    latencies_us.sort_unstable();
    let throughput = measured_confirmed as f64 / elapsed.as_secs_f64();
    let result = serde_json::json!({
        "profile": "reference-14.1",
        "payload_bytes": PAYLOAD_LEN,
        "publishers": 4,
        "consumers": 4,
        "prefetch": PREFETCH,
        "mode": "persistent+confirms",
        "warmup_seconds": warmup.as_secs(),
        "measure_seconds": elapsed.as_secs_f64(),
        "confirmed_total": measured_confirmed,
        "consumed_total": consumed.load(Ordering::Relaxed),
        "throughput_confirmed_per_sec": throughput,
        "confirm_latency_us": {
            "p50": percentile(&latencies_us, 0.50),
            "p99": percentile(&latencies_us, 0.99),
            "p999": percentile(&latencies_us, 0.999),
            "max": latencies_us.last().copied().unwrap_or(0),
        },
        "note": "short-run harness; §14.1 full runs (60s warmup, 5min measure, x3) execute in nightly CI",
    });

    let out_dir =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/results");
    std::fs::create_dir_all(&out_dir).unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let path = out_dir.join(format!("reference-{stamp}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&result).unwrap()).unwrap();
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    assert!(
        consumed.load(Ordering::Relaxed) > 0,
        "consumers must consume during the benchmark"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

async fn run_publishers(
    uri: &str,
    duration: Duration,
    confirmed: &Arc<AtomicU64>,
    latencies: Option<&Arc<std::sync::Mutex<Vec<u128>>>>,
) {
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let uri = uri.to_string();
        let confirmed = confirmed.clone();
        let latency_sink = latencies.cloned();
        tasks.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .unwrap();
            let payload = vec![7u8; PAYLOAD_LEN];
            let deadline = Instant::now() + duration;
            let mut window: Vec<(Instant, PublisherConfirm)> = Vec::new();
            let mut local_latencies: Vec<u128> = Vec::new();
            while Instant::now() < deadline {
                let sent_at = Instant::now();
                let confirm = ch
                    .basic_publish(
                        "bench.direct".into(),
                        "rk".into(),
                        BasicPublishOptions::default(),
                        payload.as_slice(),
                        BasicProperties::default().with_delivery_mode(2),
                    )
                    .await
                    .unwrap();
                window.push((sent_at, confirm));
                if window.len() >= CONFIRM_WINDOW {
                    // Bounded window (§14.1): drain before continuing.
                    for (at, c) in window.drain(..) {
                        let c = c.await.unwrap();
                        debug_assert!(matches!(c, lapin::Confirmation::Ack(_)));
                        confirmed.fetch_add(1, Ordering::Relaxed);
                        local_latencies.push(at.elapsed().as_micros());
                    }
                }
            }
            for (at, c) in window.drain(..) {
                let c = c.await.unwrap();
                debug_assert!(matches!(c, lapin::Confirmation::Ack(_)));
                confirmed.fetch_add(1, Ordering::Relaxed);
                local_latencies.push(at.elapsed().as_micros());
            }
            if let Some(sink) = latency_sink {
                sink.lock().unwrap().extend_from_slice(&local_latencies);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

/// T28 soak slice: churn with compaction — journal bytes must stay bounded
/// (no unbounded growth through declare/publish/consume/delete cycles).
///
/// Long runs: `RMQ_SOAK_MIN_SECS` keeps cycling until the wall-clock floor
/// is met (the cycle cap still applies), progress prints every 500 cycles,
/// and the journal bound is re-checked every 2,000 cycles so unbounded
/// growth fails fast instead of after a day of churning.
#[tokio::test(flavor = "multi_thread")]
async fn churn_soak_journal_bounded() {
    let Some(cycles_cap) = env_num("RMQ_SOAK_CYCLES") else {
        eprintln!("SKIP soak (set RMQ_SOAK_CYCLES=<n> to run)");
        return;
    };
    let min_secs = env_num("RMQ_SOAK_MIN_SECS");
    let started = std::time::Instant::now();
    let dir = bench_dir("soak");
    let (addr, broker) = spawn_persistent(dir.clone()).await;
    // Tiny compaction threshold: compaction runs constantly.
    broker.set_compact_threshold(1);
    let uri = format!("amqp://guest:guest@{addr}/%2F");

    let conn = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let payload = vec![1u8; 256];

    let mut cycle: u64 = 0;
    while cycle < cycles_cap {
        // Wall-clock floor: keep churning until it is met (cycle cap first).
        if min_secs.is_some_and(|m| started.elapsed().as_secs() >= m) && cycle > 0 {
            break;
        }
        let qname = format!("soak.{cycle}");
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
        for i in 0..20u32 {
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
            let c = confirm.await.unwrap();
            assert!(matches!(c, lapin::Confirmation::Ack(_)));
            let _ = i;
        }
        // Drain with get+ack.
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
        cycle += 1;

        if cycle % 500 == 0 {
            let bytes = rusty_mq_storage::snapshot::journal_bytes(&dir);
            eprintln!(
                "SOAK progress: {cycle} cycles, {:?} elapsed, journal {bytes} bytes",
                started.elapsed()
            );
        }
        if cycle % 2_000 == 0 {
            // Fail fast: reclaim now and check the bound mid-run, so a
            // compaction leak surfaces in minutes, not at the 24h finish.
            broker.compact().unwrap();
            let bytes = rusty_mq_storage::snapshot::journal_bytes(&dir);
            assert!(
                bytes < 1024 * 1024,
                "journal grew to {bytes} bytes at cycle {cycle} — compaction not reclaiming"
            );
        }
    }
    if min_secs.is_some() {
        eprintln!(
            "SOAK finished {cycle} cycles in {:?} (floor was {min_secs:?}s)",
            started.elapsed()
        );
    }

    // Boundedness: after full churn with settles+deletes+compaction, the
    // journal must not grow with cycles. With everything settled and
    // compacted repeatedly, bytes stay in a small band.
    tokio::time::sleep(Duration::from_millis(300)).await;
    broker.compact().unwrap();
    let bytes = rusty_mq_storage::snapshot::journal_bytes(&dir);
    // All state is deleted; a snapshot exists; the live journal should be
    // tiny (only the final snapshot-triggering suffix).
    assert!(
        bytes < 1024 * 1024,
        "journal grew to {bytes} bytes after churn — compaction not reclaiming"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
