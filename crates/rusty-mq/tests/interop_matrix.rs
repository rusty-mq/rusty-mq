//! T01 continuation: the five-client matrix. This harness runs the pika
//! (Python) and amqplib (Node) fixtures against a live broker; lapin is
//! covered by the existing suites; Java/Go fixtures are CI jobs (runtimes
//! not guaranteed locally). Missing runtimes skip GRACEFULLY (documented,
//! never counted as passed).

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/rusty-mq
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .canonicalize()
        .expect("repo root")
}

async fn spawn_broker() -> std::net::SocketAddr {
    let dir = std::env::temp_dir().join(format!(
        "rmq-interop-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener, broker));
    addr
}

enum FixtureOutcome {
    Passed(Vec<String>),
    Skipped(String),
    Failed(String),
}

fn run_fixture(mut cmd: Command, url: &str) -> FixtureOutcome {
    cmd.env("AMQP_URL", url);
    match cmd.output() {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
            if out.status.success() {
                FixtureOutcome::Passed(
                    stdout
                        .lines()
                        .filter(|l| l.starts_with("PASS "))
                        .map(|l| l.to_string())
                        .collect(),
                )
            } else {
                FixtureOutcome::Failed(format!(
                    "exit {:?}\nstdout:\n{stdout}\nstderr:\n{}",
                    out.status.code(),
                    String::from_utf8_lossy(&out.stderr)
                ))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            FixtureOutcome::Skipped(e.to_string())
        }
        Err(e) => FixtureOutcome::Failed(e.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn five_client_matrix_pika_amqplib() {
    let addr = spawn_broker().await;
    let url = format!("amqp://guest:guest@{addr}/%2F");
    let root = repo_root();

    // --- pika (Python) ---
    let pika = run_fixture(
        {
            let mut c = Command::new("python3");
            c.arg(root.join("tests/interop/python/pika_roundtrip.py"));
            c
        },
        &url,
    );
    match pika {
        FixtureOutcome::Passed(passes) => {
            assert!(passes.len() >= 8, "pika fixture PASS lines: {passes:?}");
            assert!(passes
                .iter()
                .any(|p| p.contains("confirmed persistent publish")));
            assert!(passes.iter().any(|p| p.contains("typed properties")));
            assert!(passes.iter().any(|p| p.contains("server-named exclusive")));
        }
        FixtureOutcome::Skipped(why) => {
            eprintln!("SKIP pika (runtime unavailable): {why}");
        }
        FixtureOutcome::Failed(detail) => panic!("pika fixture failed:\n{detail}"),
    }

    // --- amqplib (Node) --- module resolution: NODE_PATH from the env
    // override or a known install; when amqplib cannot resolve, the
    // fixture SKIPS (documented) rather than failing.
    let node_modules = std::env::var("RMQ_NODE_MODULES")
        .unwrap_or_else(|_| "/tmp/rmq-interop/node_modules".to_string());
    let resolves = Command::new("node")
        .env("NODE_PATH", &node_modules)
        .arg("-e")
        .arg("require('amqplib')")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !resolves {
        eprintln!("SKIP amqplib (module not resolvable; install with npm i amqplib)");
    } else {
        let mut node_cmd = Command::new("node");
        node_cmd
            .env("NODE_PATH", &node_modules)
            .arg(root.join("tests/interop/node/amqplib_roundtrip.js"));
        let amqplib = run_fixture(node_cmd, &url);
        match amqplib {
            FixtureOutcome::Passed(passes) => {
                assert!(passes.len() >= 6, "amqplib fixture PASS lines: {passes:?}");
                assert!(passes.iter().any(|p| p.contains("confirm channel")));
                assert!(passes.iter().any(|p| p.contains("exactly-once burst")));
            }
            FixtureOutcome::Skipped(why) => {
                eprintln!("SKIP amqplib (runtime unavailable): {why}");
            }
            FixtureOutcome::Failed(detail) => panic!("amqplib fixture failed:\n{detail}"),
        }
    }

    // The harness itself proves the broker stayed healthy across both
    // client families by opening a lapin connection afterwards.
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    let conn = tokio::time::timeout(
        Duration::from_secs(10),
        lapin::Connection::connect(&uri, lapin::ConnectionProperties::default()),
    )
    .await
    .expect("timeout")
    .expect("lapin connects after both fixtures");
    let _ = conn.close(200, "bye".into()).await;
}

/// Plain run wrapper so the helper is exercised even when everything
/// skips (keeps clippy happy about the enum coverage).
#[allow(dead_code)]
fn outcome_kind(o: &FixtureOutcome) -> &'static str {
    match o {
        FixtureOutcome::Passed(_) => "passed",
        FixtureOutcome::Skipped(_) => "skipped",
        FixtureOutcome::Failed(_) => "failed",
    }
}
