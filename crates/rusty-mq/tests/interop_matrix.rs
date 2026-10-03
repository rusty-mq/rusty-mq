//! T01 continuation: the five-client matrix. This harness runs the pika
//! (Python) and amqplib (Node) fixtures against a live broker, plus the
//! Java (RabbitMQ amqp-client) and Go (amqp091-go) fixtures when those
//! toolchains are staged; lapin is covered by the existing suites.
//! Toolchain staging (see tests/interop/README and the interop CI job):
//!   RMQ_JAVA_HOME       JDK home (javac/java under bin/)
//!   RMQ_AMQP_CLIENT_JAR path to the pinned amqp-client jar
//!   RMQ_GO_BIN          go binary
//!   RMQ_GO_MOD          module dir whose go.mod pins amqp091-go
//! Missing runtimes skip GRACEFULLY (documented, never counted as passed).

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

/// Probe whether a command exists and runs (`--version`), mapping spawn
/// failure to false (used for optional toolchains, never a hard failure).
fn runnable(cmd: &str, arg: &str) -> bool {
    Command::new(cmd)
        .arg(arg)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread")]
async fn five_client_interop_matrix() {
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

    // --- T25: request/reply fixtures (exclusive reply queues) ---
    let pika_rpc = run_fixture(
        {
            let mut c = Command::new("python3");
            c.arg(root.join("tests/interop/python/pika_request_reply.py"));
            c
        },
        &url,
    );
    match pika_rpc {
        FixtureOutcome::Passed(passes) => {
            assert!(passes.len() >= 3, "pika rpc PASS lines: {passes:?}");
            assert!(passes
                .iter()
                .any(|p| p.contains("correlated request/reply")));
            assert!(passes.iter().any(|p| p.contains("reply queue lifecycle")));
        }
        FixtureOutcome::Skipped(why) => eprintln!("SKIP pika rpc: {why}"),
        FixtureOutcome::Failed(detail) => panic!("pika rpc fixture failed:\n{detail}"),
    }

    if resolves {
        let mut node_rpc = Command::new("node");
        node_rpc
            .env("NODE_PATH", &node_modules)
            .arg(root.join("tests/interop/node/amqplib_request_reply.js"));
        let amqplib_rpc = run_fixture(node_rpc, &url);
        match amqplib_rpc {
            FixtureOutcome::Passed(passes) => {
                assert!(passes.len() >= 3, "amqplib rpc PASS lines: {passes:?}");
                assert!(passes
                    .iter()
                    .any(|p| p.contains("correlated request/reply")));
                assert!(passes.iter().any(|p| p.contains("reply queue lifecycle")));
            }
            FixtureOutcome::Skipped(why) => eprintln!("SKIP amqplib rpc: {why}"),
            FixtureOutcome::Failed(detail) => panic!("amqplib rpc fixture failed:\n{detail}"),
        }
    }

    // --- Java (RabbitMQ amqp-client) --- needs a JDK plus the pinned jar
    // (RMQ_JAVA_HOME / RMQ_AMQP_CLIENT_JAR with the documented defaults);
    // compiles into a per-run temp dir, so no build artifacts leak.
    let (javac_bin, java_bin) = match std::env::var("RMQ_JAVA_HOME") {
        Ok(home) => (format!("{home}/bin/javac"), format!("{home}/bin/java")),
        Err(_) => ("javac".into(), "java".into()),
    };
    let client_jar = PathBuf::from(
        std::env::var("RMQ_AMQP_CLIENT_JAR")
            .unwrap_or_else(|_| "/tmp/rmq-interop/amqp-client-5.21.0.jar".into()),
    );
    if !(runnable(&javac_bin, "-version") && client_jar.is_file()) {
        eprintln!(
            "SKIP java (stage a JDK and the pinned jar: RMQ_JAVA_HOME + RMQ_AMQP_CLIENT_JAR)"
        );
    } else {
        let classes = std::env::temp_dir().join(format!("rmq-interop-java-{}", std::process::id()));
        std::fs::create_dir_all(&classes).expect("java classes dir");
        let compiled = Command::new(&javac_bin)
            .arg("-cp")
            .arg(&client_jar)
            .arg("-d")
            .arg(&classes)
            .arg(root.join("tests/interop/java/RpcFixture.java"))
            .output()
            .expect("javac spawn");
        assert!(
            compiled.status.success(),
            "javac failed:\n{}",
            String::from_utf8_lossy(&compiled.stderr)
        );
        let mut jcmd = Command::new(&java_bin);
        let mut classpath = format!("{}:{}", classes.display(), client_jar.display());
        // amqp-client 5.x needs an SLF4J binding at runtime; stage one via
        // RMQ_JAVA_CP (colon-separated jars) alongside the client jar.
        if let Ok(extra) = std::env::var("RMQ_JAVA_CP") {
            classpath.push(':');
            classpath.push_str(&extra);
        }
        jcmd.arg("-cp").arg(classpath).arg("RpcFixture");
        let java_fixture = run_fixture(jcmd, &url);
        match java_fixture {
            FixtureOutcome::Passed(passes) => {
                assert!(passes.len() >= 8, "java fixture PASS lines: {passes:?}");
                assert!(passes.iter().any(|p| p.contains("handshake+auth")));
                assert!(passes
                    .iter()
                    .any(|p| p.contains("confirmed persistent publish")));
                assert!(passes.iter().any(|p| p.contains("typed properties")));
                assert!(passes.iter().any(|p| p.contains("server-named exclusive")));
                assert!(passes
                    .iter()
                    .any(|p| p.contains("correlated request/reply")));
            }
            FixtureOutcome::Skipped(why) => eprintln!("SKIP java: {why}"),
            FixtureOutcome::Failed(detail) => panic!("java fixture failed:\n{detail}"),
        }
    }

    // --- Go (amqp091-go) --- needs the go toolchain plus a module dir
    // whose go.mod pins the dependency (RMQ_GO_BIN / RMQ_GO_MOD); the
    // harness itself never fetches modules.
    let go_bin = std::env::var("RMQ_GO_BIN").unwrap_or_else(|_| "go".into());
    let go_mod = std::env::var("RMQ_GO_MOD").unwrap_or_else(|_| "/tmp/rmq-interop/go".into());
    if !(runnable(&go_bin, "version") && PathBuf::from(&go_mod).join("go.mod").is_file()) {
        eprintln!("SKIP go (stage the toolchain + module: RMQ_GO_BIN + RMQ_GO_MOD)");
    } else {
        let mut gcmd = Command::new(&go_bin);
        gcmd.current_dir(&go_mod)
            .arg("run")
            .arg(root.join("tests/interop/go/rpc_fixture.go"));
        let go_fixture = run_fixture(gcmd, &url);
        match go_fixture {
            FixtureOutcome::Passed(passes) => {
                assert!(passes.len() >= 7, "go fixture PASS lines: {passes:?}");
                assert!(passes.iter().any(|p| p.contains("handshake+auth")));
                assert!(passes
                    .iter()
                    .any(|p| p.contains("confirmed persistent publish")));
                assert!(passes.iter().any(|p| p.contains("typed properties")));
                assert!(passes.iter().any(|p| p.contains("server-named exclusive")));
                assert!(passes
                    .iter()
                    .any(|p| p.contains("correlated request/reply")));
            }
            FixtureOutcome::Skipped(why) => eprintln!("SKIP go: {why}"),
            FixtureOutcome::Failed(detail) => panic!("go fixture failed:\n{detail}"),
        }
    }

    // The harness itself proves the broker stayed healthy across all
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
