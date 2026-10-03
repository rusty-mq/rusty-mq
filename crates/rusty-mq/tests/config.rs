//! §13.2: configuration — the PRD example parses verbatim, unknown fields
//! are rejected, env overrides work (and unknown overrides error),
//! validation catches impossible combinations, and env vars are isolated
//! per test (serial, since the environment is process-global).

use std::path::PathBuf;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .canonicalize()
        .unwrap()
}

fn with_env(vars: &[(&str, &str)], f: impl FnOnce()) {
    let _guard = ENV_LOCK.lock().unwrap();
    for (k, v) in vars {
        // SAFETY of ordering: tests are serialized by the lock.
        std::env::set_var(k, v);
    }
    f();
    for (k, _) in vars {
        std::env::remove_var(k);
    }
}

#[test]
fn prd_example_parses_verbatim() {
    let path = repo_root().join("deploy/config.example.toml");
    let cfg = rusty_mq::config::load_file(&path).expect("PRD §13.2 example must parse + validate");
    assert_eq!(cfg.amqp.frame_max_bytes, 131_072);
    assert_eq!(cfg.storage.segment_bytes, 268_435_456);
    assert!(!cfg.compatibility.allow_transient_nonexclusive_queues);
    assert_eq!(cfg.management.listen, "127.0.0.1:15672");
}

#[test]
fn unknown_fields_rejected() {
    let dir = std::env::temp_dir().join(format!("rmq-cfg-{}-unknown", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bad.toml");
    std::fs::write(&path, "[amqp]\nlistenn = \"x\"\n").unwrap();
    let err = rusty_mq::config::load_file(&path).unwrap_err();
    assert!(
        err.contains("unknown field") || err.contains("listenn"),
        "got: {err}"
    );
}

#[test]
fn env_overrides_apply_and_unknown_env_errors() {
    with_env(&[("RUSTY_MQ__AMQP__LISTEN", "127.0.0.1:5699")], || {
        let path = repo_root().join("deploy/config.example.toml");
        let cfg = rusty_mq::config::load_file(&path).unwrap();
        assert_eq!(cfg.amqp.listen, "127.0.0.1:5699");
    });
    with_env(&[("RUSTY_MQ__NOT_A__THING", "1")], || {
        let path = repo_root().join("deploy/config.example.toml");
        let err = rusty_mq::config::load_file(&path).unwrap_err();
        assert!(err.contains("unknown environment override"), "got: {err}");
    });
}

#[test]
fn validation_rejects_impossible_combinations() {
    // Serialize with the env-mutating tests (the environment is global).
    let _guard = ENV_LOCK.lock().unwrap();
    let mk = |amqp: &str| {
        let dir = std::env::temp_dir().join(format!("rmq-cfg-{}-v", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.toml");
        // `amqp` supplies the full body tail; when it overrides listen,
        // it replaces the default line entirely.
        let body = if amqp.starts_with("listen") {
            format!("[amqp]\n{amqp}")
        } else {
            format!("[amqp]\nlisten = \"127.0.0.1:5672\"\n{amqp}")
        };
        std::fs::write(&path, body).unwrap();
        path
    };
    // frame_max below protocol floor.
    let err = rusty_mq::config::load_file(&mk("frame_max_bytes = 512\n")).unwrap_err();
    assert!(err.contains("4096"), "got: {err}");
    // plaintext non-loopback without opt-in.
    let err = rusty_mq::config::load_file(&mk("listen = \"0.0.0.0:5672\"\n")).unwrap_err();
    assert!(err.contains("allow_insecure_remote"), "got: {err}");
    // header budget must fit inside frame_max.
    let err = rusty_mq::config::load_file(&mk("max_header_bytes = 999999\n")).unwrap_err();
    assert!(err.contains("max_header_bytes"), "got: {err}");
}
