//! T29: migration preflight — every finding class against a realistic
//! RabbitMQ definitions export, plus the ready gate (unknown blocks).

use rusty_mq::migration::{inspect, Definitions};

fn defs(json: &str) -> Definitions {
    serde_json::from_str(json).expect("fixture parses")
}

#[test]
fn clean_export_is_ready() {
    let d = defs(
        r#"{
          "vhosts": [{"name": "/"}],
          "users": [{"name": "admin", "tags": ["administrator"], "hash_algorithm": "rabbit_password_hashing_sha256"}],
          "permissions": [{"user": "admin", "vhost": "/", "configure": ".*", "write": ".*", "read": ".*"}],
          "queues": [
            {"vhost": "/", "name": "work", "durable": true, "arguments": {}},
            {"vhost": "/", "name": "tmp", "durable": false, "exclusive": true, "arguments": {}}
          ],
          "exchanges": [
            {"vhost": "/", "name": "ev", "type": "topic", "durable": true, "arguments": {}}
          ],
          "bindings": [
            {"vhost": "/", "source": "ev", "destination": "work", "destination_type": "queue",
             "routing_key": "a.#", "arguments": {}}
          ]
        }"#,
    );
    let report = inspect(&d);
    assert!(report.ready, "no blockers, no unknowns");
    assert_eq!(report.summary.blocking, 0);
    assert_eq!(report.summary.unknown, 0);
    assert!(report.summary.compatible >= 6);
}

#[test]
fn every_finding_class_is_detected() {
    let d = defs(
        r#"{
          "vhosts": [{"name": "/"}],
          "users": [
            {"name": "admin", "tags": ["administrator"], "hash_algorithm": "rabbit_password_hashing_sha256"},
            {"name": "app", "tags": [], "hash_algorithm": "rabbit_password_hashing_sha256"}
          ],
          "permissions": [
            {"user": "app", "vhost": "/", "configure": "^ok$", "write": "(a)\\1|(?=x)", "read": ".*"}
          ],
          "queues": [
            {"vhost": "/", "name": "durable-excl", "durable": true, "exclusive": true},
            {"vhost": "/", "name": "durable-ad", "durable": true, "auto_delete": true},
            {"vhost": "/", "name": "shared-transient", "durable": false},
            {"vhost": "/", "name": "ttl", "durable": true, "arguments": {"x-message-ttl": 60000}},
            {"vhost": "/", "name": "quorum", "durable": true, "arguments": {"x-queue-type": "quorum"}},
            {"vhost": "/", "name": "classic-ok", "durable": true, "arguments": {"x-queue-type": "classic"}},
            {"vhost": "/", "name": "weird-arg", "durable": true, "arguments": {"x-mystery": 1}}
          ],
          "exchanges": [
            {"vhost": "/", "name": "hdr", "type": "headers", "durable": true},
            {"vhost": "/", "name": "ae", "type": "direct", "durable": true,
             "arguments": {"alternate-exchange": "ev"}}
          ],
          "bindings": [
            {"vhost": "/", "source": "ev", "destination": "ev2", "destination_type": "exchange"},
            {"vhost": "/", "source": "ev", "destination": "work", "destination_type": "queue",
             "routing_key": "k", "arguments": {"x-unknown-bind": true}}
          ],
          "policies": [
            {"vhost": "/", "name": "ha", "pattern": ".*", "definition": {"ha-mode": "exactly", "ha-params": 3}},
            {"vhost": "/", "name": "ttl-pol", "pattern": "tmp", "definition": {"message-ttl": 1000}}
          ],
          "operator_policies": [
            {"vhost": "/", "name": "op", "pattern": ".*", "definition": {"max-length": 10}}
          ],
          "parameters": [
            {"component": "federation-upstream", "name": "up1", "vhost": "/"}
          ],
          "global_parameters": [
            {"name": "cluster_name", "value": {}}
          ],
          "default_queue_type": "quorum",
          "enabled_plugins": ["rabbitmq_management", "rabbitmq_shovel", "rabbitmq_mystery"]
        }"#,
    );
    let report = inspect(&d);
    assert!(!report.ready);

    let has = |severity: &str, kind: &str| {
        report
            .findings
            .iter()
            .any(|f| format!("{:?}", f.severity).to_lowercase() == severity && f.kind == kind)
    };
    // Queue profiles.
    assert!(has("blocking", "queue-profile"));
    assert!(has("warning", "queue-profile")); // shared transient
                                              // Frozen queue arguments.
    assert!(has("blocking", "queue-argument"));
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.kind == "queue-argument"
                && f.detail.contains("x-queue-type=classic supported"))
    );
    // Unknown argument.
    assert!(report
        .findings
        .iter()
        .any(|f| f.kind == "queue-argument" && f.detail.contains("x-mystery")));
    // Exchange type + arguments.
    assert!(has("blocking", "exchange-type"));
    assert!(has("unknown", "exchange-argument")); // alternate-exchange not in rules table? -> unknown
                                                  // Bindings.
    assert!(has("blocking", "binding")); // e2e
    assert!(has("unknown", "binding-argument"));
    // Policies + operator policies.
    assert!(has("blocking", "policy"));
    // Runtime + global parameters + default queue type.
    assert!(has("blocking", "runtime-parameter"));
    assert!(has("unknown", "global-parameter"));
    assert!(has("blocking", "default-queue-type"));
    // Plugins: management->warning, shovel->blocking, mystery->unknown.
    assert!(report.findings.iter().any(|f| f.kind == "plugin"
        && f.resource == "plugin:rabbitmq_management"
        && f.detail.contains("not equivalent")));
    assert!(report.findings.iter().any(|f| f.kind == "plugin"
        && f.resource == "plugin:rabbitmq_shovel"
        && f.detail.contains("not supported")));
    assert!(has("unknown", "plugin"));
    // Permissions: the invalid write pattern is flagged.
    assert!(has("warning", "permission-regex"));
    // Users: credential reset warning present.
    assert!(has("warning", "user"));
}

#[test]
fn unknown_alone_blocks_readiness() {
    let d = defs(
        r#"{
          "queues": [
            {"vhost": "/", "name": "q", "durable": true, "arguments": {"x-novel": 1}}
          ]
        }"#,
    );
    let report = inspect(&d);
    assert_eq!(report.summary.blocking, 0);
    assert_eq!(report.summary.unknown, 1);
    assert!(
        !report.ready,
        "unknown behavior-bearing settings block readiness"
    );
}
