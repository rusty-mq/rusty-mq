//! RabbitMQ migration preflight (§19.1, T29): offline inspection of an
//! operator-provided definitions export. Findings group into compatible /
//! blocking / warning / unknown; unknown behavior-bearing settings block a
//! "ready to migrate" result by design. Nothing here sends data anywhere.

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct Definitions {
    #[serde(default)]
    pub vhosts: Vec<Vhost>,
    #[serde(default)]
    pub users: Vec<User>,
    #[serde(default)]
    pub permissions: Vec<Permission>,
    #[serde(default)]
    pub queues: Vec<Queue>,
    #[serde(default)]
    pub exchanges: Vec<Exchange>,
    #[serde(default)]
    pub bindings: Vec<Binding>,
    #[serde(default)]
    pub policies: Vec<Policy>,
    #[serde(default)]
    pub operator_policies: Vec<Policy>,
    #[serde(default)]
    pub parameters: Vec<ParameterValue>,
    #[serde(default)]
    pub global_parameters: Vec<GlobalParameter>,
    #[serde(default)]
    pub default_queue_type: Option<String>,
    /// Not in real exports; tests inject plugin inventories (§19.1).
    #[serde(default)]
    pub enabled_plugins: Vec<String>,
}

#[derive(Deserialize)]
pub struct Vhost {
    pub name: String,
}

#[derive(Deserialize)]
pub struct User {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub hash_algorithm: Option<String>,
}

#[derive(Deserialize)]
pub struct Permission {
    pub user: String,
    pub vhost: String,
    #[serde(default)]
    pub configure: String,
    #[serde(default)]
    pub write: String,
    #[serde(default)]
    pub read: String,
}

#[derive(Deserialize)]
pub struct Queue {
    pub vhost: String,
    pub name: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

#[derive(Deserialize)]
pub struct Exchange {
    pub vhost: String,
    pub name: String,
    #[serde(default)]
    pub type_: Option<String>,
    #[serde(rename = "type", default)]
    pub type_field: Option<String>,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

impl Exchange {
    pub fn kind(&self) -> &str {
        self.type_
            .as_deref()
            .or(self.type_field.as_deref())
            .unwrap_or("direct")
    }
}

#[derive(Deserialize)]
pub struct Binding {
    pub vhost: String,
    pub source: String,
    pub destination: String,
    #[serde(default = "default_destination_type")]
    pub destination_type: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

fn default_destination_type() -> String {
    "queue".into()
}

#[derive(Deserialize)]
pub struct Policy {
    pub vhost: String,
    pub name: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub definition: serde_json::Value,
}

#[derive(Deserialize)]
pub struct ParameterValue {
    pub component: String,
    pub name: String,
    pub vhost: Option<String>,
}

#[derive(Deserialize)]
pub struct GlobalParameter {
    pub name: String,
}

/// The four §19.1 groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Compatible,
    Blocking,
    Warning,
    Unknown,
}

#[derive(Serialize)]
pub struct Finding {
    pub severity: Severity,
    pub resource: String,
    pub kind: String,
    pub detail: String,
}

#[derive(Serialize)]
pub struct Report {
    pub ready: bool,
    pub summary: Summary,
    pub findings: Vec<Finding>,
}

#[derive(Serialize, Default)]
pub struct Summary {
    pub compatible: usize,
    pub blocking: usize,
    pub warning: usize,
    pub unknown: usize,
}

/// Queue arguments with frozen dispositions; anything else is unknown
/// (behavior-bearing by default — ADR-0005's strict posture).
const QUEUE_ARG_RULES: &[(&str, Severity, &str)] = &[
    (
        "x-message-ttl",
        Severity::Blocking,
        "per-message/per-queue TTL is a V1.1 feature",
    ),
    (
        "x-expires",
        Severity::Blocking,
        "queue expiry is a V1.1 feature",
    ),
    (
        "x-dead-letter-exchange",
        Severity::Blocking,
        "DLX is a V1.1 feature",
    ),
    (
        "x-dead-letter-routing-key",
        Severity::Blocking,
        "DLX is a V1.1 feature",
    ),
    (
        "x-max-priority",
        Severity::Blocking,
        "priority queues are V2",
    ),
    (
        "x-max-length",
        Severity::Blocking,
        "max-length/overflow is V1.1",
    ),
    (
        "x-overflow",
        Severity::Blocking,
        "overflow behavior is V1.1",
    ),
    (
        "x-single-active-consumer",
        Severity::Blocking,
        "single-active-consumer is out of V1 scope",
    ),
    (
        "x-queue-type",
        Severity::Blocking,
        "only x-queue-type=classic is supported; quorum/stream are not",
    ),
    (
        "x-quorum-initial-group-size",
        Severity::Blocking,
        "quorum queues are V3",
    ),
    (
        "x-queue-mode",
        Severity::Unknown,
        "queue-mode has no rusty-mq equivalent",
    ),
];

const PLUGIN_RULES: &[(&str, &str)] = &[
    ("rabbitmq_federation", "federation is not supported"),
    ("rabbitmq_shovel", "shovel is not supported"),
    (
        "rabbitmq_management",
        "management plugin is replaced by the native API; harmless but not equivalent",
    ),
    ("rabbitmq_stomp", "STOMP is not supported"),
    ("rabbitmq_mqtt", "MQTT is not supported"),
    ("rabbitmq_web_mqtt", "MQTT is not supported"),
    (
        "rabbitmq_delayed_message_exchange",
        "delayed-message exchange plugin is not supported",
    ),
];

pub fn inspect(defs: &Definitions) -> Report {
    let mut findings = Vec::new();
    let mut push = |severity: Severity, resource: String, kind: &str, detail: String| {
        findings.push(Finding {
            severity,
            resource,
            kind: kind.to_string(),
            detail,
        });
    };

    inspect_vhosts(defs, &mut push);
    inspect_users(defs, &mut push);
    inspect_permissions(defs, &mut push);
    inspect_queues(defs, &mut push);
    inspect_exchanges(defs, &mut push);
    inspect_bindings(defs, &mut push);
    inspect_policies(defs, &mut push);
    inspect_parameters(defs, &mut push);
    inspect_plugins(defs, &mut push);

    let mut summary = Summary::default();
    for f in &findings {
        match f.severity {
            Severity::Compatible => summary.compatible += 1,
            Severity::Blocking => summary.blocking += 1,
            Severity::Warning => summary.warning += 1,
            Severity::Unknown => summary.unknown += 1,
        }
    }
    // Unknown behavior-bearing settings block readiness (§19.1).
    let ready = summary.blocking == 0 && summary.unknown == 0;
    Report {
        ready,
        summary,
        findings,
    }
}

type Push<'a> = dyn FnMut(Severity, String, &str, String) + 'a;

fn inspect_vhosts(defs: &Definitions, push: &mut Push) {
    for v in &defs.vhosts {
        push(
            Severity::Compatible,
            format!("vhost:{}", v.name),
            "vhost",
            "vhost names map directly".into(),
        );
    }
}

fn inspect_users(defs: &Definitions, push: &mut Push) {
    for u in &defs.users {
        let admin = u.tags.iter().any(|t| t == "administrator");
        push(
            if admin {
                Severity::Compatible
            } else {
                Severity::Warning
            },
            format!("user:{}", u.name),
            "user",
            format!(
                "roles map{}; credentials must be RESET (hashes are never imported, \
                 hash_algorithm={})",
                if admin {
                    " (administrator -> admin)"
                } else {
                    ""
                },
                u.hash_algorithm.as_deref().unwrap_or("unknown"),
            ),
        );
    }
}

fn inspect_permissions(defs: &Definitions, push: &mut Push) {
    for p in &defs.permissions {
        // Rust regex subset: try compiling each pattern.
        let mut invalid = None;
        for (label, pattern) in [
            ("configure", &p.configure),
            ("write", &p.write),
            ("read", &p.read),
        ] {
            if regex::Regex::new(pattern).is_err() {
                invalid = Some(label);
                break;
            }
        }
        match invalid {
            Some(label) => push(
                Severity::Warning,
                format!("permission:{}@{}", p.user, p.vhost),
                "permission-regex",
                format!("{label} pattern is not valid Rust regex; must be translated"),
            ),
            None => push(
                Severity::Compatible,
                format!("permission:{}@{}", p.user, p.vhost),
                "permission-regex",
                "patterns valid in the Rust regex subset".into(),
            ),
        }
    }
}

fn inspect_queues(defs: &Definitions, push: &mut Push) {
    for q in &defs.queues {
        let resource = format!("queue:{}@{}", q.name, q.vhost);
        // Profile rules (§5.3).
        if q.durable && q.exclusive {
            push(
                Severity::Blocking,
                resource.clone(),
                "queue-profile",
                "durable+exclusive is rejected by the V1 profile".into(),
            );
        } else if q.durable && q.auto_delete {
            push(
                Severity::Blocking,
                resource.clone(),
                "queue-profile",
                "durable+auto-delete is rejected by the V1 profile".into(),
            );
        } else if !q.durable && !q.exclusive {
            push(
                Severity::Warning,
                resource.clone(),
                "queue-profile",
                "shared transient queues need the compatibility switch".into(),
            );
        } else {
            push(
                Severity::Compatible,
                resource.clone(),
                "queue-profile",
                "profile supported".into(),
            );
        }
        inspect_arguments(&q.arguments, &resource, "queue-argument", push);
    }
}

fn inspect_exchanges(defs: &Definitions, push: &mut Push) {
    for e in &defs.exchanges {
        let resource = format!("exchange:{}@{}", e.name, e.vhost);
        match e.kind() {
            "direct" | "fanout" | "topic" => push(
                Severity::Compatible,
                resource.clone(),
                "exchange-type",
                format!("{} supported", e.kind()),
            ),
            other => push(
                Severity::Blocking,
                resource.clone(),
                "exchange-type",
                format!("'{other}' exchanges are not supported in V1"),
            ),
        }
        inspect_arguments(&e.arguments, &resource, "exchange-argument", push);
    }
}

fn inspect_bindings(defs: &Definitions, push: &mut Push) {
    for b in &defs.bindings {
        let resource = format!("binding:{}->{}@{}", b.source, b.destination, b.vhost);
        if b.destination_type != "queue" {
            push(
                Severity::Blocking,
                resource.clone(),
                "binding",
                "exchange-to-exchange bindings are not supported in V1".into(),
            );
            continue;
        }
        inspect_arguments(&b.arguments, &resource, "binding-argument", push);
    }
}

fn inspect_arguments(args: &serde_json::Value, resource: &str, kind: &str, push: &mut Push) {
    let Some(table) = args.as_object() else {
        return;
    };
    for (key, value) in table {
        if let Some((_, severity, detail)) = QUEUE_ARG_RULES.iter().find(|(k, _, _)| k == key) {
            // x-queue-type=classic is the one accepted form.
            if key == "x-queue-type" && value.as_str() == Some("classic") {
                push(
                    Severity::Compatible,
                    resource.to_string(),
                    kind,
                    "x-queue-type=classic supported".into(),
                );
                continue;
            }
            push(*severity, resource.to_string(), kind, detail.to_string());
        } else {
            push(
                Severity::Unknown,
                resource.to_string(),
                kind,
                format!("unknown argument '{key}' (behavior-bearing by default)"),
            );
        }
    }
}

fn inspect_policies(defs: &Definitions, push: &mut Push) {
    for p in defs.policies.iter().chain(defs.operator_policies.iter()) {
        let resource = format!("policy:{}@{}", p.name, p.vhost);
        let def = p.definition.as_object().cloned().unwrap_or_default();
        if def.is_empty() {
            push(
                Severity::Unknown,
                resource,
                "policy",
                "policy with empty definition: effect unknown".into(),
            );
            continue;
        }
        for key in def.keys() {
            let severity = match key.as_str() {
                "ha-mode"
                | "ha-params"
                | "ha-sync-mode"
                | "ha-promote-on-failure"
                | "queue-master-locator" => Severity::Blocking,
                "message-ttl"
                | "expires"
                | "dead-letter-exchange"
                | "dead-letter-routing-key"
                | "max-length"
                | "overflow" => Severity::Blocking,
                "federation-upstream" | "federation-upstream-set" => Severity::Blocking,
                _ => Severity::Unknown,
            };
            let detail = match severity {
                Severity::Blocking => format!("policy key '{key}' applies unsupported behavior"),
                _ => format!("policy key '{key}' has no rusty-mq equivalent"),
            };
            push(severity, resource.clone(), "policy", detail);
        }
    }
}

fn inspect_parameters(defs: &Definitions, push: &mut Push) {
    for p in &defs.parameters {
        let resource = format!("parameter:{}:{}", p.component, p.name);
        push(
            Severity::Blocking,
            resource,
            "runtime-parameter",
            format!(
                "runtime parameter component '{}' is not supported",
                p.component
            ),
        );
    }
    for g in &defs.global_parameters {
        push(
            Severity::Unknown,
            format!("global-parameter:{}", g.name),
            "global-parameter",
            "no rusty-mq equivalent; effect must be reviewed".into(),
        );
    }
    if let Some(dqt) = &defs.default_queue_type {
        if dqt != "classic" {
            push(
                Severity::Blocking,
                "default_queue_type".into(),
                "default-queue-type",
                format!("default queue type '{dqt}' is not classic"),
            );
        }
    }
}

fn inspect_plugins(defs: &Definitions, push: &mut Push) {
    for plugin in &defs.enabled_plugins {
        let detail = PLUGIN_RULES
            .iter()
            .find(|(name, _)| name == plugin)
            .map(|(_, d)| d.to_string());
        match detail {
            Some(d) if d.contains("not equivalent") => {
                push(Severity::Warning, format!("plugin:{plugin}"), "plugin", d)
            }
            Some(d) => push(Severity::Blocking, format!("plugin:{plugin}"), "plugin", d),
            None => push(
                Severity::Unknown,
                format!("plugin:{plugin}"),
                "plugin",
                "unrecognized plugin: effect unknown".into(),
            ),
        }
    }
}
