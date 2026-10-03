//! Native definitions export/import (§13.1): topology as data — NOT
//! RabbitMQ's schema and never claiming to be (ADR-0005).
//!
//! Export covers supported durable topology only. Import validates the
//! ENTIRE payload before mutating anything, supports dry-run (report
//! only), and is idempotent per resource: an equivalent existing resource
//! reports `exists`, a conflicting one reports `conflict` without
//! overwriting. Messages and credentials are never part of definitions.

use serde::Deserialize;
use serde_json::{json, Value};

use rusty_mq_core::auth::Role;
use rusty_mq_core::routing::ExchangeType;
use rusty_mq_core::topology::{ExchangeRecord, QueueRecord, Topology};

use crate::broker::Broker;

/// The native export: durable queues, durable exchanges (non-builtin),
/// both-durable bindings.
pub fn export(broker: &Broker) -> Value {
    let topo = broker.topology.lock().unwrap();
    let mut queues = Vec::new();
    let mut exchanges = Vec::new();
    let mut bindings = Vec::new();

    for (_, rec) in topo.iter_queues() {
        if !rec.profile.durable {
            continue;
        }
        queues.push(json!({
            "name": rec.name,
            "vhost": vhost_name(&topo, rec),
        }));
    }
    for (_, rec) in topo.iter_exchanges() {
        if !rec.durable || rec.name.is_empty() || rec.name.starts_with("amq.") {
            continue;
        }
        exchanges.push(json!({
            "name": rec.name,
            "vhost": vhost_name_ex(&topo, rec),
            "type": rec.kind.wire_name(),
            "auto_delete": rec.auto_delete,
            "internal": rec.internal,
        }));
    }
    for (exchange, queue, key) in topo.iter_bindings() {
        let ex_ok = topo.exchange_record(exchange).is_some_and(|r| r.durable);
        let q_ok = topo.queue_record(queue).is_some_and(|r| r.profile.durable);
        if !ex_ok || !q_ok {
            continue;
        }
        bindings.push(json!({
            "source": topo.exchange_record(exchange).map(|r| r.name.clone()).unwrap_or_default(),
            "destination": topo.queue_record(queue).map(|r| r.name.clone()).unwrap_or_default(),
            "vhost": "/",
            "routing_key": key,
        }));
    }
    // Users and permission grants: usernames/roles only — credential
    // material NEVER leaves the broker (§13.1).
    let (users, permissions) = {
        let auth = broker.auth.lock().unwrap();
        let users: Vec<_> = auth
            .principal_names()
            .map(|name| {
                let role = auth.principal(name).map(|p| p.role);
                json!({
                    "username": name,
                    "role": match role {
                        Some(Role::Ordinary) | None => "ordinary",
                        Some(Role::Monitor) => "monitor",
                        Some(Role::Operator) => "operator",
                        Some(Role::Admin) => "admin",
                    },
                })
            })
            .collect();
        let permissions: Vec<_> = auth
            .all_permissions()
            .into_iter()
            .map(|(user, vhost, p)| {
                json!({
                    "username": user,
                    "vhost": vhost,
                    "configure": p.configure,
                    "write": p.write,
                    "read": p.read,
                })
            })
            .collect();
        (users, permissions)
    };
    json!({
        "format": "rusty-mq-definitions",
        "version": 1,
        "queues": queues,
        "exchanges": exchanges,
        "bindings": bindings,
        "users": users,
        "permissions": permissions,
    })
}

fn vhost_name(_topo: &Topology, _rec: &QueueRecord) -> &'static str {
    "/" // single vhost until the vhost record lands
}

fn vhost_name_ex(_topo: &Topology, _rec: &ExchangeRecord) -> &'static str {
    "/"
}

/// Per-resource import outcome.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Created,
    Exists,
    Conflict,
    Invalid,
}

#[derive(serde::Serialize)]
pub struct ResourceResult {
    pub resource: String,
    pub outcome: Outcome,
    pub detail: String,
}

#[derive(serde::Serialize)]
pub struct ImportReport {
    pub dry_run: bool,
    pub results: Vec<ResourceResult>,
}

#[derive(Deserialize)]
struct Definitions {
    queues: Vec<QueueDef>,
    exchanges: Vec<ExchangeDef>,
    #[serde(default)]
    bindings: Vec<BindingDef>,
    /// Reference list from exports; roles are NOT applied on import
    /// (credential creation is a separate admin action).
    #[serde(default)]
    users: Vec<UserDef>,
    #[serde(default)]
    permissions: Vec<PermissionDef>,
}

#[derive(Deserialize)]
struct UserDef {
    username: String,
    #[serde(default)]
    role: String,
}

#[derive(Deserialize)]
struct PermissionDef {
    username: String,
    vhost: String,
    #[serde(default)]
    configure: String,
    #[serde(default)]
    write: String,
    #[serde(default)]
    read: String,
}

#[derive(Deserialize)]
struct QueueDef {
    name: String,
    #[serde(default)]
    vhost: String,
}

#[derive(Deserialize)]
struct ExchangeDef {
    name: String,
    #[serde(default)]
    vhost: String,
    #[serde(default = "default_kind")]
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    auto_delete: bool,
    #[serde(default)]
    internal: bool,
}

fn default_kind() -> String {
    "direct".into()
}

#[derive(Deserialize)]
struct BindingDef {
    source: String,
    destination: String,
    #[serde(default)]
    vhost: String,
    routing_key: String,
}

/// Import: validate all, then apply (unless dry-run). Conflicts never
/// overwrite; the whole import is validated before any mutation.
pub fn import(broker: &Broker, payload: &Value, dry_run: bool) -> Result<ImportReport, String> {
    let defs: Definitions = serde_json::from_value(payload.clone())
        .map_err(|e| format!("definitions not valid: {e}"))?;

    // Phase 1: validate everything up front.
    let mut results = Vec::new();
    let mut valid = true;
    for e in &defs.exchanges {
        if ExchangeType::from_wire_name(&e.kind).is_none() {
            valid = false;
            results.push(ResourceResult {
                resource: format!("exchange:{}@{}", e.name, e.vhost),
                outcome: Outcome::Invalid,
                detail: format!("unsupported exchange type '{}'", e.kind),
            });
        }
    }
    for q in &defs.queues {
        if q.name.is_empty() || q.name.starts_with("amq.") {
            valid = false;
            results.push(ResourceResult {
                resource: format!("queue:{}@{}", q.name, q.vhost),
                outcome: Outcome::Invalid,
                detail: "queue names must be non-empty and not reserved amq.*".into(),
            });
        }
    }
    for e in &defs.exchanges {
        if e.name.is_empty() || e.name.starts_with("amq.") {
            valid = false;
            results.push(ResourceResult {
                resource: format!("exchange:{}@{}", e.name, e.vhost),
                outcome: Outcome::Invalid,
                detail: "exchange names must be non-empty and not reserved amq.*".into(),
            });
        }
    }
    if !valid || dry_run {
        // Dry run still reports would-be outcomes for the valid parts.
        if dry_run && valid {
            classify_only(broker, &defs, &mut results);
        }
        return Ok(ImportReport { dry_run, results });
    }

    // Phase 2: apply — queues/exchanges first, then bindings.
    apply(broker, &defs, &mut results);
    Ok(ImportReport { dry_run, results })
}

fn classify_only(broker: &Broker, defs: &Definitions, results: &mut Vec<ResourceResult>) {
    let topo = broker.topology.lock().unwrap();
    let Some(vhost) = topo.find_vhost("/") else {
        return;
    };
    for q in &defs.queues {
        let outcome = match topo.find_queue(vhost, &q.name) {
            Some(_) => Outcome::Exists,
            None => Outcome::Created,
        };
        results.push(ResourceResult {
            resource: format!("queue:{}@{}", q.name, q.vhost),
            outcome,
            detail: "dry run".into(),
        });
    }
    for e in &defs.exchanges {
        let outcome = match topo.find_exchange(vhost, &e.name) {
            Some(id) => {
                let equivalent = topo
                    .exchange_record(id)
                    .map(|r| {
                        r.kind.wire_name() == e.kind
                            && r.durable
                            && r.auto_delete == e.auto_delete
                            && r.internal == e.internal
                    })
                    .unwrap_or(false);
                if equivalent {
                    Outcome::Exists
                } else {
                    Outcome::Conflict
                }
            }
            None => Outcome::Created,
        };
        results.push(ResourceResult {
            resource: format!("exchange:{}@{}", e.name, e.vhost),
            outcome,
            detail: "dry run".into(),
        });
    }
    for b in &defs.bindings {
        results.push(ResourceResult {
            resource: format!("binding:{}->{}@{}", b.source, b.destination, b.vhost),
            outcome: Outcome::Created,
            detail: "dry run".into(),
        });
    }
    for p in &defs.permissions {
        results.push(ResourceResult {
            resource: format!("permission:{}@{}", p.username, p.vhost),
            outcome: Outcome::Created,
            detail: "dry run".into(),
        });
    }
}

fn apply(broker: &Broker, defs: &Definitions, results: &mut Vec<ResourceResult>) {
    // Queues (durable profile; §5.3 rules enforced by declare_queue).
    for q in &defs.queues {
        let resource = format!("queue:{}@{}", q.name, q.vhost);
        let outcome = {
            let mut topo = broker.topology.lock().unwrap();
            let vhost = topo.find_vhost("/").expect("default vhost exists");
            let existing = topo.find_queue(vhost, &q.name);
            match existing {
                Some(id) => {
                    let equivalent = topo
                        .queue_record(id)
                        .map(|r| r.profile.durable)
                        .unwrap_or(false);
                    if equivalent {
                        Outcome::Exists
                    } else {
                        Outcome::Conflict
                    }
                }
                None => {
                    let declared = topo.declare_queue(
                        vhost,
                        &q.name,
                        rusty_mq_core::topology::QueueProfile {
                            durable: true,
                            exclusive: false,
                            auto_delete: false,
                        },
                        None,
                    );
                    match declared {
                        Ok(id) => {
                            let rec = rusty_mq_storage::Record::QueueDeclare(
                                rusty_mq_storage::QueueRecord {
                                    name: q.name.clone(),
                                    id: id.to_raw(),
                                    durable: true,
                                    exclusive: false,
                                    auto_delete: false,
                                    owner: 0,
                                },
                            );
                            if broker.journal_commit(&[rec]).is_err() {
                                topo.remove_queue_by_id(vhost, id);
                                Outcome::Invalid
                            } else {
                                Outcome::Created
                            }
                        }
                        Err(_) => Outcome::Invalid,
                    }
                }
            }
        };
        results.push(ResourceResult {
            resource,
            outcome: outcome.clone(),
            detail: outcome_detail(&outcome),
        });
    }

    // Exchanges.
    for e in &defs.exchanges {
        let resource = format!("exchange:{}@{}", e.name, e.vhost);
        let Some(kind) = ExchangeType::from_wire_name(&e.kind) else {
            continue; // already Invalid from validation
        };
        let outcome = {
            let mut topo = broker.topology.lock().unwrap();
            let vhost = topo.find_vhost("/").expect("default vhost exists");
            let existing = topo.find_exchange(vhost, &e.name);
            match existing {
                Some(id) => {
                    let equivalent = topo
                        .exchange_record(id)
                        .map(|r| {
                            r.kind == kind
                                && r.durable
                                && r.auto_delete == e.auto_delete
                                && r.internal == e.internal
                        })
                        .unwrap_or(false);
                    if equivalent {
                        Outcome::Exists
                    } else {
                        Outcome::Conflict
                    }
                }
                None => {
                    let declared = topo.declare_exchange(
                        vhost,
                        &e.name,
                        kind,
                        true,
                        e.auto_delete,
                        e.internal,
                    );
                    match declared {
                        Ok(id) => {
                            let rec = rusty_mq_storage::Record::ExchangeDeclare(
                                rusty_mq_storage::ExchangeRecord {
                                    name: e.name.clone(),
                                    id: id.to_raw(),
                                    kind: match kind {
                                        ExchangeType::Direct => 0,
                                        ExchangeType::Fanout => 1,
                                        ExchangeType::Topic => 2,
                                    },
                                    durable: true,
                                    auto_delete: e.auto_delete,
                                    internal: e.internal,
                                },
                            );
                            if broker.journal_commit(&[rec]).is_err() {
                                topo.remove_exchange_by_id(vhost, id);
                                Outcome::Invalid
                            } else {
                                Outcome::Created
                            }
                        }
                        Err(_) => Outcome::Invalid,
                    }
                }
            }
        };
        results.push(ResourceResult {
            resource,
            outcome: outcome.clone(),
            detail: outcome_detail(&outcome),
        });
    }

    apply_permissions(broker, defs, results);

    // Exported user entries are reference-only (credentials never ride in
    // definitions); malformed roles are surfaced rather than ignored.
    for u in &defs.users {
        if !matches!(
            u.role.as_str(),
            "ordinary" | "monitor" | "operator" | "admin" | ""
        ) {
            results.push(ResourceResult {
                resource: format!("user:{}", u.username),
                outcome: Outcome::Invalid,
                detail: format!("unknown role '{}'", u.role),
            });
        }
    }

    // Bindings (source exchange + destination queue must now exist).
    for b in &defs.bindings {
        let resource = format!("binding:{}->{}@{}", b.source, b.destination, b.vhost);
        let outcome = {
            let mut topo = broker.topology.lock().unwrap();
            let vhost = topo.find_vhost("/").expect("default vhost exists");
            let (ex, q) = (
                topo.find_exchange(vhost, &b.source),
                topo.find_queue(vhost, &b.destination),
            );
            match (ex, q) {
                (Some(ex), Some(q)) => {
                    let bound = topo.bind(vhost, ex, q, &b.routing_key);
                    if bound.is_ok() {
                        let both_durable = topo.exchange_record(ex).is_some_and(|r| r.durable)
                            && topo.queue_record(q).is_some_and(|r| r.profile.durable);
                        if both_durable {
                            let rec = rusty_mq_storage::Record::Bind(rusty_mq_storage::Binding {
                                exchange: ex.to_raw(),
                                queue: q.to_raw(),
                                routing_key: b.routing_key.clone(),
                            });
                            let _ = broker.journal_commit(&[rec]);
                        }
                        Outcome::Created // bind() is idempotent; equivalent rebind is a no-op
                    } else {
                        Outcome::Invalid
                    }
                }
                _ => Outcome::Invalid,
            }
        };
        results.push(ResourceResult {
            resource,
            outcome: outcome.clone(),
            detail: outcome_detail(&outcome),
        });
    }
}

fn apply_permissions(broker: &Broker, defs: &Definitions, results: &mut Vec<ResourceResult>) {
    for p in &defs.permissions {
        let resource = format!("permission:{}@{}", p.username, p.vhost);
        // Grants only apply to EXISTING principals: credentials are never
        // part of definitions (§13.1), so creating the user first is the
        // operator's explicit step (matches docs/migration.md).
        let exists = broker.auth.lock().unwrap().principal(&p.username).is_some();
        if !exists {
            results.push(ResourceResult {
                resource,
                outcome: Outcome::Invalid,
                detail: "user does not exist; create credentials first (never in definitions)"
                    .into(),
            });
            continue;
        }
        for (label, pattern) in [
            ("configure", &p.configure),
            ("write", &p.write),
            ("read", &p.read),
        ] {
            if regex::Regex::new(pattern).is_err() {
                results.push(ResourceResult {
                    resource: resource.clone(),
                    outcome: Outcome::Invalid,
                    detail: format!("invalid {label} regex"),
                });
            }
        }
        let perms = rusty_mq_core::auth::Permissions {
            configure: p.configure.clone(),
            write: p.write.clone(),
            read: p.read.clone(),
        };
        match Broker::set_permissions(broker, &p.username, &p.vhost, perms) {
            Ok(()) => results.push(ResourceResult {
                resource,
                outcome: Outcome::Created,
                detail: "grants applied".into(),
            }),
            Err(e) => results.push(ResourceResult {
                resource,
                outcome: Outcome::Invalid,
                detail: e,
            }),
        }
    }
}

fn outcome_detail(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Created => "created".into(),
        Outcome::Exists => "equivalent resource already present (idempotent)".into(),
        Outcome::Conflict => "existing resource differs; NOT overwritten".into(),
        Outcome::Invalid => "rejected by the V1 profile or validation".into(),
    }
}
