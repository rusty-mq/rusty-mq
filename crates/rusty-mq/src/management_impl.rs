//! The real broker implements the management facade (one-way dependency:
//! management defines, the binary implements).

use std::sync::atomic::Ordering;

use rusty_mq_core::auth::{Permissions, Role};
use rusty_mq_core::topology::TopologyError;
use rusty_mq_management::broker_facade::BrokerHandle;

use crate::broker::Broker;
use crate::metrics;

impl BrokerHandle for Broker {
    fn authenticate(&self, user: &str, pass: &str) -> bool {
        Broker::authenticate(self, user, pass)
    }

    fn role_of(&self, user: &str) -> Option<Role> {
        self.auth.lock().unwrap().principal(user).map(|p| p.role)
    }

    fn is_ready(&self) -> bool {
        // Recovery completes before serving; a disk alarm makes readiness
        // fail while liveness stays healthy (§12.1) — durable admissions
        // are quiesced in that state.
        !self.disk_alarm()
    }

    fn render_metrics(&self) -> String {
        let ready = self.store.lock().unwrap().total_ready_entries();
        // Per-queue series are opt-in (§12.3) and snapshotted under the
        // store lock with the total.
        let queue_gauges: Vec<(String, u64)> = if self.queue_labels_enabled {
            let store = self.store.lock().unwrap();
            let topo = self.topology.lock().unwrap();
            topo.iter_queues()
                .filter_map(|(id, rec)| {
                    let name = rec.name.clone();
                    if name.is_empty() {
                        None
                    } else {
                        Some((name, store.len(id)))
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        let queues = self.topology.lock().unwrap().iter_queues().count() as u64;
        let journal = self
            .data_dir
            .as_ref()
            .map(|d| rusty_mq_storage::snapshot::journal_bytes(d))
            .unwrap_or(0);
        metrics::render_prometheus_with_queues(
            &self.metrics,
            &[
                ("rusty_mq_ready_messages", ready),
                ("rusty_mq_queues", queues),
                ("rusty_mq_journal_bytes", journal),
            ],
            &queue_gauges
                .iter()
                .map(|(n, v)| (n.as_str(), *v))
                .collect::<Vec<_>>(),
        )
    }

    fn status_summary(&self) -> serde_json::Value {
        serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "persistent": self.is_persistent(),
            "auth_version": self.auth.lock().unwrap().version(),
            "last_fence_lsn": self.last_fence_lsn.load(Ordering::SeqCst),
        })
    }

    fn create_vhost(&self, name: &str) -> Result<(), String> {
        Broker::create_vhost(self, name)
    }

    fn delete_vhost(&self, name: &str) -> Result<(), String> {
        Broker::delete_vhost(self, name)
    }

    fn list_vhosts(&self) -> Vec<serde_json::Value> {
        // All known vhosts: "/" plus any created via the management
        // plane (durable VhostDeclare records).
        let topo = self.topology.lock().unwrap();
        let mut names: Vec<String> = topo.vhost_names().collect();
        drop(topo);
        names.sort();
        names
            .into_iter()
            .map(|name| serde_json::json!({ "id": name, "name": name }))
            .collect()
    }

    fn list_queues(&self, vhost: &str) -> Vec<serde_json::Value> {
        let topo = self.topology.lock().unwrap();
        let Some(vhost_id) = topo.find_vhost(vhost) else {
            return Vec::new();
        };
        let store = self.store.lock().unwrap();
        let consumers = self.consumers.lock().unwrap();
        let mut rows = Vec::new();
        for (id, rec) in topo.iter_queues() {
            if rec.vhost != vhost_id {
                continue;
            }
            rows.push(serde_json::json!({
                "id": rec.name,
                "name": rec.name,
                "durable": rec.profile.durable,
                "auto_delete": rec.profile.auto_delete,
                "ready_messages": store.len(id),
                "consumers": consumers.consumer_count(id),
            }));
        }
        rows.sort_by(|a, b| {
            a["name"]
                .as_str()
                .unwrap_or("")
                .cmp(b["name"].as_str().unwrap_or(""))
        });
        rows
    }

    fn purge_queue(&self, vhost: &str, queue: &str) -> Result<u64, TopologyError> {
        let topo = self.topology.lock().unwrap();
        let Some(vhost_id) = topo.find_vhost(vhost) else {
            return Err(TopologyError::VhostNotFound);
        };
        let id = topo
            .find_queue(vhost_id, queue)
            .ok_or_else(|| TopologyError::QueueNotFound(queue.to_string()))?;
        drop(topo);
        Ok(self.store.lock().unwrap().purge(id))
    }

    fn delete_queue(&self, vhost: &str, queue: &str) -> Result<(), TopologyError> {
        // Management-plane delete: journals the durable fact when needed.
        let topo = self.topology.lock().unwrap();
        let Some(vhost_id) = topo.find_vhost(vhost) else {
            return Err(TopologyError::VhostNotFound);
        };
        let id = topo
            .find_queue(vhost_id, queue)
            .ok_or_else(|| TopologyError::QueueNotFound(queue.to_string()))?;
        let durable = topo.queue_record(id).is_some_and(|r| r.profile.durable);
        drop(topo);
        if durable
            && self
                .journal_commit(&[rusty_mq_storage::Record::QueueDelete { id: id.to_raw() }])
                .is_err()
        {
            return Err(TopologyError::ResourceErrorJournal);
        }
        let mut topo = self.topology.lock().unwrap();
        let vhost_id = topo.find_vhost(vhost).ok_or(TopologyError::VhostNotFound)?;
        topo.remove_queue_by_id(vhost_id, id);
        drop(topo);
        self.store.lock().unwrap().drain(id);
        Ok(())
    }

    fn create_user(&self, username: &str, password: &str, role: Role) -> Result<(), String> {
        Broker::upsert_principal(
            self,
            rusty_mq_core::auth::Principal {
                username: username.to_string(),
                password_phc: String::new(),
                role,
            },
            Some(password),
        )
    }

    fn list_users(&self) -> Vec<serde_json::Value> {
        let auth = self.auth.lock().unwrap();
        let mut rows: Vec<_> = auth
            .principal_names()
            .map(|name| {
                let role = auth.principal(name).map(|p| p.role);
                serde_json::json!({
                    "id": name,
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
        rows.sort_by(|a, b| {
            a["username"]
                .as_str()
                .unwrap_or("")
                .cmp(b["username"].as_str().unwrap_or(""))
        });
        rows
    }

    fn rotate_credentials(&self, username: &str, password: &str) -> Result<bool, String> {
        let existing = {
            let auth = self.auth.lock().unwrap();
            auth.principal(username)
                .map(|p| (p.role, p.username.clone()))
        };
        let Some((role, _)) = existing else {
            return Ok(false);
        };
        Broker::upsert_principal(
            self,
            rusty_mq_core::auth::Principal {
                username: username.to_string(),
                password_phc: String::new(),
                role,
            },
            Some(password),
        )?;
        Ok(true)
    }

    fn delete_user(&self, username: &str) -> Result<bool, String> {
        Broker::delete_principal(self, username)
    }

    fn get_permissions(&self, username: &str, vhost: &str) -> Option<Permissions> {
        self.auth.lock().unwrap().get_permissions(username, vhost)
    }

    fn set_permissions(
        &self,
        username: &str,
        vhost: &str,
        perms: Permissions,
    ) -> Result<(), String> {
        Broker::set_permissions(self, username, vhost, perms)
    }

    fn list_connections(&self) -> Vec<(String, String)> {
        Broker::list_connections(self)
            .into_iter()
            .map(|(id, user)| (id.to_raw().to_string(), user))
            .collect()
    }

    fn close_connection(&self, id: &str, reason: &str) -> bool {
        let Ok(parsed) = id.parse::<u64>() else {
            return false;
        };
        Broker::close_connection(self, rusty_mq_core::ConnectionId::from_raw(parsed), reason)
    }

    fn export_definitions(&self) -> serde_json::Value {
        crate::definitions::export(self)
    }

    fn import_definitions(
        &self,
        payload: &serde_json::Value,
        dry_run: bool,
    ) -> Result<serde_json::Value, String> {
        crate::definitions::import(self, payload, dry_run)
            .and_then(|report| serde_json::to_value(report).map_err(|e| e.to_string()))
    }

    fn list_permissions(&self) -> Vec<serde_json::Value> {
        self.auth
            .lock()
            .unwrap()
            .all_permissions()
            .into_iter()
            .map(|(user, vhost, p)| {
                serde_json::json!({
                    "username": user,
                    "vhost": vhost,
                    "configure": p.configure,
                    "write": p.write,
                    "read": p.read,
                })
            })
            .collect()
    }

    fn delete_permissions(&self, username: &str, vhost: &str) -> Result<bool, String> {
        let removed = self
            .auth
            .lock()
            .unwrap()
            .delete_permissions(username, vhost)
            .is_some();
        if removed {
            let record = rusty_mq_storage::Record::PermissionDelete {
                username: username.to_string(),
                vhost: vhost.to_string(),
            };
            self.journal_commit(&[record]).map_err(|e| e.to_string())?;
        }
        Ok(removed)
    }
}
