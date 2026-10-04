//! The broker surface the management API needs, as a trait implemented by
//! the real broker in the `rusty-mq` crate (keeps the dependency direction
//! one-way: management defines, the binary implements).

use rusty_mq_core::auth::{Permissions, Role};
use rusty_mq_core::topology::TopologyError;

pub trait BrokerHandle: Send + Sync + 'static {
    fn authenticate(&self, user: &str, pass: &str) -> bool;
    fn role_of(&self, user: &str) -> Option<Role>;
    /// Recovery complete and admissions available (readiness).
    fn is_ready(&self) -> bool;
    /// Prometheus text for /metrics.
    fn render_metrics(&self) -> String;
    /// Version/uptime/storage/alarm summary for /v1/status.
    fn status_summary(&self) -> serde_json::Value;
    fn list_vhosts(&self) -> Vec<serde_json::Value>;
    fn create_vhost(&self, name: &str) -> Result<(), String>;
    fn delete_vhost(&self, name: &str) -> Result<(), String>;
    /// Queue rows with name, ready count, consumer count.
    fn list_queues(&self, vhost: &str) -> Vec<serde_json::Value>;
    fn list_bindings(&self, vhost: &str) -> Vec<serde_json::Value>;
    fn purge_queue(&self, vhost: &str, queue: &str) -> Result<u64, TopologyError>;
    fn delete_queue(&self, vhost: &str, queue: &str) -> Result<(), TopologyError>;
    fn create_user(&self, username: &str, password: &str, role: Role) -> Result<(), String>;
    /// (username, role) rows — never password material.
    fn list_users(&self) -> Vec<serde_json::Value>;
    fn rotate_credentials(&self, username: &str, password: &str) -> Result<bool, String>;
    fn delete_user(&self, username: &str) -> Result<bool, String>;
    fn get_permissions(&self, username: &str, vhost: &str) -> Option<Permissions>;
    fn set_permissions(
        &self,
        username: &str,
        vhost: &str,
        perms: Permissions,
    ) -> Result<(), String>;
    fn delete_permissions(&self, username: &str, vhost: &str) -> Result<bool, String>;
    /// Live connections as (id-string, username).
    fn list_connections(&self) -> Vec<(String, String)>;
    fn list_channels(&self) -> Vec<serde_json::Value>;
    /// Server-initiated close; false when no such live connection.
    fn close_connection(&self, id: &str, reason: &str) -> bool;
    /// All permission rows as JSON objects.
    fn list_permissions(&self) -> Vec<serde_json::Value>;
    /// Native definitions export (durable topology).
    fn export_definitions(&self) -> serde_json::Value;
    /// Import definitions; returns the per-resource report as JSON.
    fn import_definitions(
        &self,
        payload: &serde_json::Value,
        dry_run: bool,
    ) -> Result<serde_json::Value, String>;
}

impl<T: BrokerHandle> BrokerHandle for std::sync::Arc<T> {
    fn authenticate(&self, user: &str, pass: &str) -> bool {
        (**self).authenticate(user, pass)
    }
    fn role_of(&self, user: &str) -> Option<Role> {
        (**self).role_of(user)
    }
    fn is_ready(&self) -> bool {
        (**self).is_ready()
    }
    fn render_metrics(&self) -> String {
        (**self).render_metrics()
    }
    fn status_summary(&self) -> serde_json::Value {
        (**self).status_summary()
    }
    fn create_vhost(&self, name: &str) -> Result<(), String> {
        (**self).create_vhost(name)
    }
    fn delete_vhost(&self, name: &str) -> Result<(), String> {
        (**self).delete_vhost(name)
    }
    fn list_vhosts(&self) -> Vec<serde_json::Value> {
        (**self).list_vhosts()
    }
    fn list_queues(&self, vhost: &str) -> Vec<serde_json::Value> {
        (**self).list_queues(vhost)
    }
    fn list_bindings(&self, vhost: &str) -> Vec<serde_json::Value> {
        (**self).list_bindings(vhost)
    }
    fn purge_queue(&self, vhost: &str, queue: &str) -> Result<u64, TopologyError> {
        (**self).purge_queue(vhost, queue)
    }
    fn delete_queue(&self, vhost: &str, queue: &str) -> Result<(), TopologyError> {
        (**self).delete_queue(vhost, queue)
    }
    fn create_user(&self, username: &str, password: &str, role: Role) -> Result<(), String> {
        (**self).create_user(username, password, role)
    }
    fn list_users(&self) -> Vec<serde_json::Value> {
        (**self).list_users()
    }
    fn rotate_credentials(&self, username: &str, password: &str) -> Result<bool, String> {
        (**self).rotate_credentials(username, password)
    }
    fn delete_user(&self, username: &str) -> Result<bool, String> {
        (**self).delete_user(username)
    }
    fn get_permissions(&self, username: &str, vhost: &str) -> Option<Permissions> {
        (**self).get_permissions(username, vhost)
    }
    fn set_permissions(
        &self,
        username: &str,
        vhost: &str,
        perms: Permissions,
    ) -> Result<(), String> {
        (**self).set_permissions(username, vhost, perms)
    }
    fn delete_permissions(&self, username: &str, vhost: &str) -> Result<bool, String> {
        (**self).delete_permissions(username, vhost)
    }
    fn list_connections(&self) -> Vec<(String, String)> {
        (**self).list_connections()
    }
    fn list_channels(&self) -> Vec<serde_json::Value> {
        (**self).list_channels()
    }
    fn close_connection(&self, id: &str, reason: &str) -> bool {
        (**self).close_connection(id, reason)
    }
    fn list_permissions(&self) -> Vec<serde_json::Value> {
        (**self).list_permissions()
    }
    fn export_definitions(&self) -> serde_json::Value {
        (**self).export_definitions()
    }
    fn import_definitions(
        &self,
        payload: &serde_json::Value,
        dry_run: bool,
    ) -> Result<serde_json::Value, String> {
        (**self).import_definitions(payload, dry_run)
    }
}
