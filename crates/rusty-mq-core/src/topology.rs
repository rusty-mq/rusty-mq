//! In-memory topology registry: vhosts, exchanges, queues, bindings (M2).
//!
//! Durable persistence lands in M4 (`rusty-mq-storage`); until then this
//! registry is memory-backed and makes no persistence claim (PRD early
//! safety constraint).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::ids::{ExchangeId, QueueId, VhostId};
use crate::routing::{Binding, ExchangeType};

/// Queue profile flags as declared on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueProfile {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
}

/// Compatibility switch for the optional shared-transient profile (§5.3).
#[derive(Clone, Debug)]
pub struct CompatibilitySwitches {
    /// Permit non-durable, non-exclusive queues (off by default in V1).
    pub allow_transient_nonexclusive_queues: bool,
    /// Reject unknown topology arguments (on by default; ADR-0005).
    pub reject_unknown_arguments: bool,
}

impl Default for CompatibilitySwitches {
    fn default() -> Self {
        Self {
            allow_transient_nonexclusive_queues: false,
            reject_unknown_arguments: true,
        }
    }
}

/// Identity of a binding: (exchange, queue, key) triples are unique.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BindingKey {
    pub exchange: ExchangeId,
    pub queue: QueueId,
    pub key: String,
}

/// Stored exchange record.
#[derive(Clone, Debug)]
pub struct ExchangeRecord {
    pub id: ExchangeId,
    pub vhost: VhostId,
    pub name: String,
    pub kind: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    /// `internal=true`: exists, may be bound from, but direct client
    /// publication is refused (FR-E03).
    pub internal: bool,
}

/// Stored queue record.
#[derive(Clone, Debug)]
pub struct QueueRecord {
    pub id: QueueId,
    pub vhost: VhostId,
    pub name: String,
    pub profile: QueueProfile,
    /// Owning connection for exclusive queues; `None` otherwise.
    pub owner_connection: Option<crate::ids::ConnectionId>,
    /// True once this queue has ever had a consumer (auto-delete rule).
    pub has_had_consumer: bool,
}

/// Errors from topology mutations; the protocol layer maps these to the
/// frozen error profile (404/405/406/530).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TopologyError {
    #[error("vhost not found")]
    VhostNotFound,
    #[error("exchange '{0}' not found")]
    ExchangeNotFound(String),
    #[error("queue '{0}' not found")]
    QueueNotFound(String),
    #[error("exchange '{0}' already exists with different properties")]
    ExchangePreconditionFailed(String),
    #[error("queue '{0}' already exists with different properties")]
    QueuePreconditionFailed(String),
    #[error("queue '{0}' is exclusive to another connection")]
    QueueLocked(String),
    #[error("binding already exists (idempotent)")]
    BindingExists,
    #[error("binding not found")]
    BindingNotFound,
    #[error("'{0}' is a reserved amq. name")]
    ReservedName(String),
    #[error("queue not unused")]
    QueueInUse,
    #[error("queue not empty")]
    QueueNotEmpty,
    /// Journal commit failed for a durable mutation; live state unchanged.
    /// (Storage-layer failure surfaced through the topology vocabulary so
    /// handlers map it uniformly to 506.)
    #[error("durable journal commit failed")]
    ResourceErrorJournal,
}

/// Declaration-rejection reasons for unsupported V1 profiles (§5.3 table).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DeclareQueueError {
    #[error("durable+exclusive queues are not supported in V1")]
    DurableExclusive,
    #[error("durable+auto-delete queues are not supported in V1")]
    DurableAutoDelete,
    #[error("transient non-exclusive queues require the compatibility switch")]
    TransientNonExclusive,
    #[error(transparent)]
    Topology(#[from] TopologyError),
}

/// Declaration-rejection reasons for exchanges.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DeclareExchangeError {
    #[error("exchange type '{0}' is not supported")]
    UnsupportedType(String),
    #[error("internal exchanges cannot be declared by clients with internal=false change")]
    InternalConflict,
    #[error(transparent)]
    Topology(#[from] TopologyError),
}

/// The full in-memory topology for one broker instance.
///
/// All maps are keyed by opaque internal ids; names are stored on the records
/// and secondary name indexes exist per vhost for lookup. Deleted+recreated
/// entities get fresh ids (INV-07).
pub struct Topology {
    vhosts: HashMap<VhostId, String>,
    /// vhost -> (exchange name -> id)
    exchange_names: HashMap<VhostId, BTreeMap<String, ExchangeId>>,
    /// vhost -> (queue name -> id)
    queue_names: HashMap<VhostId, BTreeMap<String, QueueId>>,
    exchanges: HashMap<ExchangeId, ExchangeRecord>,
    queues: HashMap<QueueId, QueueRecord>,
    /// vhost -> exchange id -> bindings from that exchange.
    bindings: HashMap<VhostId, HashMap<ExchangeId, Vec<Binding>>>,
    seq: AtomicU64,
    compat: CompatibilitySwitches,
}

impl Topology {
    /// Empty topology with the given compatibility switches.
    pub fn new(compat: CompatibilitySwitches) -> Self {
        let mut t = Self {
            vhosts: HashMap::new(),
            exchange_names: HashMap::new(),
            queue_names: HashMap::new(),
            exchanges: HashMap::new(),
            queues: HashMap::new(),
            bindings: HashMap::new(),
            seq: AtomicU64::new(0),
            compat,
        };
        t.add_vhost("/".to_string());
        t
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Create a vhost (idempotent on name).
    pub fn add_vhost(&mut self, name: String) -> VhostId {
        if let Some((id, _)) = self.vhosts.iter().find(|(_, n)| *n == &name) {
            return *id;
        }
        let id = VhostId::new();
        self.vhosts.insert(id, name);
        self.exchange_names.entry(id).or_default();
        self.queue_names.entry(id).or_default();
        self.bindings.entry(id).or_default();
        self.ensure_builtin_exchanges(id);
        id
    }

    /// Look up a vhost by name (exact match; "/" is the default vhost).
    /// All vhost names (unordered).
    pub fn vhost_names(&self) -> impl Iterator<Item = String> + '_ {
        self.vhosts.values().cloned()
    }

    /// Exchange names declared in a vhost (§12.1 destructive checks).
    pub fn exchange_names_of(&self, vhost: VhostId) -> Vec<String> {
        self.exchange_names
            .get(&vhost)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Queue count in a vhost by name map size (cheap; §12.1 checks).
    pub fn queue_name_count(&self, vhost: VhostId) -> usize {
        self.queue_names.get(&vhost).map(|m| m.len()).unwrap_or(0)
    }

    /// Remove a vhost's registry entries (name maps + bindings). Queue/
    /// exchange records keep their vhost ids; with the name map gone the
    /// ids are unreachable (deletion is gated upstream on emptiness, so
    /// nothing live references them).
    pub fn remove_vhost(&mut self, name: &str) {
        if let Some(id) = self.find_vhost(name) {
            self.vhosts.remove(&id);
            self.exchange_names.remove(&id);
            self.queue_names.remove(&id);
            self.bindings.remove(&id);
        }
    }

    pub fn find_vhost(&self, name: &str) -> Option<VhostId> {
        self.vhosts
            .iter()
            .find(|(_, n)| n.as_str() == name)
            .map(|(id, _)| *id)
    }

    /// Declare or passively check the default exchange and `amq.*` built-ins
    /// for a vhost. Called at vhost creation.
    fn ensure_builtin_exchanges(&mut self, vhost: VhostId) {
        for (name, kind) in [
            ("", ExchangeType::Direct),
            ("amq.direct", ExchangeType::Direct),
            ("amq.fanout", ExchangeType::Fanout),
            ("amq.topic", ExchangeType::Topic),
        ] {
            if !self.exchange_names[&vhost].contains_key(name) {
                let id = ExchangeId::new();
                self.exchanges.insert(
                    id,
                    ExchangeRecord {
                        id,
                        vhost,
                        name: name.to_string(),
                        kind,
                        durable: true,
                        auto_delete: false,
                        internal: false,
                    },
                );
                self.exchange_names
                    .entry(vhost)
                    .or_default()
                    .insert(name.to_string(), id);
            }
        }
    }

    /// Declare an exchange (create or equivalence check). `None` name means a
    /// server-generated name is not allowed for exchanges (they must be
    /// named).
    pub fn declare_exchange(
        &mut self,
        vhost: VhostId,
        name: &str,
        kind: ExchangeType,
        durable: bool,
        auto_delete: bool,
        internal: bool,
    ) -> Result<ExchangeId, DeclareExchangeError> {
        if !self.vhosts.contains_key(&vhost) {
            return Err(TopologyError::VhostNotFound.into());
        }
        self.ensure_builtin_exchanges(vhost);
        if let Some(existing) = self.exchange_names[&vhost].get(name) {
            let rec = &self.exchanges[existing];
            let equivalent = rec.kind == kind
                && rec.durable == durable
                && rec.auto_delete == auto_delete
                && rec.internal == internal;
            return if equivalent {
                Ok(*existing)
            } else {
                Err(TopologyError::ExchangePreconditionFailed(name.to_string()).into())
            };
        }
        if is_reserved_exchange_name(name) {
            // Only predeclared built-ins may carry amq. names.
            return Err(TopologyError::ReservedName(name.to_string()).into());
        }
        let id = ExchangeId::new();
        self.exchanges.insert(
            id,
            ExchangeRecord {
                id,
                vhost,
                name: name.to_string(),
                kind,
                durable,
                auto_delete,
                internal,
            },
        );
        self.exchange_names
            .entry(vhost)
            .or_default()
            .insert(name.to_string(), id);
        Ok(id)
    }

    /// Declare (or passively check via `passive=true` handled by caller) a
    /// queue. Enforces the V1 profile restrictions.
    pub fn declare_queue(
        &mut self,
        vhost: VhostId,
        name: &str,
        profile: QueueProfile,
        owner: Option<crate::ids::ConnectionId>,
    ) -> Result<QueueId, DeclareQueueError> {
        if !self.vhosts.contains_key(&vhost) {
            return Err(TopologyError::VhostNotFound.into());
        }
        self.ensure_builtin_exchanges(vhost);
        if !name.is_empty() {
            if let Some(existing) = self.queue_names[&vhost].get(name) {
                let rec = &self.queues[existing];
                let equivalent =
                    rec.profile == profile && rec.owner_connection.is_some() == profile.exclusive;
                if !equivalent {
                    return Err(TopologyError::QueuePreconditionFailed(name.to_string()).into());
                }
                // An equivalent redeclare by a *different* connection is a
                // lock conflict when the queue is exclusive (405, FR-Q04).
                if rec.profile.exclusive && rec.owner_connection != owner {
                    return Err(TopologyError::QueueLocked(name.to_string()).into());
                }
                return Ok(*existing);
            }
        }
        // V1 profile gate (§5.3).
        if profile.durable && profile.exclusive {
            return Err(DeclareQueueError::DurableExclusive);
        }
        if profile.durable && profile.auto_delete {
            return Err(DeclareQueueError::DurableAutoDelete);
        }
        if !profile.durable
            && !profile.exclusive
            && !self.compat.allow_transient_nonexclusive_queues
        {
            return Err(DeclareQueueError::TransientNonExclusive);
        }
        let generated = name.is_empty();
        let id = QueueId::new();
        let name = if generated {
            format!("amq.gen-jr{}", self.next_seq())
        } else {
            name.to_string()
        };
        self.queues.insert(
            id,
            QueueRecord {
                id,
                vhost,
                name: name.clone(),
                profile,
                owner_connection: if profile.exclusive { owner } else { None },
                has_had_consumer: false,
            },
        );
        self.queue_names.entry(vhost).or_default().insert(name, id);
        Ok(id)
    }

    /// Resolve a queue by name within a vhost.
    pub fn find_queue(&self, vhost: VhostId, name: &str) -> Option<QueueId> {
        self.queue_names.get(&vhost)?.get(name).copied()
    }

    /// Resolve an exchange by name within a vhost.
    pub fn find_exchange(&self, vhost: VhostId, name: &str) -> Option<ExchangeId> {
        self.exchange_names.get(&vhost)?.get(name).copied()
    }

    pub fn queue_record(&self, id: QueueId) -> Option<&QueueRecord> {
        self.queues.get(&id)
    }

    pub fn exchange_record(&self, id: ExchangeId) -> Option<&ExchangeRecord> {
        self.exchanges.get(&id)
    }

    /// Fresh topology for projection loading (same shape as `new`).
    pub fn default_for_projection() -> Self {
        Self::new(CompatibilitySwitches::default())
    }

    /// Replay-only: restore a queue exactly as journaled (durable
    /// declarations only). Idempotent on identity.
    pub fn restore_queue(
        &mut self,
        vhost: VhostId,
        name: &str,
        id: QueueId,
        profile: QueueProfile,
    ) {
        if self.queues.contains_key(&id) {
            return;
        }
        self.queues.insert(
            id,
            QueueRecord {
                id,
                vhost,
                name: name.to_string(),
                profile,
                owner_connection: None, // ownership is session state, never restored (§9.3)
                has_had_consumer: false,
            },
        );
        self.queue_names
            .entry(vhost)
            .or_default()
            .insert(name.to_string(), id);
    }

    /// Replay-only: restore an exchange exactly as journaled.
    #[allow(clippy::too_many_arguments)] // replay mirror of the journal record
    pub fn restore_exchange(
        &mut self,
        vhost: VhostId,
        name: &str,
        id: ExchangeId,
        kind: crate::routing::ExchangeType,
        durable: bool,
        auto_delete: bool,
        internal: bool,
    ) {
        if self.exchanges.contains_key(&id) {
            return;
        }
        self.exchanges.insert(
            id,
            ExchangeRecord {
                id,
                vhost,
                name: name.to_string(),
                kind,
                durable,
                auto_delete,
                internal,
            },
        );
        self.exchange_names
            .entry(vhost)
            .or_default()
            .insert(name.to_string(), id);
    }

    /// Iterate all queues with their records (snapshot capture).
    pub fn iter_queues(&self) -> impl Iterator<Item = (QueueId, &QueueRecord)> {
        self.queues.iter().map(|(id, rec)| (*id, rec))
    }

    /// Iterate all exchanges with their records (snapshot capture).
    pub fn iter_exchanges(&self) -> impl Iterator<Item = (ExchangeId, &ExchangeRecord)> {
        self.exchanges.iter().map(|(id, rec)| (*id, rec))
    }

    /// Iterate bindings as (exchange, queue, key) triples across vhosts
    /// (snapshot capture).
    pub fn iter_bindings(&self) -> impl Iterator<Item = (ExchangeId, QueueId, &str)> {
        self.bindings.values().flat_map(|per_vhost| {
            per_vhost
                .iter()
                .flat_map(|(ex, list)| list.iter().map(|b| (*ex, b.queue, b.key.as_str())))
        })
    }

    /// Remove an exchange by id (replay path); drops its bindings.
    pub fn remove_exchange_by_id(&mut self, vhost: VhostId, id: ExchangeId) {
        if let Some(rec) = self.exchanges.remove(&id) {
            if let Some(names) = self.exchange_names.get_mut(&vhost) {
                names.remove(&rec.name);
            }
            if let Some(vb) = self.bindings.get_mut(&vhost) {
                vb.remove(&id);
            }
        }
    }

    /// Remove one binding by identity (replay path); no error if absent.
    pub fn remove_binding(
        &mut self,
        vhost: VhostId,
        exchange: ExchangeId,
        queue: QueueId,
        key: &str,
    ) {
        if let Some(list) = self
            .bindings
            .get_mut(&vhost)
            .and_then(|m| m.get_mut(&exchange))
        {
            list.retain(|b| !(b.queue == queue && b.key == key));
        }
    }

    /// Replay-only: restore a binding (idempotent).
    pub fn restore_binding(
        &mut self,
        vhost: VhostId,
        exchange: ExchangeId,
        queue: QueueId,
        key: &str,
    ) {
        let list = self
            .bindings
            .entry(vhost)
            .or_default()
            .entry(exchange)
            .or_default();
        if list.iter().any(|b| b.queue == queue && b.key == key) {
            return;
        }
        list.push(crate::routing::Binding {
            exchange,
            queue,
            key: key.to_string(),
        });
    }

    /// Remove every queue owned by `connection` (exclusive queues and their
    /// bindings) — called when the owning connection ends (FR-Q04/FR-P09).
    /// Returns the removed queue ids so callers can drop associated state.
    pub fn remove_owned_queues(
        &mut self,
        vhost: VhostId,
        connection: crate::ids::ConnectionId,
    ) -> Vec<QueueId> {
        let owned: Vec<QueueId> = self
            .queues
            .values()
            .filter(|q| q.vhost == vhost && q.owner_connection == Some(connection))
            .map(|q| q.id)
            .collect();
        for id in &owned {
            self.remove_queue_by_id(vhost, *id);
        }
        owned
    }

    /// Number of bindings attached to an exchange (for `exchange.delete
    /// if_unused`).
    /// Total queues in a vhost (§10 admission budget check).
    pub fn queue_count(&self, vhost: VhostId) -> usize {
        self.queues.values().filter(|q| q.vhost == vhost).count()
    }

    /// Whether an exact (exchange, queue, routing-key) binding exists —
    /// duplicate binds are idempotent and never grow admission budgets.
    pub fn binding_exists(
        &self,
        vhost: VhostId,
        exchange: ExchangeId,
        queue: QueueId,
        key: &str,
    ) -> bool {
        self.bindings
            .get(&vhost)
            .and_then(|m| m.get(&exchange))
            .is_some_and(|v| v.iter().any(|b| b.queue == queue && b.key == key))
    }

    /// Total bindings in a vhost across exchanges (§10 admission budget).
    pub fn bindings_total(&self, vhost: VhostId) -> usize {
        self.bindings
            .get(&vhost)
            .map(|m| m.values().map(|v| v.len()).sum())
            .unwrap_or(0)
    }

    pub fn binding_count(&self, vhost: VhostId, exchange: ExchangeId) -> usize {
        self.bindings_of(vhost, exchange).len()
    }

    /// Add a binding; idempotent on (exchange, queue, key).
    pub fn bind(
        &mut self,
        vhost: VhostId,
        exchange: ExchangeId,
        queue: QueueId,
        key: &str,
    ) -> Result<(), TopologyError> {
        let ex = self
            .exchanges
            .get(&exchange)
            .filter(|e| e.vhost == vhost)
            .ok_or_else(|| {
                TopologyError::ExchangeNotFound(
                    self.exchanges
                        .get(&exchange)
                        .map(|e| e.name.clone())
                        .unwrap_or_default(),
                )
            })?;
        if ex.name.is_empty() {
            // Default-exchange bindings are implicit only (FR-E07).
            return Err(TopologyError::ReservedName("default exchange".into()));
        }
        if self.queues.get(&queue).is_none_or(|q| q.vhost != vhost) {
            return Err(TopologyError::QueueNotFound(String::new()));
        }
        let list = self
            .bindings
            .entry(vhost)
            .or_default()
            .entry(exchange)
            .or_default();
        if list.iter().any(|b| b.queue == queue && b.key == key) {
            return Ok(()); // idempotent (FR-E04)
        }
        list.push(Binding {
            exchange,
            queue,
            key: key.to_string(),
        });
        Ok(())
    }

    /// Remove a binding (no error if it does not exist is NOT allowed:
    /// unbind of a missing binding is an error per AMQP).
    pub fn unbind(
        &mut self,
        vhost: VhostId,
        exchange: ExchangeId,
        queue: QueueId,
        key: &str,
    ) -> Result<(), TopologyError> {
        let list = self
            .bindings
            .get_mut(&vhost)
            .and_then(|m| m.get_mut(&exchange))
            .ok_or(TopologyError::BindingNotFound)?;
        let before = list.len();
        list.retain(|b| !(b.queue == queue && b.key == key));
        if list.len() == before {
            return Err(TopologyError::BindingNotFound);
        }
        // Auto-delete exchange: delete after losing its final binding.
        self.maybe_auto_delete_exchange(vhost, exchange);
        Ok(())
    }

    fn maybe_auto_delete_exchange(&mut self, vhost: VhostId, exchange: ExchangeId) {
        let should_delete = self
            .exchanges
            .get(&exchange)
            .is_some_and(|e| e.auto_delete && e.vhost == vhost)
            && self
                .bindings
                .get(&vhost)
                .and_then(|m| m.get(&exchange))
                .is_none_or(|l| l.is_empty());
        if should_delete {
            self.delete_exchange_internal(vhost, exchange);
        }
    }

    /// Delete an exchange; built-ins are protected.
    pub fn delete_exchange(
        &mut self,
        vhost: VhostId,
        name: &str,
    ) -> Result<ExchangeId, TopologyError> {
        let id = self
            .find_exchange(vhost, name)
            .ok_or_else(|| TopologyError::ExchangeNotFound(name.to_string()))?;
        if is_reserved_exchange_name(name) {
            return Err(TopologyError::ReservedName(name.to_string()));
        }
        self.delete_exchange_internal(vhost, id);
        Ok(id)
    }

    fn delete_exchange_internal(&mut self, vhost: VhostId, id: ExchangeId) {
        if let Some(rec) = self.exchanges.remove(&id) {
            if let Some(names) = self.exchange_names.get_mut(&vhost) {
                names.remove(&rec.name);
            }
            if let Some(vb) = self.bindings.get_mut(&vhost) {
                vb.remove(&id);
            }
        }
    }

    /// Delete a queue with `if_unused`/`if_empty` conditions. Returns the
    /// queue id so callers can drop its messages and delivery state.
    ///
    /// `consumer_count` and `ready_messages` are supplied by the queue
    /// scheduler at the ordering point of the delete.
    pub fn delete_queue(
        &mut self,
        vhost: VhostId,
        name: &str,
        if_unused: bool,
        if_empty: bool,
        consumer_count: usize,
        ready_messages: u64,
    ) -> Result<QueueId, TopologyError> {
        let id = self
            .find_queue(vhost, name)
            .ok_or_else(|| TopologyError::QueueNotFound(name.to_string()))?;
        if if_unused && consumer_count > 0 {
            return Err(TopologyError::QueueInUse);
        }
        if if_empty && ready_messages > 0 {
            return Err(TopologyError::QueueNotEmpty);
        }
        self.remove_queue_by_id(vhost, id);
        Ok(id)
    }

    /// Remove a queue by id (used by connection teardown for exclusive and
    /// auto-delete queues). Idempotent.
    pub fn remove_queue_by_id(&mut self, vhost: VhostId, id: QueueId) {
        let mut affected: Vec<ExchangeId> = Vec::new();
        if let Some(rec) = self.queues.remove(&id) {
            if let Some(names) = self.queue_names.get_mut(&vhost) {
                names.remove(&rec.name);
            }
            if let Some(vb) = self.bindings.get_mut(&vhost) {
                for (exchange, list) in vb.iter_mut() {
                    let before = list.len();
                    list.retain(|b| b.queue != id);
                    if list.len() != before {
                        affected.push(*exchange);
                    }
                }
            }
        }
        // An auto-delete exchange whose last binding disappeared with this
        // queue is itself deleted (FR-E03 lifecycle).
        for exchange in affected {
            self.maybe_auto_delete_exchange(vhost, exchange);
        }
    }

    /// Bindings from an exchange within a vhost (for routing).
    pub fn bindings_of(&self, vhost: VhostId, exchange: ExchangeId) -> &[Binding] {
        self.bindings
            .get(&vhost)
            .and_then(|m| m.get(&exchange))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Called when a queue gains its first consumer (auto-delete lifecycle).
    pub fn queue_got_consumer(&mut self, id: QueueId) {
        if let Some(q) = self.queues.get_mut(&id) {
            q.has_had_consumer = true;
        }
    }

    /// Whether a queue exists and is owned by `connection` (exclusive queues).
    pub fn queue_owned_by(&self, id: QueueId, connection: crate::ids::ConnectionId) -> bool {
        self.queues
            .get(&id)
            .is_some_and(|q| q.owner_connection == Some(connection))
    }

    /// Exclusivity gate: `None` if the queue does not exist; `Some(true)` if
    /// access is allowed (not exclusive, or owned by this connection).
    pub fn check_exclusive_access(
        &self,
        id: QueueId,
        connection: crate::ids::ConnectionId,
    ) -> Option<bool> {
        self.queues
            .get(&id)
            .map(|q| !q.profile.exclusive || q.owner_connection == Some(connection))
    }
}

/// All `amq.` names are reserved. The predeclared built-ins (`amq.direct`,
/// `amq.fanout`, `amq.topic`) already exist when a vhost is created, so
/// reaching this check with such a name means someone tried to create it
/// from scratch — also refused.
pub fn is_reserved_exchange_name(name: &str) -> bool {
    name.starts_with("amq.")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ConnectionId;

    fn topo() -> Topology {
        Topology::new(CompatibilitySwitches::default())
    }

    fn vhost(t: &mut Topology) -> VhostId {
        t.find_vhost("/").unwrap()
    }

    #[test]
    fn default_vhost_and_builtin_exchanges_exist() {
        let mut t = topo();
        let v = vhost(&mut t);
        assert!(t.find_exchange(v, "").is_some());
        assert!(t.find_exchange(v, "amq.direct").is_some());
        assert!(t.find_exchange(v, "amq.fanout").is_some());
        assert!(t.find_exchange(v, "amq.topic").is_some());
        // Built-ins cannot be deleted.
        assert!(matches!(
            t.delete_exchange(v, "amq.topic"),
            Err(TopologyError::ReservedName(_))
        ));
    }

    #[test]
    fn declare_exchange_equivalence_and_conflict() {
        let mut t = topo();
        let v = vhost(&mut t);
        let id = t
            .declare_exchange(v, "jobs", ExchangeType::Direct, true, false, false)
            .unwrap();
        let again = t
            .declare_exchange(v, "jobs", ExchangeType::Direct, true, false, false)
            .unwrap();
        assert_eq!(id, again);
        assert!(matches!(
            t.declare_exchange(v, "jobs", ExchangeType::Fanout, true, false, false),
            Err(DeclareExchangeError::Topology(
                TopologyError::ExchangePreconditionFailed(_)
            ))
        ));
    }

    #[test]
    fn reserved_amq_names_rejected_for_new_exchanges() {
        let mut t = topo();
        let v = vhost(&mut t);
        assert!(matches!(
            t.declare_exchange(v, "amq.custom", ExchangeType::Direct, true, false, false),
            Err(DeclareExchangeError::Topology(TopologyError::ReservedName(
                _
            )))
        ));
    }

    #[test]
    fn queue_profiles_enforced() {
        let mut t = topo();
        let v = vhost(&mut t);
        let conn = ConnectionId::new();

        // Durable work queue: allowed.
        t.declare_queue(
            v,
            "work",
            QueueProfile {
                durable: true,
                exclusive: false,
                auto_delete: false,
            },
            None,
        )
        .unwrap();

        // Durable+exclusive: rejected.
        assert_eq!(
            t.declare_queue(
                v,
                "bad1",
                QueueProfile {
                    durable: true,
                    exclusive: true,
                    auto_delete: false
                },
                Some(conn)
            )
            .unwrap_err(),
            DeclareQueueError::DurableExclusive
        );

        // Durable+auto-delete: rejected.
        assert_eq!(
            t.declare_queue(
                v,
                "bad2",
                QueueProfile {
                    durable: true,
                    exclusive: false,
                    auto_delete: true
                },
                None
            )
            .unwrap_err(),
            DeclareQueueError::DurableAutoDelete
        );

        // Shared transient: rejected by default, allowed with switch.
        assert_eq!(
            t.declare_queue(
                v,
                "bad3",
                QueueProfile {
                    durable: false,
                    exclusive: false,
                    auto_delete: false
                },
                None
            )
            .unwrap_err(),
            DeclareQueueError::TransientNonExclusive
        );
        t.compat.allow_transient_nonexclusive_queues = true;
        t.declare_queue(
            v,
            "ok3",
            QueueProfile {
                durable: false,
                exclusive: false,
                auto_delete: false,
            },
            None,
        )
        .unwrap();
    }

    #[test]
    fn generated_queue_names_unique_and_redeclare_equivalent() {
        let mut t = topo();
        let v = vhost(&mut t);
        let conn = ConnectionId::new();
        let id1 = t
            .declare_queue(
                v,
                "",
                QueueProfile {
                    durable: false,
                    exclusive: true,
                    auto_delete: false,
                },
                Some(conn),
            )
            .unwrap();
        let rec = t.queue_record(id1).unwrap();
        let name1 = rec.name.clone();
        let id2 = t
            .declare_queue(
                v,
                &name1,
                QueueProfile {
                    durable: false,
                    exclusive: true,
                    auto_delete: false,
                },
                Some(conn),
            )
            .unwrap();
        assert_eq!(id1, id2);
        let id3 = t
            .declare_queue(
                v,
                "",
                QueueProfile {
                    durable: false,
                    exclusive: true,
                    auto_delete: false,
                },
                Some(conn),
            )
            .unwrap();
        assert_ne!(id1, id3);
        assert_ne!(name1, t.queue_record(id3).unwrap().name);
    }

    #[test]
    fn queue_recreation_gets_new_identity() {
        // INV-07.
        let mut t = topo();
        let v = vhost(&mut t);
        let id1 = t
            .declare_queue(
                v,
                "same-name",
                QueueProfile {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                },
                None,
            )
            .unwrap();
        t.remove_queue_by_id(v, id1);
        let id2 = t
            .declare_queue(
                v,
                "same-name",
                QueueProfile {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                },
                None,
            )
            .unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn bind_idempotent_and_unbind() {
        let mut t = topo();
        let v = vhost(&mut t);
        let ex = t
            .declare_exchange(v, "e", ExchangeType::Topic, false, false, false)
            .unwrap();
        let q = t
            .declare_queue(
                v,
                "",
                QueueProfile {
                    durable: false,
                    exclusive: true,
                    auto_delete: false,
                },
                Some(ConnectionId::new()),
            )
            .unwrap();
        t.bind(v, ex, q, "a.b").unwrap();
        t.bind(v, ex, q, "a.b").unwrap(); // idempotent
        assert_eq!(t.bindings_of(v, ex).len(), 1);
        t.unbind(v, ex, q, "a.b").unwrap();
        assert_eq!(t.bindings_of(v, ex).len(), 0);
        assert!(matches!(
            t.unbind(v, ex, q, "a.b"),
            Err(TopologyError::BindingNotFound)
        ));
    }

    #[test]
    fn default_exchange_cannot_be_bound_manually() {
        let mut t = topo();
        let v = vhost(&mut t);
        let def = t.find_exchange(v, "").unwrap();
        let q = t
            .declare_queue(
                v,
                "",
                QueueProfile {
                    durable: false,
                    exclusive: true,
                    auto_delete: false,
                },
                Some(ConnectionId::new()),
            )
            .unwrap();
        assert!(matches!(
            t.bind(v, def, q, "x"),
            Err(TopologyError::ReservedName(_))
        ));
    }

    #[test]
    fn auto_delete_exchange_dies_with_last_binding() {
        let mut t = topo();
        let v = vhost(&mut t);
        let ex = t
            .declare_exchange(v, "temp", ExchangeType::Fanout, false, true, false)
            .unwrap();
        let q = t
            .declare_queue(
                v,
                "",
                QueueProfile {
                    durable: false,
                    exclusive: true,
                    auto_delete: false,
                },
                Some(ConnectionId::new()),
            )
            .unwrap();
        t.bind(v, ex, q, "").unwrap();
        assert!(t.find_exchange(v, "temp").is_some());
        t.unbind(v, ex, q, "").unwrap();
        assert!(t.find_exchange(v, "temp").is_none());
    }

    #[test]
    fn delete_queue_conditions() {
        let mut t = topo();
        let v = vhost(&mut t);
        t.declare_queue(
            v,
            "cond",
            QueueProfile {
                durable: true,
                exclusive: false,
                auto_delete: false,
            },
            None,
        )
        .unwrap();
        assert!(matches!(
            t.delete_queue(v, "cond", true, true, 1, 0),
            Err(TopologyError::QueueInUse)
        ));
        assert!(matches!(
            t.delete_queue(v, "cond", true, true, 0, 5),
            Err(TopologyError::QueueNotEmpty)
        ));
        t.delete_queue(v, "cond", true, true, 0, 0).unwrap();
        assert!(t.find_queue(v, "cond").is_none());
    }
}
