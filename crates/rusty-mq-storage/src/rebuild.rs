//! Restart replay: fold committed journal records into a fresh live state
//! (§9.8). Runs before the broker accepts traffic; the journal is the only
//! source of truth (ADR-0002), so this rebuild is authoritative, not a
//! merge with any pre-existing memory state.

use rusty_mq_core::ids::{bump_past, ExchangeId, QueueId, VhostId};
use rusty_mq_core::routing::ExchangeType;
use rusty_mq_core::store::{MessageStore, StoredMessage};
use rusty_mq_core::topology::{CompatibilitySwitches, QueueProfile, Topology};

use crate::journal::{recover, JournalWriter};
use crate::record::Record;

/// The rebuilt broker state after recovery.
pub struct Rebuilt {
    pub topology: Topology,
    pub store: MessageStore,
    /// Count of committed records replayed (observability; §12 telemetry).
    pub replayed: usize,
    /// Highest restored entity id (id-mint bump safety).
    pub max_entity_id: u64,
}

/// The default vhost's id must be stable across restarts: the fresh
/// `Topology::new` mints ids for its built-ins, so replay of durable
/// topology (which references the default vhost) must land in the SAME
/// vhost instance. Rebuilt therefore reuses the fresh topology's default
/// vhost id for records journaled against vhost 0 (the only vhost in V1
/// before the management API exists).
const JOURNALED_DEFAULT_VHOST: u64 = 0;

/// Rebuild live state from the journal at `dir`. The byte budget mirrors
/// the runtime store budget (caller-supplied).
pub fn rebuild(
    dir: &std::path::Path,
    byte_budget: usize,
) -> Result<Rebuilt, crate::record::FormatError> {
    let records = recover(dir)?;
    let mut topology = Topology::new(CompatibilitySwitches::default());
    let mut store = MessageStore::new(byte_budget);
    let default_vhost = topology
        .find_vhost("/")
        .expect("fresh topology always has the default vhost");

    let mut replayed = 0usize;
    let mut max_entity_id = 0u64;
    for item in records {
        replayed += 1;
        match item.record {
            Record::QueueDeclare(q) => {
                max_entity_id = max_entity_id.max(q.id);
                // Live ownership is session state; restored durable queues
                // are non-exclusive by construction (§9.3).
                topology.restore_queue(
                    vhost_of(&topology, default_vhost),
                    &q.name,
                    QueueId::from_raw(q.id),
                    QueueProfile {
                        durable: q.durable,
                        exclusive: false,
                        auto_delete: false,
                    },
                );
            }
            Record::QueueDelete { id } => {
                topology.remove_queue_by_id(default_vhost, QueueId::from_raw(id));
                store.drain(QueueId::from_raw(id));
            }
            Record::ExchangeDeclare(e) => {
                max_entity_id = max_entity_id.max(e.id);
                let kind = match e.kind {
                    0 => ExchangeType::Direct,
                    1 => ExchangeType::Fanout,
                    _ => ExchangeType::Topic,
                };
                topology.restore_exchange(
                    vhost_of(&topology, default_vhost),
                    &e.name,
                    ExchangeId::from_raw(e.id),
                    kind,
                    e.durable,
                    e.auto_delete,
                    e.internal,
                );
            }
            Record::ExchangeDelete { id } => {
                topology.remove_exchange_by_id(default_vhost, ExchangeId::from_raw(id));
            }
            Record::Bind(b) => {
                topology.restore_binding(
                    vhost_of(&topology, default_vhost),
                    ExchangeId::from_raw(b.exchange),
                    QueueId::from_raw(b.queue),
                    &b.routing_key,
                );
            }
            Record::Unbind(b) => {
                topology.remove_binding(
                    vhost_of(&topology, default_vhost),
                    ExchangeId::from_raw(b.exchange),
                    QueueId::from_raw(b.queue),
                    &b.routing_key,
                );
            }
            Record::Enqueue(e) => {
                for (queue, seq) in &e.destinations {
                    let message = StoredMessage {
                        property_bytes: e.property_bytes.clone(),
                        body: e.body.clone(),
                        exchange: e.exchange.clone(),
                        routing_key: e.routing_key.clone(),
                        persistent: e.persistent,
                        // Delivery-attempt markers (§9.6) arrive with M5;
                        // restored entries are conservatively ready.
                        redelivered: false,
                    };
                    store.restore_with_seq(QueueId::from_raw(*queue), *seq, message);
                }
            }
            Record::SettleAck { queue, seq } | Record::SettleDiscard { queue, seq } => {
                store.discard(QueueId::from_raw(queue), seq);
            }
            Record::Delivered { queue, seq } => {
                store.mark_redelivered(QueueId::from_raw(queue), seq);
            }
            Record::Purge { queue, seqs } => {
                for seq in seqs {
                    store.discard(QueueId::from_raw(queue), seq);
                }
            }
        }
    }
    // Freshly minted ids must never collide with restored identities.
    bump_past(max_entity_id);
    Ok(Rebuilt {
        topology,
        store,
        replayed,
        max_entity_id,
    })
}

fn vhost_of(_topology: &Topology, default: VhostId) -> VhostId {
    // V1 before the management API journals only the default vhost
    // (JOURNALED_DEFAULT_VHOST); keep the indirection explicit so adding
    // vhosts later changes one place.
    let _ = JOURNALED_DEFAULT_VHOST;
    default
}

/// Open a persistent broker backend: recover first (rebuild live state),
/// then open the writer for appends. The recovery root is the journal.
pub fn open_persistent(
    dir: &std::path::Path,
    byte_budget: usize,
    config: crate::journal::JournalConfig,
) -> Result<(Rebuilt, JournalWriter), crate::record::FormatError> {
    // A first run initializes an empty directory; an existing directory
    // must validate (failures inside recover() are explicit).
    std::fs::create_dir_all(dir).map_err(|e| crate::record::FormatError::Io(e.to_string()))?;
    let rebuilt = rebuild(dir, byte_budget)?;
    let writer = JournalWriter::open(dir, config)?;
    Ok((rebuilt, writer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::JournalConfig;
    use crate::record::{Enqueue, QueueRecord};
    use std::fs;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-rebuild-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn rebuild_restores_topology_messages_and_settlements() {
        let dir = tmp("full");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[Record::QueueDeclare(QueueRecord {
                name: "jobs".into(),
                id: 5,
                durable: true,
                exclusive: false,
                auto_delete: false,
                owner: 0,
            })])
            .unwrap();
            w.commit(&[Record::Enqueue(Enqueue {
                message_id: 1,
                property_bytes: vec![9],
                body: b"one".to_vec(),
                exchange: "".into(),
                routing_key: "jobs".into(),
                persistent: true,
                destinations: vec![(5, 0), (5, 1)],
            })])
            .unwrap();
            // The second entry was acked before the "crash".
            w.commit(&[Record::SettleAck { queue: 5, seq: 1 }]).unwrap();
            drop(w);
        }

        let mut rebuilt = rebuild(&dir, 1024 * 1024).unwrap();
        assert_eq!(rebuilt.replayed, 3);
        let vhost = rebuilt.topology.find_vhost("/").unwrap();
        let qid = rebuilt
            .topology
            .find_queue(vhost, "jobs")
            .expect("queue restored");
        assert_eq!(qid.to_raw(), 5, "journaled identity preserved");
        assert_eq!(
            rebuilt.store.len(qid),
            1,
            "settled entry gone, other restored"
        );
        let entry = rebuilt.store.pop_ready(qid).unwrap();
        assert_eq!(entry.seq, 0);
        assert_eq!(entry.message.body, b"one".to_vec());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rebuild_after_delete_does_not_resurrect() {
        let dir = tmp("delete");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[Record::QueueDeclare(QueueRecord {
                name: "temp".into(),
                id: 3,
                durable: true,
                exclusive: false,
                auto_delete: false,
                owner: 0,
            })])
            .unwrap();
            w.commit(&[Record::Enqueue(Enqueue {
                message_id: 1,
                property_bytes: vec![],
                body: b"x".to_vec(),
                exchange: "".into(),
                routing_key: "temp".into(),
                persistent: true,
                destinations: vec![(3, 0)],
            })])
            .unwrap();
            w.commit(&[Record::QueueDelete { id: 3 }]).unwrap();
            drop(w);
        }
        let rebuilt = rebuild(&dir, 1024 * 1024).unwrap();
        let vhost = rebuilt.topology.find_vhost("/").unwrap();
        assert!(rebuilt.topology.find_queue(vhost, "temp").is_none());
        assert_eq!(rebuilt.store.len(rusty_mq_core::QueueId::for_test(3)), 0);
        let _ = fs::remove_dir_all(&dir);
    }
}
