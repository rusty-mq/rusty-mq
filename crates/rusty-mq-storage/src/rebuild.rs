//! Restart replay: fold committed journal records (and, when published, the
//! snapshot the manifest points at) into a fresh live state (§9.8). The
//! journal — or manifest+snapshot+journal-suffix — is the only source of
//! truth (ADR-0002); this rebuild is authoritative, not a merge.

use rusty_mq_core::ids::{bump_past, ExchangeId, QueueId, VhostId};
use rusty_mq_core::routing::ExchangeType;
use rusty_mq_core::store::{MessageStore, StoredMessage};
use rusty_mq_core::topology::{CompatibilitySwitches, QueueProfile, Topology};

use crate::journal::JournalWriter;
use crate::record::Record;

/// The rebuilt broker state after recovery.
pub struct Rebuilt {
    pub topology: Topology,
    pub store: MessageStore,
    /// Count of records replayed (observability; §12 telemetry).
    pub replayed: usize,
    /// Highest restored entity id (id-mint bump safety).
    pub max_entity_id: u64,
}

/// V1 journals only the default vhost (before the management API exists);
/// the indirection keeps adding vhosts later a one-line change.
fn vhost_of(topology: &Topology) -> VhostId {
    topology
        .find_vhost("/")
        .expect("fresh topology always has the default vhost")
}

/// Fold one record into live state. Idempotent (INV-11): restores are
/// identity-guarded, settlements/discards/purges are no-ops when absent —
/// shared by snapshot replay and journal-suffix replay.
fn apply_record(
    topology: &mut Topology,
    store: &mut MessageStore,
    max_entity_id: &mut u64,
    record: &Record,
) {
    let vhost = vhost_of(topology);
    match record {
        Record::QueueDeclare(q) => {
            *max_entity_id = (*max_entity_id).max(q.id);
            // Live ownership is session state; restored durable queues are
            // non-exclusive by construction (§9.3).
            topology.restore_queue(
                vhost,
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
            topology.remove_queue_by_id(vhost, QueueId::from_raw(*id));
            store.drain(QueueId::from_raw(*id));
        }
        Record::ExchangeDeclare(e) => {
            *max_entity_id = (*max_entity_id).max(e.id);
            let kind = match e.kind {
                0 => ExchangeType::Direct,
                1 => ExchangeType::Fanout,
                _ => ExchangeType::Topic,
            };
            topology.restore_exchange(
                vhost,
                &e.name,
                ExchangeId::from_raw(e.id),
                kind,
                e.durable,
                e.auto_delete,
                e.internal,
            );
        }
        Record::ExchangeDelete { id } => {
            topology.remove_exchange_by_id(vhost, ExchangeId::from_raw(*id));
        }
        Record::Bind(b) => {
            topology.restore_binding(
                vhost,
                ExchangeId::from_raw(b.exchange),
                QueueId::from_raw(b.queue),
                &b.routing_key,
            );
        }
        Record::Unbind(b) => {
            topology.remove_binding(
                vhost,
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
                    // Delivery-attempt markers (§9.6) restore the hint via
                    // the Delivered record; fresh restores start clean.
                    redelivered: false,
                };
                store.restore_with_seq(QueueId::from_raw(*queue), *seq, message);
            }
        }
        Record::SettleAck { queue, seq } | Record::SettleDiscard { queue, seq } => {
            store.discard(QueueId::from_raw(*queue), *seq);
        }
        Record::Delivered { queue, seq } => {
            store.mark_redelivered(QueueId::from_raw(*queue), *seq);
        }
        Record::Purge { queue, seqs } => {
            for seq in seqs {
                store.discard(QueueId::from_raw(*queue), *seq);
            }
        }
    }
}

/// Rebuild live state from the recovery root at `dir`:
/// manifest → snapshot + journal suffix, or the journal alone before the
/// first snapshot. The byte budget mirrors the runtime store budget.
pub fn rebuild(
    dir: &std::path::Path,
    byte_budget: usize,
) -> Result<Rebuilt, crate::record::FormatError> {
    let manifest = crate::snapshot::read_manifest(dir)?;
    let (snapshot_records, covered_lsn) = match &manifest {
        Some(m) => {
            let snap_dir = dir.join("snapshots").join(&m.snapshot);
            if !snap_dir.exists() {
                return Err(crate::record::FormatError::Corruption(format!(
                    "manifest references missing snapshot '{}'",
                    m.snapshot
                )));
            }
            let snap = crate::snapshot::read_snapshot(&snap_dir)?;
            if snap.generation != m.generation {
                return Err(crate::record::FormatError::Corruption(
                    "manifest/snapshot generation mismatch".into(),
                ));
            }
            (snap.records, m.covered_lsn)
        }
        None => (Vec::new(), 0),
    };
    // With a published manifest the chain head may have been reclaimed
    // under it (§9.9); without one the chain must be intact.
    let records = crate::journal::recover_with_options(dir, manifest.is_some())?;
    let mut topology = Topology::new(CompatibilitySwitches::default());
    let mut store = MessageStore::new(byte_budget);

    let mut replayed = 0usize;
    let mut max_entity_id = 0u64;
    // Snapshot first, then only the uncovered suffix — both idempotent.
    for record in &snapshot_records {
        replayed += 1;
        apply_record(&mut topology, &mut store, &mut max_entity_id, record);
    }
    for item in &records {
        if item.lsn <= covered_lsn {
            continue; // covered by the snapshot
        }
        replayed += 1;
        apply_record(&mut topology, &mut store, &mut max_entity_id, &item.record);
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

/// Open a persistent broker backend: recover first (rebuild live state),
/// then open the writer for appends. The recovery root is authoritative.
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
    use crate::snapshot::{publish_manifest, write_snapshot};
    use std::fs;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-rebuild-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn queue_declare(id: u64, name: &str) -> Record {
        Record::QueueDeclare(QueueRecord {
            name: name.into(),
            id,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })
    }

    fn enqueue(msg: u64, seq: u64, body: &[u8]) -> Record {
        Record::Enqueue(Enqueue {
            message_id: msg,
            property_bytes: vec![9],
            body: body.to_vec(),
            exchange: "".into(),
            routing_key: "jobs".into(),
            persistent: true,
            destinations: vec![(5, seq)],
        })
    }

    #[test]
    fn rebuild_restores_topology_messages_and_settlements() {
        let dir = tmp("full");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[queue_declare(5, "jobs")]).unwrap();
            w.commit(&[enqueue(1, 0, b"one"), enqueue(2, 1, b"two")])
                .unwrap();
            w.commit(&[Record::SettleAck { queue: 5, seq: 1 }]).unwrap();
            drop(w);
        }

        let mut rebuilt = rebuild(&dir, 1024 * 1024).unwrap();
        assert_eq!(rebuilt.replayed, 4, "one per journaled record");
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
            w.commit(&[queue_declare(3, "temp")]).unwrap();
            w.commit(&[enqueue(1, 0, b"x")]).unwrap();
            w.commit(&[Record::QueueDelete { id: 3 }]).unwrap();
            drop(w);
        }
        let rebuilt = rebuild(&dir, 1024 * 1024).unwrap();
        let vhost = rebuilt.topology.find_vhost("/").unwrap();
        assert!(rebuilt.topology.find_queue(vhost, "temp").is_none());
        assert_eq!(rebuilt.store.len(rusty_mq_core::QueueId::for_test(3)), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_plus_suffix_recovery() {
        let dir = tmp("snap-suffix");
        let covered_lsn;
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[queue_declare(5, "jobs")]).unwrap();
            w.commit(&[enqueue(1, 0, b"one"), enqueue(2, 1, b"two")])
                .unwrap();
            covered_lsn = w.commit(&[Record::SettleAck { queue: 5, seq: 0 }]).unwrap();
            // Snapshot the live state AS OF the covered LSN: queue + entry
            // seq 1 (seq 0 settled).
            let snap_dir = write_snapshot(
                &dir,
                1,
                covered_lsn,
                &[queue_declare(5, "jobs"), enqueue(2, 1, b"two")],
            )
            .unwrap();
            let _ = snap_dir;
            publish_manifest(
                &dir,
                &crate::snapshot::Manifest {
                    generation: 1,
                    covered_lsn,
                    snapshot: "snapshot-00000000000000000001".into(),
                },
            )
            .unwrap();
            // Suffix AFTER the snapshot: one more message.
            w.commit(&[enqueue(3, 2, b"three")]).unwrap();
            drop(w);
        }

        let mut rebuilt = rebuild(&dir, 1024 * 1024).unwrap();
        let vhost = rebuilt.topology.find_vhost("/").unwrap();
        let qid = rebuilt.topology.find_queue(vhost, "jobs").unwrap();
        // Snapshot state (seq 1) + suffix (seq 2); the settled seq 0 is
        // gone even though a pre-snapshot journal record mentions it.
        assert_eq!(rebuilt.store.len(qid), 2);
        let e1 = rebuilt.store.pop_ready(qid).unwrap();
        assert_eq!(e1.message.body, b"two".to_vec());
        let e2 = rebuilt.store.pop_ready(qid).unwrap();
        assert_eq!(e2.message.body, b"three".to_vec());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_pointing_at_missing_snapshot_fails() {
        let dir = tmp("missing-snap");
        fs::create_dir_all(&dir).unwrap();
        publish_manifest(
            &dir,
            &crate::snapshot::Manifest {
                generation: 1,
                covered_lsn: 5,
                snapshot: "snapshot-00000000000000000001".into(),
            },
        )
        .unwrap();
        assert!(rebuild(&dir, 1024 * 1024).is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
