//! The redb projection (§9.7): a derived, rebuildable index/checkpoint
//! accelerator over the authoritative journal.
//!
//! Contract (ADR-0002 / INV-12):
//! - `applied_lsn` advances ATOMICALLY with every index update, inside the
//!   same redb transaction.
//! - The projection never leads the journal: startup rejects a projection
//!   whose applied_lsn exceeds the journal's committed end and rebuilds
//!   from authority instead.
//! - A missing, corrupt, stale-schema, or ahead-of-journal projection is
//!   deleted and rebuilt from the journal/snapshot — never silently
//!   trusted, never fatally blocking startup.
//! - redb durability is set explicitly (Immediate), matching the
//!   commit-per-transaction posture (R16).

use std::path::Path;

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};

use rusty_mq_core::ids::{ExchangeId, QueueId};
use rusty_mq_core::routing::ExchangeType;
use rusty_mq_core::store::{MessageStore, StoredMessage};
use rusty_mq_core::topology::{QueueProfile, Topology};

use crate::record::Record;

const SCHEMA: u64 = 1;
const META: TableDefinition<(), (u64, u64, u64)> = TableDefinition::new("proj_meta");
const QUEUES: TableDefinition<u64, &[u8]> = TableDefinition::new("queues");
const EXCHANGES: TableDefinition<u64, &[u8]> = TableDefinition::new("exchanges");
/// (exchange id, queue id, routing key).
type BindingKey<'a> = (u64, u64, &'a str);
const BINDINGS: TableDefinition<BindingKey, ()> = TableDefinition::new("bindings");
/// (queue id, sequence).
type EntryKey = (u64, u64);
const ENTRIES: TableDefinition<EntryKey, &[u8]> = TableDefinition::new("entries");
const DELIVERED: TableDefinition<EntryKey, ()> = TableDefinition::new("delivered");

fn io_err(e: impl std::fmt::Display) -> crate::record::FormatError {
    crate::record::FormatError::Io(e.to_string())
}

/// An open projection handle.
pub struct Projection {
    db: Database,
}

/// Why the projection could not be used (and was rebuilt).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionStatus {
    /// Built fresh: no usable projection existed.
    Rebuilt { reason: String },
    /// Loaded and only journal records after `applied_lsn` replayed.
    Loaded { applied_lsn: u64 },
}

impl Projection {
    /// Open (creating if absent) the projection at `dir/index/state.redb`.
    /// All tables are created eagerly so reads never hit "does not exist".
    pub fn open(dir: &Path) -> Result<Self, crate::record::FormatError> {
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&index_dir).map_err(io_err)?;
        let db = Database::create(index_dir.join("state.redb"))
            .map_err(|e| crate::record::FormatError::Corruption(format!("index open: {e}")))?;
        let projection = Self { db };
        // Schema init runs only when the META table itself is missing.
        let needs_schema = matches!(
            projection.applied_lsn_or_err(),
            Err(e) if e.to_string().contains("does not exist")
        );
        if needs_schema {
            let mut tx = projection.db.begin_write().map_err(io_err)?;
            // Opening a table inside a write txn creates it.
            let _ = tx.open_table(QUEUES).map_err(io_err)?;
            let _ = tx.open_table(EXCHANGES).map_err(io_err)?;
            let _ = tx.open_table(BINDINGS).map_err(io_err)?;
            let _ = tx.open_table(ENTRIES).map_err(io_err)?;
            let _ = tx.open_table(DELIVERED).map_err(io_err)?;
            // META row zero (schema marker; applied_lsn 0 = empty).
            tx.open_table(META)
                .map_err(io_err)?
                .insert((), (SCHEMA, 0, 0))
                .map_err(io_err)?;
            tx.set_durability(Durability::Immediate).map_err(io_err)?;
            tx.commit().map_err(io_err)?;
        }
        Ok(projection)
    }

    fn applied_lsn_or_err(&self) -> Result<Option<u64>, crate::record::FormatError> {
        let tx = self.db.begin_read().map_err(io_err)?;
        let table = tx.open_table(META).map_err(io_err)?;
        Ok(table.get(()).map_err(io_err)?.map(|v| v.value().2))
    }

    /// Delete the projection files entirely (rebuild path).
    pub fn discard(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir.join("index"));
    }

    /// Apply records + advance applied_lsn atomically. `lsn` is the fence
    /// LSN of the committed transaction these records belong to.
    pub fn apply(&self, records: &[Record], lsn: u64) -> Result<(), crate::record::FormatError> {
        let mut tx = self.db.begin_write().map_err(io_err)?;
        // §9.7: explicit durability — the projection never claims progress
        // that a crash could erase beyond what redb's Immediate guarantees.
        tx.set_durability(Durability::Immediate).map_err(io_err)?;
        let result = Self::apply_in_tx(&mut tx, records, lsn);
        match result {
            Ok(()) => tx.commit().map_err(io_err),
            Err(e) => {
                // Aborting leaves applied_lsn untouched (atomicity).
                tx.abort().ok();
                Err(e)
            }
        }
    }

    fn apply_in_tx(
        tx: &mut WriteTransaction,
        records: &[Record],
        lsn: u64,
    ) -> Result<(), crate::record::FormatError> {
        for record in records {
            match record {
                Record::QueueDeclare(q) => {
                    let payload = Record::QueueDeclare(q.clone()).encode();
                    let mut table = tx.open_table(QUEUES).map_err(io_err)?;
                    table.insert(q.id, payload.as_slice()).map_err(io_err)?;
                }
                Record::QueueDelete { id } => {
                    let mut table = tx.open_table(QUEUES).map_err(io_err)?;
                    table.remove(id).map_err(io_err)?;
                    // Entries of the queue go with it: drain the
                    // queue's key range ((id, ..) sorts contiguously).
                    let mut entries = tx.open_table(ENTRIES).map_err(io_err)?;
                    let stale: Vec<EntryKey> = entries
                        .range((*id, u64::MIN)..=(*id, u64::MAX))
                        .map_err(io_err)?
                        .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                        .collect();
                    for k in stale {
                        entries.remove(k).map_err(io_err)?;
                        tx.open_table(DELIVERED)
                            .map_err(io_err)?
                            .remove(k)
                            .map_err(io_err)?;
                    }
                }
                Record::ExchangeDeclare(e) => {
                    let payload = Record::ExchangeDeclare(e.clone()).encode();
                    let mut table = tx.open_table(EXCHANGES).map_err(io_err)?;
                    table.insert(e.id, payload.as_slice()).map_err(io_err)?;
                }
                Record::ExchangeDelete { id } => {
                    let mut table = tx.open_table(EXCHANGES).map_err(io_err)?;
                    table.remove(id).map_err(io_err)?;
                }
                Record::Bind(b) => {
                    let mut table = tx.open_table(BINDINGS).map_err(io_err)?;
                    table
                        .insert((b.exchange, b.queue, b.routing_key.as_str()), ())
                        .map_err(io_err)?;
                }
                Record::Unbind(b) => {
                    let mut table = tx.open_table(BINDINGS).map_err(io_err)?;
                    table
                        .remove((b.exchange, b.queue, b.routing_key.as_str()))
                        .map_err(io_err)?;
                }
                Record::Enqueue(e) => {
                    let mut table = tx.open_table(ENTRIES).map_err(io_err)?;
                    // Store the Enqueue payload per destination; identity is
                    // (queue, seq), so multi-destination enqueues fan out.
                    let mut single = e.clone();
                    single.destinations.clear();
                    let payload = Record::Enqueue(single).encode();
                    for (queue, seq) in &e.destinations {
                        table
                            .insert((*queue, *seq), payload.as_slice())
                            .map_err(io_err)?;
                    }
                }
                Record::SettleAck { queue, seq } | Record::SettleDiscard { queue, seq } => {
                    let mut table = tx.open_table(ENTRIES).map_err(io_err)?;
                    table.remove((*queue, *seq)).map_err(io_err)?;
                    tx.open_table(DELIVERED)
                        .map_err(io_err)?
                        .remove((*queue, *seq))
                        .map_err(io_err)?;
                }
                Record::Delivered { queue, seq } => {
                    let mut table = tx.open_table(DELIVERED).map_err(io_err)?;
                    table.insert((*queue, *seq), ()).map_err(io_err)?;
                }
                Record::Purge { queue, seqs } => {
                    let mut table = tx.open_table(ENTRIES).map_err(io_err)?;
                    let mut delivered = tx.open_table(DELIVERED).map_err(io_err)?;
                    for seq in seqs {
                        table.remove((*queue, *seq)).map_err(io_err)?;
                        delivered.remove((*queue, *seq)).map_err(io_err)?;
                    }
                }
                // Auth records are outside the data projection's scope:
                // the authoritative auth state replays from the journal
                // (§9.1 — one source of truth); applied_lsn still advances.
                Record::PrincipalUpsert(_)
                | Record::PrincipalDelete { .. }
                | Record::PermissionSet(_)
                | Record::PermissionDelete { .. } => {}
            }
        }
        let mut meta = tx.open_table(META).map_err(io_err)?;
        meta.insert((), (SCHEMA, 0, lsn)).map_err(io_err)?;
        Ok(())
    }

    /// Current applied LSN (None when the projection is empty).
    pub fn applied_lsn(&self) -> Result<Option<u64>, crate::record::FormatError> {
        self.applied_lsn_or_err()
    }

    /// Load live state from the projection. Fails when the schema is wrong.
    fn load(&self) -> Result<(Topology, MessageStore, u64), crate::record::FormatError> {
        let read = self.db.begin_read().map_err(io_err)?;
        let mut topology = Topology::default_for_projection();
        let mut store = MessageStore::new(usize::MAX / 2);
        let applied;
        {
            let meta = read.open_table(META).map_err(io_err)?;
            if let Some(v) = meta.get(()).map_err(io_err)? {
                let (schema, _generation, lsn) = v.value();
                if schema != SCHEMA {
                    return Err(crate::record::FormatError::Corruption(format!(
                        "projection schema {schema} != {SCHEMA}"
                    )));
                }
                applied = lsn;
            } else {
                return Err(crate::record::FormatError::Corruption(
                    "projection has no meta row".into(),
                ));
            }
        }
        let vhost = topology
            .find_vhost("/")
            .expect("projection topology has the default vhost");
        for row in read
            .open_table(QUEUES)
            .map_err(io_err)?
            .iter()
            .map_err(io_err)?
        {
            let (id_g, blob_g) = row.map_err(io_err)?;
            let id = id_g.value();
            let blob: &[u8] = blob_g.value();
            let Record::QueueDeclare(q) = Record::decode(crate::record::kind::QUEUE_DECLARE, blob)?
            else {
                unreachable!("queues table stores QueueDeclare payloads");
            };
            topology.restore_queue(
                vhost,
                &q.name,
                QueueId::from_raw(id),
                QueueProfile {
                    durable: q.durable,
                    exclusive: false,
                    auto_delete: false,
                },
            );
        }
        for row in read
            .open_table(EXCHANGES)
            .map_err(io_err)?
            .iter()
            .map_err(io_err)?
        {
            let (id_g, blob_g) = row.map_err(io_err)?;
            let id = id_g.value();
            let blob: &[u8] = blob_g.value();
            let Record::ExchangeDeclare(e) =
                Record::decode(crate::record::kind::EXCHANGE_DECLARE, blob)?
            else {
                unreachable!();
            };
            let kind = match e.kind {
                0 => ExchangeType::Direct,
                1 => ExchangeType::Fanout,
                _ => ExchangeType::Topic,
            };
            topology.restore_exchange(
                vhost,
                &e.name,
                ExchangeId::from_raw(id),
                kind,
                e.durable,
                e.auto_delete,
                e.internal,
            );
        }
        for row in read
            .open_table(BINDINGS)
            .map_err(io_err)?
            .iter()
            .map_err(io_err)?
        {
            let (key_g, _) = row.map_err(io_err)?;
            let key = key_g.value();
            topology.restore_binding(
                vhost,
                ExchangeId::from_raw(key.0),
                QueueId::from_raw(key.1),
                key.2,
            );
        }
        let delivered = read.open_table(DELIVERED).map_err(io_err)?;
        for row in read
            .open_table(ENTRIES)
            .map_err(io_err)?
            .iter()
            .map_err(io_err)?
        {
            let (key_g, blob_g) = row.map_err(io_err)?;
            let key = key_g.value();
            let blob: &[u8] = blob_g.value();
            let Record::Enqueue(e) = Record::decode(crate::record::kind::ENQUEUE, blob)? else {
                unreachable!();
            };
            let redelivered = delivered.get((key.0, key.1)).map_err(io_err)?.is_some();
            let message = StoredMessage {
                property_bytes: e.property_bytes,
                body: e.body,
                exchange: e.exchange,
                routing_key: e.routing_key,
                persistent: e.persistent,
                redelivered,
            };
            store.restore_with_seq(QueueId::from_raw(key.0), key.1, message);
        }
        Ok((topology, store, applied))
    }
}

/// Recover with the projection as a checkpoint: load it when valid and
/// behind the journal, replay only the suffix; otherwise rebuild it from
/// the authoritative recovery root (§9.7 rules 3-4).
pub fn recover_with_projection(
    dir: &Path,
    byte_budget: usize,
) -> Result<(Topology, MessageStore, Projection, ProjectionStatus), crate::record::FormatError> {
    // Authoritative replay (manifest-aware) computes the journal truth.
    let journal_rebuilt = crate::rebuild::rebuild(dir, byte_budget)?;
    let last_lsn = crate::journal::last_committed_lsn(dir)?;

    let try_projection = Projection::open(dir).and_then(|p| {
        let applied = p.applied_lsn()?;
        Ok((p, applied))
    });
    // A projection file that cannot even be opened is corruption: discard
    // and rebuild from the authority (§9.7 rule 4).

    match try_projection {
        // applied == 0 means an empty projection: nothing loadable, use
        // the rebuild path (which also applies all records to the index).
        Ok((projection, Some(applied))) if applied > 0 && applied <= last_lsn => {
            match projection.load() {
                Ok((topology, store, loaded_lsn)) if loaded_lsn == applied => {
                    // Suffix: journal records after the projection point
                    // (up to last committed) applied idempotently.
                    let suffix: Vec<Record> = crate::journal::recover(dir)?
                        .into_iter()
                        .filter(|r| r.lsn > applied)
                        .map(|r| r.record)
                        .collect();
                    let mut topology = topology;
                    let mut store = store;
                    let mut max_id = 0u64;
                    for record in &suffix {
                        crate::rebuild::apply_record_pub(
                            &mut topology,
                            &mut store,
                            &mut max_id,
                            record,
                        );
                    }
                    rusty_mq_core::ids::bump_past(max_id);
                    // Bring the projection itself up to date (it may trail
                    // the journal when it was behind).
                    if !suffix.is_empty() {
                        projection.apply(&suffix, last_lsn)?;
                    }
                    Ok((
                        topology,
                        store,
                        projection,
                        ProjectionStatus::Loaded {
                            applied_lsn: applied,
                        },
                    ))
                }
                _ => rebuild_projection(dir, byte_budget, journal_rebuilt),
            }
        }
        // Empty, corrupt, schema-mismatched, or ahead of the journal
        // (INV-12: never trust a projection leading the authority).
        _ => rebuild_projection(dir, byte_budget, journal_rebuilt),
    }
}

fn rebuild_projection(
    dir: &Path,
    _byte_budget: usize,
    journal_rebuilt: crate::rebuild::Rebuilt,
) -> Result<(Topology, MessageStore, Projection, ProjectionStatus), crate::record::FormatError> {
    Projection::discard(dir);
    let projection = Projection::open(dir)?;
    // Rebuild from the authoritative fold's records: replay ALL journal
    // records through the projection (snapshot state is implied by the
    // authoritative rebuild result; the journal remains the chain of
    // record events the projection indexes).
    let records: Vec<Record> = crate::journal::recover(dir)?
        .into_iter()
        .map(|r| r.record)
        .collect();
    let last_lsn = crate::journal::last_committed_lsn(dir)?;
    projection.apply(&records, last_lsn)?;
    Ok((
        journal_rebuilt.topology,
        journal_rebuilt.store,
        projection,
        ProjectionStatus::Rebuilt {
            reason: "projection missing/corrupt/stale/ahead".into(),
        },
    ))
}

// JournalWriter is not used directly here; keep the import meaningful for
// the projection's contract documentation (applied_lsn ≤ durable LSN).
#[allow(unused_imports)]
use crate::journal::JournalWriter as _;

// ---------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{JournalConfig, JournalWriter};
    use crate::record::{Enqueue, QueueRecord};
    use std::fs;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-proj-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn q(id: u64, name: &str) -> Record {
        Record::QueueDeclare(QueueRecord {
            name: name.into(),
            id,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })
    }

    fn msg(id: u64, seq: u64) -> Record {
        Record::Enqueue(Enqueue {
            message_id: id,
            property_bytes: vec![],
            body: format!("m{id}").into_bytes(),
            exchange: "".into(),
            routing_key: "jobs".into(),
            persistent: true,
            destinations: vec![(7, seq)],
        })
    }

    #[test]
    fn fresh_startup_builds_projection_from_journal() {
        let dir = tmp("fresh");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[q(7, "jobs")]).unwrap();
            w.commit(&[msg(1, 0)]).unwrap();
        }
        let (topology, store, _proj, status) = recover_with_projection(&dir, 1 << 20).unwrap();
        assert!(matches!(status, ProjectionStatus::Rebuilt { .. }));
        let vhost = topology.find_vhost("/").unwrap();
        let qid = topology.find_queue(vhost, "jobs").unwrap();
        assert_eq!(store.len(qid), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn second_startup_loads_from_projection() {
        let dir = tmp("load");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[q(7, "jobs")]).unwrap();
            w.commit(&[msg(1, 0), msg(2, 1)]).unwrap();
        }
        // First open builds; a second open (no new journal writes) loads.
        let (_, _, _, s1) = recover_with_projection(&dir, 1 << 20).unwrap();
        assert!(matches!(s1, ProjectionStatus::Rebuilt { .. }));
        let (topology, store, _, s2) = recover_with_projection(&dir, 1 << 20).unwrap();
        assert!(
            matches!(s2, ProjectionStatus::Loaded { .. }),
            "second startup loads the projection"
        );
        let vhost = topology.find_vhost("/").unwrap();
        let qid = topology.find_queue(vhost, "jobs").unwrap();
        assert_eq!(store.len(qid), 2, "loaded state equals the built state");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn trailing_journal_replays_over_loaded_projection() {
        let dir = tmp("suffix");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[q(7, "jobs")]).unwrap();
        }
        let (_, _, proj, _) = recover_with_projection(&dir, 1 << 20).unwrap();
        drop(proj);
        // Journal gains a suffix the projection has not seen.
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[msg(9, 0)]).unwrap();
        }
        let (topology, store, _, status) = recover_with_projection(&dir, 1 << 20).unwrap();
        assert!(matches!(status, ProjectionStatus::Loaded { .. }));
        let vhost = topology.find_vhost("/").unwrap();
        let qid = topology.find_queue(vhost, "jobs").unwrap();
        assert_eq!(store.len(qid), 1, "suffix entry recovered over the load");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_index_is_rebuilt_not_trusted() {
        let dir = tmp("corrupt");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[q(7, "jobs")]).unwrap();
            w.commit(&[msg(1, 0)]).unwrap();
        }
        let _ = recover_with_projection(&dir, 1 << 20).unwrap(); // build
        let db_path = dir.join("index").join("state.redb");
        let data = fs::read(&db_path).unwrap();
        // Truncate to a fraction: guaranteed structural corruption that
        // redb detects at open (mid-file edits can land in unchecked
        // regions and go unnoticed by lazy page reads).
        assert!(data.len() > 4096);
        fs::write(&db_path, &data[..data.len() / 2]).unwrap();

        let (topology, store, _, status) = recover_with_projection(&dir, 1 << 20).unwrap();
        assert!(matches!(status, ProjectionStatus::Rebuilt { .. }));
        let vhost = topology.find_vhost("/").unwrap();
        let qid = topology.find_queue(vhost, "jobs").unwrap();
        assert_eq!(store.len(qid), 1, "state rebuilt from the authority");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn settlement_reflected_in_projection_state() {
        let dir = tmp("settle");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[q(7, "jobs")]).unwrap();
            w.commit(&[msg(1, 0), msg(2, 1)]).unwrap();
            w.commit(&[Record::SettleAck { queue: 7, seq: 0 }]).unwrap();
        }
        let (topology, store, _, _) = recover_with_projection(&dir, 1 << 20).unwrap();
        let (topology2, store2, _, s2) = recover_with_projection(&dir, 1 << 20).unwrap();
        assert!(matches!(s2, ProjectionStatus::Loaded { .. }));
        let vhost = topology2.find_vhost("/").unwrap();
        let qid = topology2.find_queue(vhost, "jobs").unwrap();
        assert_eq!(store2.len(qid), 1, "settled entry absent from the load");
        let _ = topology;
        let _ = store;
        let _ = fs::remove_dir_all(&dir);
    }
}
