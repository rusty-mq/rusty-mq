//! Shared broker state: topology, M1 test users, connection registry.
//!
//! M1 authentication is a development-credentials map (clearly logged);
//! salted password hashes and permission checks arrive in M7 (FR-S01/S03).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::consumers::{Consumers, Job};
use rusty_mq_core::store::MessageStore;
use rusty_mq_core::topology::{CompatibilitySwitches, Topology};
use rusty_mq_core::ConnectionId;
use rusty_mq_core::QueueId;
use rusty_mq_storage::record::Record;
use rusty_mq_storage::{journal::JournalConfig, JournalWriter};

/// A journal commit failed; callers must surface it (never fabricate
/// success, §6.4). Details are logged at the failure site.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("journal commit failed")]
pub struct JournalCommitError;

/// The durable live state expressed as journal records (snapshot payload,
/// §9.9 step 1): durable topology, bindings between durable endpoints,
/// and every ready entry with its exact identity. Unacked entries are NOT
/// in the ready set (held out) — they are captured via their journaled
/// Enqueue plus Delivered/Settle records replaying after the snapshot.
fn snapshot_records(
    topo: &Topology,
    store: &rusty_mq_core::store::MessageStore,
) -> Vec<rusty_mq_storage::Record> {
    use rusty_mq_core::ids::{ExchangeId, QueueId};
    use rusty_mq_core::routing::ExchangeType;
    use rusty_mq_storage::record::{Binding, Enqueue, ExchangeRecord, QueueRecord};

    let mut records = Vec::new();
    let mut durable_queues: Vec<(QueueId, &rusty_mq_core::topology::QueueRecord)> = Vec::new();
    for (id, rec) in topo.iter_queues() {
        if rec.profile.durable {
            durable_queues.push((id, rec));
        }
    }
    let durable_exchanges: Vec<(ExchangeId, &rusty_mq_core::topology::ExchangeRecord)> = topo
        .iter_exchanges()
        .filter(|(_, rec)| rec.durable)
        .collect();

    for (id, rec) in &durable_queues {
        records.push(rusty_mq_storage::Record::QueueDeclare(QueueRecord {
            name: rec.name.clone(),
            id: id.to_raw(),
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        }));
    }
    for (id, rec) in &durable_exchanges {
        // Skip built-ins (the default exchange and amq.* always exist).
        if rec.name.starts_with("amq.") || rec.name.is_empty() {
            continue;
        }
        records.push(rusty_mq_storage::Record::ExchangeDeclare(ExchangeRecord {
            name: rec.name.clone(),
            id: id.to_raw(),
            kind: match rec.kind {
                ExchangeType::Direct => 0,
                ExchangeType::Fanout => 1,
                ExchangeType::Topic => 2,
            },
            durable: rec.durable,
            auto_delete: rec.auto_delete,
            internal: rec.internal,
        }));
    }
    for (exchange, queue, key) in topo.iter_bindings() {
        let ex_durable = durable_exchanges.iter().any(|(id, _)| *id == exchange);
        let q_durable = durable_queues.iter().any(|(id, _)| *id == queue);
        if ex_durable && q_durable {
            records.push(rusty_mq_storage::Record::Bind(Binding {
                exchange: exchange.to_raw(),
                queue: queue.to_raw(),
                routing_key: key.to_string(),
            }));
        }
    }
    // Ready entries: one Enqueue record per entry, single destination.
    for (queue, rec) in &durable_queues {
        for entry in store.ready_entries(*queue) {
            records.push(rusty_mq_storage::Record::Enqueue(Enqueue {
                message_id: 0,
                property_bytes: entry.message.property_bytes.clone(),
                body: entry.message.body.clone(),
                exchange: entry.message.exchange.clone(),
                routing_key: entry.message.routing_key.clone(),
                persistent: true,
                destinations: vec![(queue.to_raw(), entry.seq)],
            }));
            if entry.message.redelivered {
                records.push(rusty_mq_storage::Record::Delivered {
                    queue: queue.to_raw(),
                    seq: entry.seq,
                });
            }
            let _ = rec;
        }
    }
    records
}

/// Aggregate in-memory message budget for the development broker
/// (bounded by construction, INV-09; configurable in M7's config surface).
const MESSAGE_BYTE_BUDGET: usize = 64 * 1024 * 1024;
/// Auto-compaction ceiling: when journal bytes exceed this, snapshot +
/// manifest + reclaim run inline after a commit (§9.9: V1 must reclaim
/// during ordinary operation).
const COMPACT_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;

/// Permitted credentials for the development alpha.
#[derive(Clone)]
pub struct TestUser {
    pub username: String,
    pub password: String,
}

/// The broker singleton shared by all connections.
pub struct Broker {
    /// Data directory when persistent (compaction target).
    data_dir: Option<std::path::PathBuf>,
    /// Snapshot generation sequence.
    snapshot_generation: AtomicU64,
    /// Journal-size ceiling before inline compaction (test hook).
    compact_threshold: std::sync::atomic::AtomicU64,
    pub topology: Mutex<Topology>,
    /// In-memory message store (M2); the durable journal augments this in M4.
    pub store: Mutex<MessageStore>,
    /// Durable journal (None = memory-backed development mode: accepted
    /// durable declarations make no persistence claim).
    pub journal: Mutex<Option<JournalWriter>>,
    /// Derived redb projection (None in memory mode). Rebuildable;
    /// advanced after journal commits with the fence LSN (§9.7).
    pub projection: Mutex<Option<rusty_mq_storage::projection::Projection>>,
    /// Last fence LSN committed to the journal (projection target).
    last_fence_lsn: AtomicU64,
    /// Consumer registry (M3).
    pub consumers: Mutex<Consumers>,
    /// M1: exactly one test user; M7 replaces this with durable principals.
    pub test_user: TestUser,
    connection_seq: AtomicU64,
    /// Server-generated consumer-tag sequence.
    consumer_tag_seq: AtomicU64,
}

impl Broker {
    pub fn new(user: String, password: String) -> Self {
        Self {
            data_dir: None,
            snapshot_generation: AtomicU64::new(0),
            compact_threshold: std::sync::atomic::AtomicU64::new(COMPACT_THRESHOLD_BYTES),
            topology: Mutex::new(Topology::new(CompatibilitySwitches::default())),
            store: Mutex::new(MessageStore::new(MESSAGE_BYTE_BUDGET)),
            journal: Mutex::new(None),
            projection: Mutex::new(None),
            last_fence_lsn: AtomicU64::new(0),
            consumers: Mutex::new(Consumers::new()),
            test_user: TestUser {
                username: user,
                password,
            },
            connection_seq: AtomicU64::new(1),
            consumer_tag_seq: AtomicU64::new(1),
        }
    }

    pub fn next_connection_id(&self) -> ConnectionId {
        ConnectionId::new()
    }

    pub fn connections_opened(&self) -> u64 {
        self.connection_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Open a persistent broker on a data directory: recover the journal
    /// into live state first (the journal is the only source of truth,
    /// ADR-0002), then open the writer for appends.
    pub fn open_persistent(user: String, password: String, data_dir: &std::path::Path) -> Self {
        let (topology, store, projection, writer) =
            rusty_mq_storage::rebuild::open_persistent_with_projection(
                data_dir,
                MESSAGE_BYTE_BUDGET,
                JournalConfig::default(),
            )
            .expect("recovery must succeed or startup must fail explicitly");
        tracing::info!(
            data_dir = %data_dir.display(),
            "recovered durable state (journal + projection)"
        );
        Self {
            data_dir: Some(data_dir.to_path_buf()),
            snapshot_generation: AtomicU64::new(0),
            compact_threshold: std::sync::atomic::AtomicU64::new(COMPACT_THRESHOLD_BYTES),
            topology: Mutex::new(topology),
            store: Mutex::new(store),
            journal: Mutex::new(Some(writer)),
            projection: Mutex::new(Some(projection)),
            last_fence_lsn: AtomicU64::new(0),
            consumers: Mutex::new(Consumers::new()),
            test_user: TestUser {
                username: user,
                password,
            },
            connection_seq: AtomicU64::new(1),
            consumer_tag_seq: AtomicU64::new(1),
        }
    }

    /// Commit records through the journal writer (the durable boundary,
    /// ADR-0001). Memory-backed mode accepts and continues (it never had a
    /// persistence claim); a persistent-mode failure is an error the caller
    /// must surface — never a fabricated success (§6.4).
    pub fn journal_commit(
        &self,
        records: &[Record],
    ) -> std::result::Result<(), JournalCommitError> {
        let fence = {
            let mut journal = self.journal.lock().unwrap();
            match journal.as_mut() {
                Some(writer) => writer.commit(records).map_err(|e| {
                    tracing::error!(error = %e, "journal commit failed");
                    JournalCommitError
                }),
                None => return Ok(()),
            }
        }?;
        // Advance the derived projection with the fence LSN atomically
        // with its index updates (§9.7). A projection failure NEVER fails
        // the committed transaction: the journal is authoritative and a
        // broken projection rebuilds at the next startup.
        {
            let mut guard = self.projection.lock().unwrap();
            if let Some(projection) = guard.as_ref() {
                if let Err(e) = projection.apply(records, fence) {
                    tracing::warn!(error = %e, "projection apply failed; will rebuild");
                    *guard = None;
                }
            }
        }
        self.last_fence_lsn.store(fence, Ordering::SeqCst);
        self.maybe_compact();
        Ok(())
    }

    /// Test hook: lower the auto-compaction ceiling.
    #[doc(hidden)]
    pub fn set_compact_threshold(&self, bytes: u64) {
        self.compact_threshold.store(bytes, Ordering::SeqCst);
    }

    /// Snapshot current durable state, publish the manifest, then reclaim
    /// covered segments (§9.9). The covered LSN is read AFTER the state
    /// capture, so it is ≥ every event the snapshot reflects; suffix
    /// records re-apply idempotently on recovery (INV-11).
    pub fn compact(&self) -> Result<(), String> {
        let Some(dir) = self.data_dir.clone() else {
            return Ok(()); // memory mode: nothing to compact
        };
        // 1. Capture a consistent view + the records describing it, then
        //    read the durable LSN (≥ every captured event). try_lock only:
        //    callers may hold state locks across journal_commit (the
        //    publish path holds the store lock through the commit), and
        //    std Mutexes are not reentrant — a contended capture defers to
        //    the next commit rather than deadlocking.
        let (records, covered_lsn, keep_segment) = {
            let Ok(topo) = self.topology.try_lock() else {
                return Ok(()); // busy: retry on a later commit
            };
            let Ok(store) = self.store.try_lock() else {
                return Ok(());
            };
            let records = snapshot_records(&topo, &store);
            let (covered_lsn, keep_segment) = {
                let Ok(journal) = self.journal.try_lock() else {
                    return Ok(());
                };
                let w = journal.as_ref().expect("compact requires the journal");
                (w.durable_lsn(), w.current_segment_id())
            };
            (records, covered_lsn, keep_segment)
        };
        // 2. Write the immutable snapshot, then atomically publish the
        //    recovery root (§9.9 steps 2-4).
        let generation = self.snapshot_generation.fetch_add(1, Ordering::SeqCst) + 1;
        rusty_mq_storage::snapshot::write_snapshot(&dir, generation, covered_lsn, &records)
            .map_err(|e| e.to_string())?;
        rusty_mq_storage::snapshot::publish_manifest(
            &dir,
            &rusty_mq_storage::snapshot::Manifest {
                generation,
                covered_lsn,
                snapshot: format!("snapshot-{generation:020}"),
            },
        )
        .map_err(|e| e.to_string())?;
        // 3. Only now may covered segments and superseded snapshots go
        //    (§9.9 step 5).
        rusty_mq_storage::snapshot::reclaim(&dir, covered_lsn, keep_segment, generation)
            .map_err(|e| e.to_string())?;
        tracing::info!(generation, covered_lsn, "compaction complete");
        Ok(())
    }

    /// Auto-compaction check after a successful commit. Failures are
    /// logged, never fatal to the committed transaction (the journal
    /// remains the authority either way).
    fn maybe_compact(&self) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let threshold = self.compact_threshold.load(Ordering::Relaxed);
        if rusty_mq_storage::snapshot::journal_bytes(dir) <= threshold {
            return;
        }
        if let Err(e) = self.compact() {
            tracing::warn!(error = %e, "auto-compaction failed; journal remains authoritative");
        }
    }

    /// Install a journal failpoint (test-only; T13/T14). No-op in memory
    /// mode.
    #[doc(hidden)]
    pub fn set_journal_failpoint(
        &self,
        fp: Option<std::sync::Arc<rusty_mq_storage::journal::Failpoint>>,
    ) {
        if let Some(writer) = self.journal.lock().unwrap().as_mut() {
            writer.set_failpoint(fp);
        }
    }

    /// Whether the journal is active (persistence claims are possible).
    pub fn is_persistent(&self) -> bool {
        self.journal.lock().unwrap().is_some()
    }

    /// Server-generated consumer tag (RabbitMQ-style amq.ctag-...).
    pub fn next_consumer_tag(&self) -> String {
        let n = self.consumer_tag_seq.fetch_add(1, Ordering::Relaxed);
        format!("amq.ctag-rusty-{n}")
    }

    /// Schedule ready entries of `queue` to eligible consumers.
    /// Lock order: consumers → store.
    pub fn dispatch_queue(&self, queue: QueueId) {
        let mut consumers = self.consumers.lock().unwrap();
        let mut store = self.store.lock().unwrap();
        consumers.dispatch(queue, &mut *store);
    }

    /// Auto-delete pass for queues that lost their last consumer
    /// (FR-Q05: only queues that have ever had a consumer). Deletion is
    /// silent.
    /// Lock order: consumers → topology → store.
    pub fn maybe_auto_delete_queues(&self, queues: &[QueueId]) {
        for queue in queues {
            let vhost_to_delete = {
                let consumers = self.consumers.lock().unwrap();
                if consumers.consumer_count(*queue) > 0 {
                    continue;
                }
                let topo = self.topology.lock().unwrap();
                topo.queue_record(*queue)
                    .and_then(|r| (r.profile.auto_delete && r.has_had_consumer).then_some(r.vhost))
            };
            if let Some(vhost) = vhost_to_delete {
                self.topology
                    .lock()
                    .unwrap()
                    .remove_queue_by_id(vhost, *queue);
                self.store.lock().unwrap().drain(*queue);
            }
        }
    }

    /// Delete a queue from the consumer side: cancel-notify capable
    /// consumers first, then deregister everything on it.
    /// Lock order: consumers (only).
    pub fn cancel_and_deregister_queue(
        &self,
        queue: QueueId,
    ) -> Vec<(tokio::sync::mpsc::Sender<Job>, Job)> {
        let mut consumers = self.consumers.lock().unwrap();
        let jobs = consumers.cancel_notify_jobs(queue);
        consumers.deregister_queue(queue);
        jobs
    }
}
