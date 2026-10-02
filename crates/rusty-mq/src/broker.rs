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

/// Aggregate in-memory message budget for the development broker
/// (bounded by construction, INV-09; configurable in M7's config surface).
const MESSAGE_BYTE_BUDGET: usize = 64 * 1024 * 1024;

/// Permitted credentials for the development alpha.
#[derive(Clone)]
pub struct TestUser {
    pub username: String,
    pub password: String,
}

/// The broker singleton shared by all connections.
pub struct Broker {
    pub topology: Mutex<Topology>,
    /// In-memory message store (M2); the durable journal augments this in M4.
    pub store: Mutex<MessageStore>,
    /// Durable journal (None = memory-backed development mode: accepted
    /// durable declarations make no persistence claim).
    pub journal: Mutex<Option<JournalWriter>>,
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
            topology: Mutex::new(Topology::new(CompatibilitySwitches::default())),
            store: Mutex::new(MessageStore::new(MESSAGE_BYTE_BUDGET)),
            journal: Mutex::new(None),
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
        let (rebuilt, writer) = rusty_mq_storage::rebuild::open_persistent(
            data_dir,
            MESSAGE_BYTE_BUDGET,
            JournalConfig::default(),
        )
        .expect("recovery must succeed or startup must fail explicitly");
        tracing::info!(
            replayed = rebuilt.replayed,
            data_dir = %data_dir.display(),
            "recovered durable state from journal"
        );
        Self {
            topology: Mutex::new(rebuilt.topology),
            store: Mutex::new(rebuilt.store),
            journal: Mutex::new(Some(writer)),
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
        let mut journal = self.journal.lock().unwrap();
        match journal.as_mut() {
            Some(writer) => writer.commit(records).map(|_| ()).map_err(|e| {
                tracing::error!(error = %e, "journal commit failed");
                JournalCommitError
            }),
            None => Ok(()),
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
