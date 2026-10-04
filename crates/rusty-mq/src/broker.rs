//! Shared broker state: topology, M1 test users, connection registry.
//!
//! M1 authentication is a development-credentials map (clearly logged);
//! salted password hashes and permission checks arrive in M7 (FR-S01/S03).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::alarms::Alarms;
use crate::consumers::{Consumers, Job};
use crate::metrics::Metrics;
use rusty_mq_core::auth::{AuthState, Permissions, Principal, Role};
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

/// Control messages pushed to live connections (alarm notifications).
#[derive(Clone, Debug)]
pub enum Control {
    Blocked(String),
    Unblocked,
    /// Server-initiated connection.close (operator action or revocation,
    /// FR-S08); the connection performs the close handshake then ends.
    Close {
        reply_code: u16,
        reason: String,
    },
}

/// A registered live connection (alarm fan-out target).
pub struct LiveConnection {
    pub id: ConnectionId,
    pub username: String,
    /// Bounded control channel; a full channel drops the notification
    /// (bounded by construction — the socket writer is the real backstop).
    pub control: tokio::sync::mpsc::Sender<Control>,
}

/// Argon2id hash of a fresh password (PHC string with a random salt).
pub fn argon2_hash(password: &str) -> Result<String, String> {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    use argon2::Argon2;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

/// Verify a password against a PHC string.
fn argon2_verify(phc: &str, password: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    use argon2::Argon2;
    PasswordHash::new(phc)
        .map(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok()
        })
        .unwrap_or(false)
}

/// A precomputed PHC string for unknown-user dummy verifies (bounded cost,
/// anti-enumeration timing; FR-S05). The password is unguessable and the
/// hash is public-by-construction.
const DUMMY_PHC: &str = "$argon2id$v=19$m=19456,t=2,p=1$cnVzdHktbXEtZHVtbXk$5vYjHSJlzlPmIGPQeVMAmNFaDXpoHkUqyEqHkwYF2/Q";

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

/// §10 admission caps (ADR-0004): count budgets enforced at the
/// mutation boundary — exhaustion is a channel-scoped 506, never silent
/// acceptance.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionCaps {
    pub max_connections: u32,
    pub max_queues_per_vhost: u32,
    pub max_bindings_per_vhost: u32,
    pub max_destinations_per_publish: u32,
    pub max_pending_confirms_per_channel: u32,
}

impl Default for AdmissionCaps {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            max_queues_per_vhost: 10_000,
            max_bindings_per_vhost: 100_000,
            max_destinations_per_publish: 1024,
            max_pending_confirms_per_channel: 10_000,
        }
    }
}
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
    pub(crate) data_dir: Option<std::path::PathBuf>,
    /// Snapshot generation sequence.
    snapshot_generation: AtomicU64,
    /// Journal-size ceiling before inline compaction (test hook).
    compact_threshold: std::sync::atomic::AtomicU64,
    /// Per-connection protocol limits (config-driven; §13.2).
    pub protocol_limits: rusty_mq_protocol::ProtocolLimits,
    /// Memory-alarm trigger bytes (limits.memory_alarm_bytes).
    pub memory_alarm_bytes: u64,
    /// Message-store byte budget (limits.managed_buffer_bytes).
    pub message_budget: usize,
    /// §10 count budgets (limits.*).
    pub admission: AdmissionCaps,
    /// §12.3: opt-in per-queue metric labels (bounded by the queue cap).
    pub queue_labels_enabled: bool,
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
    pub(crate) last_fence_lsn: AtomicU64,
    /// Consumer registry (M3).
    pub consumers: Mutex<Consumers>,
    /// Principals + permissions (M7); journaled in persistent mode.
    pub auth: Mutex<AuthState>,
    /// Process metrics (§12.2): bounded-cardinality counters.
    pub metrics: Metrics,
    /// Auth-failure throttle (FR-S05).
    pub auth_throttle: crate::throttle::AuthThrottle,
    /// Resource alarms (§10) + connection registry for notifications.
    pub alarms: Mutex<Alarms>,
    pub live_connections: Mutex<Vec<LiveConnection>>,
    /// M1: exactly one test user; M7 replaces this with durable principals.
    pub test_user: TestUser,
    connection_seq: AtomicU64,
    /// Server-generated consumer-tag sequence.
    consumer_tag_seq: AtomicU64,
}

impl Broker {
    pub fn new(user: String, password: String) -> Self {
        let mut auth = AuthState::new();
        auth.upsert_principal(Principal {
            username: user.clone(),
            password_phc: format!("dev-plaintext:{}", password.clone()),
            role: Role::Admin,
        });
        auth.set_permissions(
            &user,
            "/",
            Permissions {
                configure: ".*".into(),
                write: ".*".into(),
                read: ".*".into(),
            },
        );
        Self {
            auth: Mutex::new(auth),
            data_dir: None,
            snapshot_generation: AtomicU64::new(0),
            compact_threshold: std::sync::atomic::AtomicU64::new(COMPACT_THRESHOLD_BYTES),
            protocol_limits: rusty_mq_protocol::ProtocolLimits::default(),
            memory_alarm_bytes: MESSAGE_BYTE_BUDGET as u64,
            message_budget: MESSAGE_BYTE_BUDGET,
            admission: AdmissionCaps::default(),
            queue_labels_enabled: false,
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
            metrics: Metrics::default(),
            auth_throttle: crate::throttle::AuthThrottle::new(
                std::time::Duration::from_secs(10),
                10,
                100,
            ),
            alarms: Mutex::new(Alarms::default()),
            live_connections: Mutex::new(Vec::new()),
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

    /// Verify credentials for SASL PLAIN. Dev mode accepts the flagged
    /// plaintext pair; persistent mode verifies Argon2id PHC strings.
    /// Bounded-cost verify (FR-S05): Argon2id parameters are fixed by the
    /// PHC string stored at creation; unknown users still pay a dummy
    /// verify to avoid a user-enumeration timing signal.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        let auth = self.auth.lock().unwrap();
        match auth.principal(username) {
            Some(principal) => {
                if let Some(phc) = principal.password_phc.strip_prefix("dev-plaintext:") {
                    return phc == password;
                }
                argon2_verify(&principal.password_phc, password)
            }
            None => {
                // Dummy verify: constant-ish cost whether or not the user
                // exists.
                argon2_verify(DUMMY_PHC, password)
            }
        }
    }

    /// Journaled principal upsert; memory mode mutates only.
    pub fn upsert_principal(
        &self,
        principal: Principal,
        password_for_hash: Option<&str>,
    ) -> Result<(), String> {
        // Hash when a raw password is supplied (CLI admin path).
        let principal = match password_for_hash {
            Some(pw) => {
                let phc = argon2_hash(pw).map_err(|e| e.to_string())?;
                Principal {
                    password_phc: phc,
                    ..principal
                }
            }
            None => principal,
        };
        let record = rusty_mq_storage::Record::PrincipalUpsert(rusty_mq_storage::PrincipalRecord {
            username: principal.username.clone(),
            password_phc: principal.password_phc.clone(),
            role: principal.role.as_u8(),
        });
        self.journal_commit(&[record]).map_err(|e| e.to_string())?;
        self.auth.lock().unwrap().upsert_principal(principal);
        Ok(())
    }

    /// Journaled permission set (also used at bootstrap).
    pub fn set_permissions(
        &self,
        username: &str,
        vhost: &str,
        perms: Permissions,
    ) -> Result<(), String> {
        let record = rusty_mq_storage::Record::PermissionSet(rusty_mq_storage::PermissionRecord {
            username: username.to_string(),
            vhost: vhost.to_string(),
            configure: perms.configure.clone(),
            write: perms.write.clone(),
            read: perms.read.clone(),
        });
        self.journal_commit(&[record]).map_err(|e| e.to_string())?;
        self.auth
            .lock()
            .unwrap()
            .set_permissions(username, vhost, perms);
        Ok(())
    }

    /// Journaled principal deletion (revocation; live connections are
    /// closed by the caller when the HTTP API lands — FR-S08's full path).
    pub fn delete_principal(&self, username: &str) -> Result<bool, String> {
        let record = rusty_mq_storage::Record::PrincipalDelete {
            username: username.to_string(),
        };
        self.journal_commit(&[record]).map_err(|e| e.to_string())?;
        let removed = self
            .auth
            .lock()
            .unwrap()
            .delete_principal(username)
            .is_some();
        if removed {
            // FR-S08: revocation takes effect on the wire immediately.
            self.close_connections_of(username);
        }
        Ok(removed)
    }

    /// Open a persistent broker on a data directory: recover the journal
    /// into live state first (the journal is the only source of truth,
    /// ADR-0002), then open the writer for appends.
    pub fn open_persistent(user: String, password: String, data_dir: &std::path::Path) -> Self {
        Self::open_persistent_with_journal(user, password, data_dir, JournalConfig::default())
    }

    /// Memory-mode broker honoring a §13.2 configuration (switches,
    /// limits, budgets, alarm settings).
    pub fn new_from_config(user: String, password: String, cfg: &crate::config::Config) -> Self {
        let mut broker = Self::new(user, password);
        broker.apply_config(cfg);
        broker
    }

    /// Persistent-mode broker honoring a §13.2 configuration (journal
    /// shape AND the redb page-cache budget via the cached recovery
    /// path).
    pub fn open_persistent_from_config(
        user: String,
        password: String,
        data_dir: &std::path::Path,
        cfg: &crate::config::Config,
    ) -> Self {
        let (topology, store, projection, writer, auth) =
            rusty_mq_storage::rebuild::open_persistent_with_projection_cached(
                data_dir,
                MESSAGE_BYTE_BUDGET,
                cfg.journal_config(),
                Some(cfg.storage.index_cache_bytes),
            )
            .expect("recovery must succeed or startup must fail explicitly");
        let mut broker = Self::from_recovered(
            user, password, data_dir, topology, store, projection, writer, auth,
        );
        broker.apply_config(cfg);
        broker
    }

    /// Apply the config-driven surfaces after construction. Runs BEFORE
    /// the listener accepts (serve path), so no connection observes the
    /// defaults.
    fn apply_config(&mut self, cfg: &crate::config::Config) {
        self.protocol_limits = cfg.protocol_limits();
        self.message_budget = cfg.limits.managed_buffer_bytes as usize;
        self.memory_alarm_bytes = cfg.limits.memory_alarm_bytes;
        self.queue_labels_enabled = cfg.metrics.queue_labels_enabled;
        self.admission = AdmissionCaps {
            max_connections: cfg.limits.max_connections,
            max_queues_per_vhost: cfg.limits.max_queues_per_vhost,
            max_bindings_per_vhost: cfg.limits.max_bindings_per_vhost,
            max_destinations_per_publish: cfg.limits.max_destinations_per_publish,
            max_pending_confirms_per_channel: cfg.limits.max_pending_confirms_per_channel,
        };
        *self.topology.lock().unwrap() = Topology::new(cfg.compatibility());
        let (min_bytes, ratio) = cfg.alarm_settings();
        let mut alarms = self.alarms.lock().unwrap();
        alarms.disk_free_min_bytes = min_bytes;
        alarms.disk_free_min_ratio = ratio;
    }

    /// Test/ops variant of [`Broker::open_persistent`] with an explicit
    /// journal configuration (e.g. no-rotation segments to exercise the
    /// seal-on-compact path; T28 regression).
    #[doc(hidden)]
    pub fn open_persistent_with_journal(
        user: String,
        password: String,
        data_dir: &std::path::Path,
        journal: JournalConfig,
    ) -> Self {
        let (topology, store, projection, writer, auth) =
            rusty_mq_storage::rebuild::open_persistent_with_projection(
                data_dir,
                MESSAGE_BYTE_BUDGET,
                journal,
            )
            .expect("recovery must succeed or startup must fail explicitly");
        Self::from_recovered(
            user, password, data_dir, topology, store, projection, writer, auth,
        )
    }

    /// Assemble a persistent broker from an already-recovered state
    /// tuple (shared by the plain and cache-sized recovery paths;
    /// includes first-run admin bootstrap).
    #[allow(clippy::too_many_arguments)] // one-shot assembly tuple
    fn from_recovered(
        user: String,
        password: String,
        data_dir: &std::path::Path,
        topology: Topology,
        store: MessageStore,
        projection: rusty_mq_storage::projection::Projection,
        writer: JournalWriter,
        auth: rusty_mq_core::auth::AuthState,
    ) -> Self {
        let mut auth = auth;
        let writer = writer;
        if auth.principal(&user).is_none() && auth.principal("admin").is_none() {
            // First run bootstrap: the flagged credentials become the
            // durable admin (clearly logged; rotatable via the CLI).
            tracing::warn!(
                user = %user,
                "no durable principals found; bootstrapping the flagged credentials as admin"
            );
            let phc = argon2_hash(&password).expect("argon2 hashing is infallible in-process");
            auth.upsert_principal(Principal {
                username: user.clone(),
                password_phc: phc,
                role: Role::Admin,
            });
            auth.set_permissions(
                &user,
                "/",
                Permissions {
                    configure: ".*".into(),
                    write: ".*".into(),
                    read: ".*".into(),
                },
            );
            // Persist the bootstrap before serving (direct commit; the
            // broker isn't shared yet, ordering is construction-local).
            let bootstrap = vec![
                rusty_mq_storage::Record::PrincipalUpsert(rusty_mq_storage::PrincipalRecord {
                    username: user.clone(),
                    password_phc: auth.principal(&user).unwrap().password_phc.clone(),
                    role: Role::Admin.as_u8(),
                }),
                rusty_mq_storage::Record::PermissionSet(rusty_mq_storage::PermissionRecord {
                    username: user.clone(),
                    vhost: "/".into(),
                    configure: ".*".into(),
                    write: ".*".into(),
                    read: ".*".into(),
                }),
            ];
            let fence = writer
                .commit(&bootstrap)
                .expect("bootstrap journal commit must succeed");
            let _ = projection.apply(&bootstrap, fence).inspect_err(|e| {
                tracing::warn!(error = %e, "bootstrap projection apply failed; will rebuild");
            });
        }
        tracing::info!(
            data_dir = %data_dir.display(),
            "recovered durable state (journal + projection)"
        );
        Self {
            data_dir: Some(data_dir.to_path_buf()),
            snapshot_generation: AtomicU64::new(0),
            compact_threshold: std::sync::atomic::AtomicU64::new(COMPACT_THRESHOLD_BYTES),
            protocol_limits: rusty_mq_protocol::ProtocolLimits::default(),
            memory_alarm_bytes: MESSAGE_BYTE_BUDGET as u64,
            message_budget: MESSAGE_BYTE_BUDGET,
            admission: AdmissionCaps::default(),
            queue_labels_enabled: false,
            auth: Mutex::new(auth),
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
            metrics: Metrics::default(),
            auth_throttle: crate::throttle::AuthThrottle::new(
                std::time::Duration::from_secs(10),
                10,
                100,
            ),
            alarms: Mutex::new(Alarms::default()),
            live_connections: Mutex::new(Vec::new()),
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
        // §6.4/§7.4: a disk alarm stops durable writes — never a false
        // successful confirm under uncertain persistence.
        if self.disk_alarm() {
            tracing::error!("journal commit refused: disk alarm active");
            return Err(JournalCommitError);
        }
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
        // 3. Seal the ACTIVE segment when the snapshot fully covers it so
        //    reclamation may drop it too (T28 soak finding: without the
        //    seal, a writer that never reaches segment rotation keeps one
        //    growing segment and compaction reclaims nothing). Busy journal
        //    falls back to the pre-seal keep_segment — conservative, the
        //    next compaction seals.
        let keep_segment = match self.journal.try_lock() {
            Ok(journal) => {
                let w = journal.as_ref().expect("compact requires the journal");
                if w.seal_if_covered(covered_lsn).map_err(|e| e.to_string())? {
                    w.current_segment_id() // post-seal fresh segment
                } else {
                    keep_segment
                }
            }
            Err(_) => keep_segment, // busy: conservative, seal next round
        };
        // 4. Only now may covered segments and superseded snapshots go
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

    /// Register a live connection. Returns false when the §10
    /// max_connections budget is exhausted — the caller must refuse the
    /// connection (never silent acceptance).
    pub fn register_connection(&self, conn: LiveConnection) -> bool {
        let mut live = self.live_connections.lock().unwrap();
        if live.len() >= self.admission.max_connections as usize {
            return false;
        }
        live.push(conn);
        true
    }

    pub fn unregister_connection(&self, id: ConnectionId) {
        self.live_connections.lock().unwrap().retain(|c| c.id != id);
    }

    /// Record the authenticated username once SASL PLAIN completes
    /// (registration happens pre-auth).
    pub fn set_connection_user(&self, id: ConnectionId, username: &str) {
        if let Some(conn) = self
            .live_connections
            .lock()
            .unwrap()
            .iter_mut()
            .find(|c| c.id == id)
        {
            conn.username = username.to_string();
        }
    }

    /// Snapshot live connections (id, username) for the management API.
    pub fn list_connections(&self) -> Vec<(ConnectionId, String)> {
        self.live_connections
            .lock()
            .unwrap()
            .iter()
            .map(|c| (c.id, c.username.clone()))
            .collect()
    }

    /// Server-initiated close of one connection (operator action). True
    /// when a live connection accepted the command.
    pub fn close_connection(&self, id: ConnectionId, reason: &str) -> bool {
        let target = self
            .live_connections
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.id == id)
            .map(|c| c.control.clone());
        match target {
            Some(control) => control
                .try_send(Control::Close {
                    reply_code: 320, // CONNECTION_FORCED
                    reason: reason.to_string(),
                })
                .is_ok(),
            None => false,
        }
    }

    /// Close every live connection authenticated as `username` (FR-S08:
    /// revocation takes effect on the wire, not just for new ops).
    pub fn close_connections_of(&self, username: &str) {
        let targets: Vec<ConnectionId> = self
            .live_connections
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.username == username)
            .map(|c| c.id)
            .collect();
        for id in targets {
            self.close_connection(id, "credentials revoked");
        }
    }

    /// Evaluate alarms and broadcast blocked/unblocked transitions to all
    /// live connections (FR-R07). Callers pass the connection filter
    /// (capability gating happens connection-side).
    pub fn evaluate_alarms_and_notify(&self) {
        let transitions = {
            let store_bytes = self.store.lock().unwrap().total_bytes();
            let mut alarms = self.alarms.lock().unwrap();
            alarms.evaluate(
                store_bytes,
                self.memory_alarm_bytes as usize,
                self.data_dir.as_deref(),
            )
        };
        if !transitions.any() {
            return;
        }
        let notifications: Vec<Control> = [
            (
                transitions.memory_raised || transitions.disk_raised,
                Control::Blocked("resource alarm".into()),
            ),
            (
                transitions.memory_cleared || transitions.disk_cleared,
                Control::Unblocked,
            ),
        ]
        .iter()
        .filter(|(fire, _)| *fire)
        .map(|(_, msg)| msg.clone())
        .collect();
        if notifications.is_empty() {
            return;
        }
        let mut live = self.live_connections.lock().unwrap();
        live.retain(|conn| {
            // try_send: never block the evaluating thread; a full control
            // channel means the connection is slow — its bounded writer
            // already backpressures it.
            notifications
                .iter()
                .all(|msg| conn.control.try_send(msg.clone()).is_ok())
                || conn.control.try_send(Control::Unblocked).is_ok()
        });
    }

    /// Whether durable admissions must quiesce (disk alarm, §6.4).
    pub fn disk_alarm(&self) -> bool {
        self.alarms.lock().unwrap().disk()
    }

    /// Memory alarm state (admission gating, §10).
    pub fn memory_alarm(&self) -> bool {
        self.alarms.lock().unwrap().memory()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argon2_roundtrip() {
        let phc = argon2_hash("app-pass").unwrap();
        assert!(phc.starts_with("$argon2id$"));
        assert!(argon2_verify(&phc, "app-pass"), "correct password verifies");
        assert!(!argon2_verify(&phc, "wrong"), "wrong password fails");
    }
}
