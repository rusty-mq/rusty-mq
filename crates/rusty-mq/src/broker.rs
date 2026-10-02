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
