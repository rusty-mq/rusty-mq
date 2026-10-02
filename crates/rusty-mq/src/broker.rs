//! Shared broker state: topology, M1 test users, connection registry.
//!
//! M1 authentication is a development-credentials map (clearly logged);
//! salted password hashes and permission checks arrive in M7 (FR-S01/S03).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use rusty_mq_core::store::MessageStore;
use rusty_mq_core::topology::{CompatibilitySwitches, Topology};
use rusty_mq_core::ConnectionId;

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
    /// M1: exactly one test user; M7 replaces this with durable principals.
    pub test_user: TestUser,
    connection_seq: AtomicU64,
}

impl Broker {
    pub fn new(user: String, password: String) -> Self {
        Self {
            topology: Mutex::new(Topology::new(CompatibilitySwitches::default())),
            store: Mutex::new(MessageStore::new(MESSAGE_BYTE_BUDGET)),
            test_user: TestUser {
                username: user,
                password,
            },
            connection_seq: AtomicU64::new(1),
        }
    }

    pub fn next_connection_id(&self) -> ConnectionId {
        ConnectionId::new()
    }

    pub fn connections_opened(&self) -> u64 {
        self.connection_seq.fetch_add(1, Ordering::Relaxed)
    }
}
