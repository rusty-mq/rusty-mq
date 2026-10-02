//! Strong identifiers and generation tokens (PRD §15.2).
//!
//! Numeric AMQP channel ids, delivery tags, and internal entity ids are never
//! mixed: each has its own type. Queue identities survive restarts as opaque
//! internal ids; user-facing queue names are never used as storage paths.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic id minted for every live connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(u64);

/// Stable identity of a vhost (opaque; assigned at creation).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VhostId(u64);

/// Stable identity of an exchange within a vhost (opaque).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExchangeId(u64);

/// Stable identity of a queue within a vhost (opaque).
///
/// A deleted and recreated queue with the same user-facing name receives a
/// *new* `QueueId` (INV-07); journal records reference these ids, never names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueId(u64);

/// Generation token minted at each `channel.open` (ADR-0003, INV-04).
///
/// Settlements carry the generation of the channel that owns the delivery; a
/// stale generation can never mutate a re-created channel's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChannelGeneration(u64);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn mint() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

macro_rules! simple_id {
    ($name:ident, $label:expr) => {
        impl $name {
            /// Mint a fresh unique id.
            pub fn new() -> Self {
                Self(mint())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($label, "{}"), self.0)
            }
        }
    };
}

simple_id!(ConnectionId, "conn:");
simple_id!(VhostId, "vh:");
simple_id!(ExchangeId, "ex:");
simple_id!(QueueId, "q:");
simple_id!(ChannelGeneration, "chgen:");

impl QueueId {
    /// Deterministic construction for white-box tests (also used from
    /// integration tests in other crates; not used by broker logic).
    pub fn for_test(v: u64) -> Self {
        Self(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique() {
        assert_ne!(QueueId::new(), QueueId::new());
        assert_ne!(VhostId::new(), VhostId::new());
    }

    #[test]
    fn display_is_prefixed() {
        // Format only; numeric value is opaque.
        assert!(QueueId::new().to_string().starts_with("q:"));
    }
}
