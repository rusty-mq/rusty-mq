//! Broker core: typed identifiers, topology registry, and routing.
//!
//! This crate is wire-agnostic: it knows AMQP *semantics* (exchanges, queues,
//! bindings, routing) but not frames. The protocol crate translates frames
//! into the typed operations defined here (PRD §8 boundaries).
//!
//! M2 status: in-memory topology and routing are implemented; persistence
//! arrives in M4 via `rusty-mq-storage`.

pub mod auth;
pub mod ids;
pub mod routing;
pub mod store;
pub mod topology;

pub use auth::{Access, AuthState, Permissions, Principal, Role};
pub use ids::{ChannelGeneration, ConnectionId, ExchangeId, QueueId, VhostId};
pub use routing::{route_message, ExchangeType};
pub use store::{AdmitError, MessageStore, QueueEntry, StoredMessage};
pub use topology::{
    BindingKey, DeclareExchangeError, DeclareQueueError, QueueProfile, Topology, TopologyError,
};
