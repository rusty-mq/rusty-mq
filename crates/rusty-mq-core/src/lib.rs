//! Broker core: typed identifiers, topology registry, and routing.
//!
//! This crate is wire-agnostic: it knows AMQP *semantics* (exchanges, queues,
//! bindings, routing) but not frames. The protocol crate translates frames
//! into the typed operations defined here (PRD §8 boundaries).
//!
//! M2 status: in-memory topology and routing are implemented; persistence
//! arrives in M4 via `rusty-mq-storage`.

pub mod ids;
pub mod routing;
pub mod topology;

pub use ids::{ChannelGeneration, ConnectionId, ExchangeId, QueueId, VhostId};
pub use routing::{route_message, ExchangeType};
pub use topology::{
    BindingKey, DeclareExchangeError, DeclareQueueError, QueueProfile, Topology, TopologyError,
};
