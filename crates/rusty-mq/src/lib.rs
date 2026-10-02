//! rusty-mq broker library: composition surface for the executable and the
//! integration test harness.

pub mod alarms;
pub mod broker;
pub mod connection;
pub mod consumers;
pub mod management_impl;
pub mod metrics;
pub mod server;

pub use broker::Broker;
pub use rusty_mq_core::{Permissions, Principal, Role};
pub use rusty_mq_management::broker_facade::BrokerHandle;
