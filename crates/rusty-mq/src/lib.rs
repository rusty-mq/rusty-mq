//! rusty-mq broker library: composition surface for the executable and the
//! integration test harness.

pub mod admin_client;
pub mod alarms;
pub mod broker;
pub mod config;
pub mod connection;
pub mod consumers;
pub mod definitions;
pub mod management_impl;
pub mod metrics;
pub mod migration;
pub mod serve_settings;
pub mod server;
pub mod throttle;
pub mod tls;

pub use broker::Broker;
pub use rusty_mq_core::{Permissions, Principal, Role};
pub use rusty_mq_management::broker_facade::BrokerHandle;
