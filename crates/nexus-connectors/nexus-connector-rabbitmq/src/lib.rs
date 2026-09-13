//! Generic bridging connector for RabbitMQ (AMQP 0-9-1): each message
//! payload is JSON, projected onto `fields`, same contract as
//! `nexus-connector-kafka`/`nexus-connector-nats`.
//!
//! `lapin` is a real async network dependency, so the client is
//! behind the `client` Cargo feature (CLAUDE.md §8.5) — building this
//! crate with no features enabled compiles config/payload parsing
//! only, no network client linkage.

mod config;
mod payload;
#[cfg(feature = "client")]
mod sink;
#[cfg(feature = "client")]
mod source;

pub use config::{RabbitmqConnectorConfig, RabbitmqDataType, RabbitmqFieldSpec};
pub use payload::{build_schema, parse_payload};
#[cfg(feature = "client")]
pub use sink::RabbitmqSink;
#[cfg(feature = "client")]
pub use source::RabbitmqSource;

#[cfg(feature = "client")]
nexus_core::submit_connector!(
    "rabbitmq",
    nexus_core::ConnectorCapability::Bridged,
    RabbitmqConnectorConfig
);
