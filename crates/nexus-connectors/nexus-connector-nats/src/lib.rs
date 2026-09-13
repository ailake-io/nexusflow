//! Generic bridging connector for core NATS pub/sub (not JetStream):
//! each message payload is JSON, projected onto `fields`, same
//! contract as `nexus-connector-kafka`/`nexus-connector-mqtt`.
//!
//! `async-nats` is a real async network dependency, so the client is
//! behind the `client` Cargo feature (CLAUDE.md §8.5) — building this
//! crate with no features enabled compiles config/payload parsing
//! only, no network client linkage.

mod config;
mod payload;
#[cfg(feature = "client")]
mod sink;
#[cfg(feature = "client")]
mod source;

pub use config::{NatsConnectorConfig, NatsDataType, NatsFieldSpec};
pub use payload::{build_schema, parse_payload};
#[cfg(feature = "client")]
pub use sink::NatsSink;
#[cfg(feature = "client")]
pub use source::NatsSource;

#[cfg(feature = "client")]
nexus_core::submit_connector!(
    "nats",
    nexus_core::ConnectorCapability::Bridged,
    NatsConnectorConfig
);
