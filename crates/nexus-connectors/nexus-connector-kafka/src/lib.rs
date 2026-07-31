//! Bridging connector for Kafka. Two decode modes: `Raw` (each message
//! payload is JSON, projected onto `fields`) and `Debezium` (Marco 4 CDC —
//! decodes a Debezium change event and appends the `__opcode` column). See
//! ARCHITECTURE.md §2/§4.1/§7 and IMPLEMENTATION_PLAN.md Marco 3/4.
//!
//! `librdkafka` is a native C dependency, so the consumer is behind the
//! `consumer` Cargo feature (CLAUDE.md §8.5) — building this crate with no
//! features enabled compiles config/payload parsing only, no native linkage.

mod config;
mod payload;
#[cfg(feature = "consumer")]
mod source;

pub use config::{KafkaConnectorConfig, KafkaDataType, KafkaEnvelope, KafkaFieldSpec};
pub use nexus_core::OPCODE_COLUMN;
pub use payload::{build_schema, parse_payload};
#[cfg(feature = "consumer")]
pub use source::KafkaSource;

#[cfg(feature = "consumer")]
nexus_core::submit_connector!("kafka", nexus_core::ConnectorCapability::Bridged);
