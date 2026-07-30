//! Bridging connector for Kafka. Basic consumer: decodes each message
//! payload as JSON and converts it to `RecordBatch` via `RecordBatchBuilder`.
//! Foundation for Marco 4's Debezium envelope/CDC opcode mode — see
//! ARCHITECTURE.md §2/§4.1 and IMPLEMENTATION_PLAN.md Marco 3/4.
//!
//! `librdkafka` is a native C dependency, so the consumer is behind the
//! `consumer` Cargo feature (CLAUDE.md §8.5) — building this crate with no
//! features enabled compiles config/payload parsing only, no native linkage.

mod config;
mod payload;
#[cfg(feature = "consumer")]
mod source;

pub use config::{KafkaConnectorConfig, KafkaDataType, KafkaFieldSpec};
pub use payload::{build_schema, parse_payload};
#[cfg(feature = "consumer")]
pub use source::KafkaSource;

#[cfg(feature = "consumer")]
nexus_core::submit_connector!("kafka", nexus_core::ConnectorCapability::Bridged);
