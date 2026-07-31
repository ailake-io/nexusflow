//! AI-Lake sink/source — self-contained Parquet+HNSW vector-native
//! Lakehouse format (github.com/ailake-io/ai-lakehouse). Embedded backend
//! (`HadoopCatalog` + `LocalStore`), no server/container. See
//! IMPLEMENTATION_PLAN.md Marco 6.

mod bridge;
mod config;
mod rows;
mod sink;
mod source;

pub use config::AilakeConnectorConfig;
pub use sink::AilakeSink;
pub use source::AilakeSource;

nexus_core::submit_connector!("ailake", nexus_core::ConnectorCapability::Bridged);
