//! AI-Lake sink/source — self-contained Parquet+HNSW vector-native
//! Lakehouse format (github.com/ailake-io/ai-lakehouse). Embedded backend
//! (`HadoopCatalog` + `LocalStore`), no server/container. See
//! IMPLEMENTATION_PLAN.md Marco 6.

mod bridge;
#[cfg(feature = "cdc")]
mod cdc;
mod config;
mod rows;
mod sink;
mod source;

#[cfg(feature = "cdc")]
pub use cdc::AilakeCdcSource;
#[cfg(feature = "cdc")]
pub use config::{AilakeCdcConfig, AilakeConnectorConfig, AilakeStorageOptions};
pub use sink::AilakeSink;
pub use source::AilakeSource;

nexus_core::submit_connector!(
    "ailake",
    nexus_core::ConnectorCapability::Bridged,
    AilakeConnectorConfig
);

#[cfg(feature = "cdc")]
nexus_core::submit_connector!(
    "ailake-cdc",
    nexus_core::ConnectorCapability::Bridged,
    AilakeCdcConfig
);
