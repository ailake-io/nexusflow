//! Pure Parquet sink/source — Marco 6 (Data Lake formats), no Delta/Iceberg
//! metadata layer, `parquet` crate directly. See IMPLEMENTATION_PLAN.md
//! Marco 6.

mod config;
mod rows;
mod sink;
mod source;
mod store;

pub use config::{ParquetCompression, ParquetConnectorConfig, StorageType};
pub use sink::ParquetSink;
pub use source::ParquetSource;

nexus_core::submit_connector!(
    "parquet",
    nexus_core::ConnectorCapability::Bridged,
    ParquetConnectorConfig
);
nexus_core::submit_local_path_connector!("parquet");
