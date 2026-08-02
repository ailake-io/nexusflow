//! Pure Parquet sink/source — Marco 6 (Data Lake formats), no Delta/Iceberg
//! metadata layer, `parquet` crate directly. See IMPLEMENTATION_PLAN.md
//! Marco 6.

mod config;
mod rows;
mod sink;
mod source;

pub use config::ParquetConnectorConfig;
pub use sink::ParquetSink;
pub use source::ParquetSource;

nexus_core::submit_connector!(
    "parquet",
    nexus_core::ConnectorCapability::Bridged,
    ParquetConnectorConfig
);
