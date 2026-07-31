//! Iceberg sink/source — Marco 6 (Data Lake formats), `iceberg` +
//! `iceberg-catalog-sql` crates. See IMPLEMENTATION_PLAN.md Marco 6.

mod catalog;
mod config;
mod sink;
mod source;

pub use config::{IcebergConnectorConfig, IcebergFormatVersion};
pub use sink::IcebergSink;
pub use source::IcebergSource;

nexus_core::submit_connector!("iceberg", nexus_core::ConnectorCapability::Bridged);
