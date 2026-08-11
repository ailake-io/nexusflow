//! Iceberg sink/source — Marco 6 (Data Lake formats), `iceberg` +
//! `iceberg-catalog-sql` crates. See IMPLEMENTATION_PLAN.md Marco 6.

mod catalog;
#[cfg(feature = "cdc")]
mod cdc;
mod config;
mod sink;
mod source;

#[cfg(feature = "cdc")]
pub use cdc::IcebergCdcSource;
#[cfg(feature = "cdc")]
pub use config::IcebergCdcConfig;
pub use config::{IcebergConnectorConfig, IcebergFormatVersion};
pub use sink::IcebergSink;
pub use source::IcebergSource;

nexus_core::submit_connector!(
    "iceberg",
    nexus_core::ConnectorCapability::Bridged,
    IcebergConnectorConfig
);

#[cfg(feature = "cdc")]
nexus_core::submit_connector!(
    "iceberg-cdc",
    nexus_core::ConnectorCapability::Bridged,
    IcebergCdcConfig
);
