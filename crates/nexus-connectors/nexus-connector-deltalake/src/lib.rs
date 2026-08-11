//! Delta Lake sink/source — Marco 6 (Data Lake formats), `deltalake` crate
//! directly. See IMPLEMENTATION_PLAN.md Marco 6.

#[cfg(feature = "cdc")]
mod cdc;
mod config;
mod rows;
mod sink;
mod source;

#[cfg(feature = "cdc")]
pub use cdc::DeltaCdcSource;
pub use config::DeltaConnectorConfig;
#[cfg(feature = "cdc")]
pub use config::DeltaCdcConfig;
pub use sink::DeltaSink;
pub use source::DeltaSource;

nexus_core::submit_connector!(
    "deltalake",
    nexus_core::ConnectorCapability::Bridged,
    DeltaConnectorConfig
);

#[cfg(feature = "cdc")]
nexus_core::submit_connector!(
    "deltalake-cdc",
    nexus_core::ConnectorCapability::Bridged,
    DeltaCdcConfig
);
