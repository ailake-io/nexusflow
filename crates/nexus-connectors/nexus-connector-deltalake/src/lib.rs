//! Delta Lake sink/source — Marco 6 (Data Lake formats), `deltalake` crate
//! directly. See IMPLEMENTATION_PLAN.md Marco 6.

mod config;
mod rows;
mod sink;
mod source;

pub use config::DeltaConnectorConfig;
pub use sink::DeltaSink;
pub use source::DeltaSource;

nexus_core::submit_connector!("deltalake", nexus_core::ConnectorCapability::Bridged);
