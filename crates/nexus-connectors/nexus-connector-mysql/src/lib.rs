//! Bridging connector for MySQL/MariaDB. No ADBC driver exists upstream (see
//! `config.rs`'s module doc), so the batch mode (`"mysql"`) converts rows via
//! `RecordBatchBuilder` — same posture as `nexus-connector-mongodb`. CDC
//! (`"mysql-cdc"`) reads the binlog directly via `mysql_cdc`, no Debezium/
//! Kafka in front. See `ARCHITECTURE.md §7`.

#[cfg(feature = "cdc")]
mod cdc;
mod config;
mod rows;
mod sink;
mod source;

#[cfg(feature = "cdc")]
pub use cdc::MySqlCdcSource;
#[cfg(feature = "cdc")]
pub use config::MySqlCdcConfig;
pub use config::{MySqlCdcDataType, MySqlCdcFieldSpec, MySqlConnectorConfig};
pub use sink::MySqlSink;
pub use source::MySqlSource;

nexus_core::submit_connector!(
    "mysql",
    nexus_core::ConnectorCapability::Bridged,
    MySqlConnectorConfig
);

#[cfg(feature = "cdc")]
nexus_core::submit_connector!(
    "mysql-cdc",
    nexus_core::ConnectorCapability::Bridged,
    MySqlCdcConfig
);
