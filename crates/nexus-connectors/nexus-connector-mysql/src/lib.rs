//! Native CDC connector for MySQL/MariaDB — reads the binlog directly via
//! `mysql_cdc`, no Debezium/Kafka in front. See `ARCHITECTURE.md §7`. CDC
//! only: no batch/table-scan mode exists in this crate (mirrors
//! `nexus-connector-kafka`'s posture — a connector inherently built around
//! a streaming protocol).

mod cdc;
mod config;

pub use cdc::MySqlCdcSource;
pub use config::{MySqlCdcConfig, MySqlCdcDataType, MySqlCdcFieldSpec};

nexus_core::submit_connector!(
    "mysql-cdc",
    nexus_core::ConnectorCapability::Bridged,
    MySqlCdcConfig
);
