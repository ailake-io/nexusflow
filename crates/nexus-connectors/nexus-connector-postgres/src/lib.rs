//! ADBC fast-path connector for PostgreSQL. See ARCHITECTURE.md §3 and
//! IMPLEMENTATION_PLAN.md Marco 1.
//!
//! Requires `libadbc_driver_postgresql.so` at the path pointed to by the
//! `ADBC_DRIVER_POSTGRESQL_PATH` env var — build it with
//! `scripts/build-adbc-postgresql-driver.sh` (there is no crates.io
//! distribution of the driver itself).

#[cfg(feature = "cdc")]
mod cdc;
mod config;
mod driver;
mod introspect;
mod sink;
mod source;

#[cfg(feature = "cdc")]
pub use cdc::PostgresCdcSource;
#[cfg(feature = "cdc")]
pub use config::{PostgresCdcConfig, PostgresCdcDataType, PostgresCdcFieldSpec};
pub use config::{PostgresConnectorConfig, PostgresSslMode};
pub use driver::DRIVER_PATH_ENV;
pub use introspect::{primary_key_bounds, table_schema, PkPartitionKind};
pub use sink::PostgresSink;
pub use source::{split_into_partitions, PartitionRange, PostgresSource};

nexus_core::submit_connector!(
    "postgres",
    nexus_core::ConnectorCapability::AdbcNative,
    PostgresConnectorConfig
);

#[cfg(feature = "cdc")]
nexus_core::submit_connector!(
    "postgres-cdc",
    nexus_core::ConnectorCapability::Bridged,
    PostgresCdcConfig
);
