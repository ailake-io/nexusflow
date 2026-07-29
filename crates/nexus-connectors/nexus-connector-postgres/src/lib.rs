//! ADBC fast-path connector for PostgreSQL. See ARCHITECTURE.md §3 and
//! IMPLEMENTATION_PLAN.md Marco 1.
//!
//! Requires `libadbc_driver_postgresql.so` at the path pointed to by the
//! `ADBC_DRIVER_POSTGRESQL_PATH` env var — build it with
//! `scripts/build-adbc-postgresql-driver.sh` (there is no crates.io
//! distribution of the driver itself).

mod config;
mod driver;
mod introspect;
mod sink;
mod source;

pub use config::PostgresConnectorConfig;
pub use driver::DRIVER_PATH_ENV;
pub use introspect::{primary_key_bounds, table_schema};
pub use sink::PostgresSink;
pub use source::{PartitionRange, PostgresSource, split_into_partitions};

nexus_core::submit_connector!("postgres", nexus_core::ConnectorCapability::AdbcNative);
