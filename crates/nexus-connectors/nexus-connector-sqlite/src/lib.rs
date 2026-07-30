//! ADBC connector for SQLite. See ARCHITECTURE.md §3 and
//! IMPLEMENTATION_PLAN.md Marco 2 — this is the second connector, proving
//! `ConnectorRegistry` works with 2+ connectors without changing
//! nexus-server.
//!
//! Requires `libadbc_driver_sqlite.so` at the path pointed to by the
//! `ADBC_DRIVER_SQLITE_PATH` env var — build it with
//! `scripts/build-adbc-sqlite-driver.sh`.

mod config;
mod driver;
mod sink;
mod source;

pub use config::SqliteConnectorConfig;
pub use driver::DRIVER_PATH_ENV;
pub use sink::SqliteSink;
pub use source::SqliteSource;

nexus_core::submit_connector!("sqlite", nexus_core::ConnectorCapability::AdbcNative);
