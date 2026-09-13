//! ADBC fast-path connector for DuckDB (embedded, like SQLite — a file or
//! `:memory:`, no server process). See ARCHITECTURE.md §3.
//!
//! Requires the DuckDB ADBC driver at the path pointed to by the
//! `ADBC_DRIVER_DUCKDB_PATH` env var. Like ClickHouse (and unlike
//! Postgres/SQLite), this is an official driver distributed via the ADBC
//! Driver Foundry (`dbc install duckdb`, see
//! https://adbc-drivers.org/drivers/duckdb/) — no manual build needed.
//!
//! Unlike ClickHouse, DuckDB supports a real `INSERT ... ON CONFLICT DO
//! UPDATE` (3.24+-SQLite-style syntax, present since DuckDB 0.9), so the sink
//! upserts by `primary_key` instead of being append-only — see `sink.rs`.
//! No `partition_column`/parallel-read splitting (same choice as SQLite: a
//! single embedded-file connection has no benefit from range partitioning
//! the way a networked server does).

mod config;
mod driver;
mod sink;
mod source;

pub use config::DuckdbConnectorConfig;
pub use driver::DRIVER_PATH_ENV;
pub use sink::DuckdbSink;
pub use source::DuckdbSource;

nexus_core::submit_connector!(
    "duckdb",
    nexus_core::ConnectorCapability::AdbcNative,
    DuckdbConnectorConfig
);
nexus_core::submit_local_path_connector!("duckdb");
