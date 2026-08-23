//! ADBC fast-path connector for ClickHouse. See ARCHITECTURE.md §3.
//!
//! Requires the ClickHouse ADBC driver at the path pointed to by the
//! `ADBC_DRIVER_CLICKHOUSE_PATH` env var. Unlike Postgres/SQLite, this is an
//! official driver (built by ClickHouse, Inc.) with a one-command install:
//! `dbc install clickhouse` (ADBC Driver Foundry, see
//! https://adbc-drivers.org/drivers/clickhouse/) — no manual build needed.
//!
//! Sink is append-only: ClickHouse has no lightweight upsert/delete
//! equivalent to Postgres's `ON CONFLICT`. See `sink.rs`'s doc comment.

mod config;
mod driver;
mod sink;
mod source;

pub use config::ClickHouseConnectorConfig;
pub use driver::DRIVER_PATH_ENV;
pub use sink::ClickHouseSink;
pub use source::{split_into_partitions, ClickHouseSource, PartitionRange};

nexus_core::submit_connector!(
    "clickhouse",
    nexus_core::ConnectorCapability::AdbcNative,
    ClickHouseConnectorConfig
);
