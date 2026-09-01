use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to the DuckDB ADBC driver shared library. Like
/// `nexus-connector-clickhouse` (and unlike Postgres/SQLite, which need a
/// manual build via `scripts/build-adbc-*-driver.sh`), this driver is
/// official and installs with a single command: `dbc install duckdb` (ADBC
/// Driver Foundry, see https://adbc-drivers.org/drivers/duckdb/). Point this
/// env var at the installed `.so`/`.dylib`/`.dll`.
///
/// Assumption (unverified against a live driver in this sandbox — no network
/// access to actually install/run it): the Driver Foundry package exposes
/// the standard `AdbcDriverInit` entrypoint symbol like every other driver
/// wrapped by `dbc install`, so `load_dynamic_from_filename`'s entrypoint
/// argument is `None` here exactly as it is for sqlite/clickhouse. If a real
/// build turns out to need a different symbol (DuckDB's own bundled
/// `duckdb_adbc_init` is a plausible alternative name if a raw `libduckdb`
/// build is used instead of the Foundry package), pass
/// `Some("duckdb_adbc_init")` as the second argument below.
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_DUCKDB_PATH";

pub(crate) fn open_connection(uri: &str) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at the DuckDB ADBC driver \
             (install with `dbc install duckdb`, see \
             https://adbc-drivers.org/drivers/duckdb/)"
        ))
    })?;

    let mut driver =
        ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)
            .map_err(|e| NexusError::Connector(format!("failed to load ADBC driver: {e}")))?;

    let opts = [(OptionDatabase::Uri, uri.into())];
    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| NexusError::Connector(format!("failed to open database: {e}")))?;

    database
        .new_connection()
        .map_err(|e| NexusError::Connector(format!("failed to open connection: {e}")))
}
