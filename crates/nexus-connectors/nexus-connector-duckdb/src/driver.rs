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
/// The library must expose the standard `AdbcDriverInit` entrypoint symbol,
/// except for DuckDB's own bundled build (e.g. the one dbt downloads), which
/// exports only `duckdb_adbc_init` — `open_connection` falls back to that
/// symbol automatically. Point this env var at the installed
/// `.so`/`.dylib`/`.dll`.
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_DUCKDB_PATH";

pub(crate) fn open_connection(uri: &str) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at the DuckDB ADBC driver \
             (install with `dbc install duckdb`, see \
             https://adbc-drivers.org/drivers/duckdb/)"
        ))
    })?;

    // Distribution-dependent entrypoint symbol: the ADBC Driver Foundry
    // package (`dbc install duckdb`) exports the standard `AdbcDriverInit`,
    // while DuckDB's own bundled build (e.g. the one dbt downloads) exports
    // only `duckdb_adbc_init`. Try the standard name first, fall back to the
    // DuckDB-specific one — a failed symbol resolution has no side effects,
    // so the fallback is safe.
    let mut driver =
        ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)
            .or_else(|_| {
                ManagedDriver::load_dynamic_from_filename(
                    &driver_path,
                    Some(b"duckdb_adbc_init"),
                    AdbcVersion::V110,
                )
            })
            .map_err(|e| NexusError::Connector(format!("failed to load ADBC driver: {e}")))?;

    let opts = [(OptionDatabase::Uri, uri.into())];
    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| NexusError::Connector(format!("failed to open database: {e}")))?;

    database
        .new_connection()
        .map_err(|e| NexusError::Connector(format!("failed to open connection: {e}")))
}
