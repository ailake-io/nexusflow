use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to the ClickHouse ADBC driver shared library. Unlike Postgres/SQLite
/// (which need a manual build via `scripts/build-adbc-*-driver.sh`), this
/// driver is official (built by ClickHouse, Inc.) and installs with a single
/// command: `dbc install clickhouse` (ADBC Driver Foundry, see
/// https://adbc-drivers.org/drivers/clickhouse/). Point this env var at the
/// installed `.so`/`.dylib`/`.dll`.
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_CLICKHOUSE_PATH";

pub(crate) fn open_connection(
    uri: &str,
    username: &str,
    password: &str,
) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at the ClickHouse ADBC driver \
             (install with `dbc install clickhouse`, see \
             https://adbc-drivers.org/drivers/clickhouse/)"
        ))
    })?;

    let mut driver =
        ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)
            .map_err(|e| NexusError::Connector(format!("failed to load ADBC driver: {e}")))?;

    // The ClickHouse ADBC driver expects the HTTP URL without a database path
    // component; credentials are supplied as separate options rather than the
    // URI userinfo (which the underlying Rust client does not parse).
    let opts: [(OptionDatabase, OptionValue); 3] = [
        (OptionDatabase::Uri, uri.into()),
        (OptionDatabase::Username, username.into()),
        (OptionDatabase::Password, password.into()),
    ];
    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| NexusError::Connector(format!("failed to open database: {e}")))?;

    database
        .new_connection()
        .map_err(|e| NexusError::Connector(format!("failed to open connection: {e}")))
}
