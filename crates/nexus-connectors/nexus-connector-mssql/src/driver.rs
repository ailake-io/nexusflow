use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to `libadbc_driver_mssql.so` — the free, no-account ADBC
/// Driver Foundry driver for SQL Server/Synapse (confirmed real this
/// session: `dbc install mssql` installed it with no auth needed,
/// under a "Permissive Binary License" that explicitly allows
/// redistribution — see `scripts/fetch-adbc-mssql-driver.sh`).
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_MSSQL_PATH";

pub(crate) fn open_connection(uri: &str) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at libadbc_driver_mssql.so \
             (run scripts/fetch-adbc-mssql-driver.sh)"
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
