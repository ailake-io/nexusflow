use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to `libadbc_driver_postgresql.so`. There is no crates.io distribution
/// of the driver (it's a C++/libpq implementation) — build it with
/// `scripts/build-adbc-postgresql-driver.sh` and point this env var at the
/// output. See ARCHITECTURE.md §3.
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_POSTGRESQL_PATH";

pub(crate) fn open_connection(uri: &str) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at libadbc_driver_postgresql.so \
             (run scripts/build-adbc-postgresql-driver.sh)"
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
