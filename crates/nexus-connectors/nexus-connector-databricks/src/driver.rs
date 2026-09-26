use crate::config::DatabricksConnectorConfig;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to the Databricks ADBC driver's shared library. Installed via
/// [ADBC Driver Foundry](https://adbc-drivers.org/drivers/databricks/)'s
/// own CLI — `dbc install databricks` — which needs no account/login,
/// unlike Snowflake/BigQuery's drivers, which this repo extracts from a
/// PyPI wheel via a `scripts/fetch-adbc-*.sh` script. No such script
/// exists for Databricks; point this env var at wherever `dbc install`
/// placed the library.
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_DATABRICKS_PATH";

/// Databricks's driver takes the whole connection as a single `"uri"`
/// option key (confirmed at adbc-drivers.org/drivers/databricks/,
/// 2026-08-22) — unlike Snowflake's driver, which exposes ~8 separate
/// `adbc.snowflake.sql.*` option keys (confirmed by reading that driver's
/// own Python wheel source). No per-field mapping needed here beyond
/// building the URI itself (`DatabricksConnectorConfig::connection_uri`).
pub(crate) fn open_connection(
    cfg: &DatabricksConnectorConfig,
) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at the Databricks ADBC driver's \
             shared library (install with `dbc install databricks`, see \
             https://adbc-drivers.org/drivers/databricks/)"
        ))
    })?;

    let mut driver =
        ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)
            .map_err(|e| NexusError::Connector(format!("failed to load ADBC driver: {e}")))?;

    let opts = vec![(
        OptionDatabase::Other("uri".into()),
        cfg.connection_uri().into(),
    )];

    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| NexusError::Connector(format!("failed to open database: {e}")))?;

    database
        .new_connection()
        .map_err(|e| NexusError::Connector(format!("failed to open connection: {e}")))
}
