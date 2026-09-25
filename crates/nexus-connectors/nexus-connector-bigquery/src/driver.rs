use crate::config::{BigqueryAuth, BigqueryConnectorConfig};
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to `libadbc_driver_bigquery.so` — see
/// `scripts/fetch-adbc-bigquery-driver.sh` (extracted prebuilt from the
/// official `adbc-driver-bigquery` PyPI wheel, same mechanism as
/// `nexus-connector-snowflake`'s driver).
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_BIGQUERY_PATH";

/// Option key names below are the real ones the official driver exposes —
/// confirmed by reading `adbc_driver_bigquery/__init__.py`'s
/// `DatabaseOptions` enum straight out of the PyPI wheel (2026-08-18),
/// not guessed from generic ADBC docs.
mod bigquery_options {
    pub const PROJECT_ID: &str = "adbc.bigquery.sql.project_id";
    pub const DATASET_ID: &str = "adbc.bigquery.sql.dataset_id";
    pub const LOCATION: &str = "adbc.bigquery.sql.location";
    pub const AUTH_TYPE: &str = "adbc.bigquery.sql.auth_type";
    pub const AUTH_CREDENTIALS: &str = "adbc.bigquery.sql.auth_credentials";

    pub const AUTH_TYPE_JSON_CREDENTIAL_STRING: &str =
        "adbc.bigquery.sql.auth_type.json_credential_string";
}

pub(crate) fn open_connection(
    cfg: &BigqueryConnectorConfig,
) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at libadbc_driver_bigquery.so \
             (run scripts/fetch-adbc-bigquery-driver.sh)"
        ))
    })?;

    let mut driver =
        ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)
            .map_err(|e| NexusError::Connector(format!("failed to load ADBC driver: {e}")))?;

    let mut opts = vec![
        (
            OptionDatabase::Other(bigquery_options::PROJECT_ID.into()),
            cfg.project_id.clone().into(),
        ),
        (
            OptionDatabase::Other(bigquery_options::DATASET_ID.into()),
            cfg.dataset_id.clone().into(),
        ),
    ];
    if let Some(location) = &cfg.location {
        opts.push((
            OptionDatabase::Other(bigquery_options::LOCATION.into()),
            location.clone().into(),
        ));
    }
    match &cfg.auth {
        BigqueryAuth::ServiceAccountJson { credentials_json } => {
            opts.push((
                OptionDatabase::Other(bigquery_options::AUTH_TYPE.into()),
                bigquery_options::AUTH_TYPE_JSON_CREDENTIAL_STRING.into(),
            ));
            opts.push((
                OptionDatabase::Other(bigquery_options::AUTH_CREDENTIALS.into()),
                credentials_json.clone().into(),
            ));
        }
    }

    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| NexusError::Connector(format!("failed to open database: {e}")))?;

    database
        .new_connection()
        .map_err(|e| NexusError::Connector(format!("failed to open connection: {e}")))
}
