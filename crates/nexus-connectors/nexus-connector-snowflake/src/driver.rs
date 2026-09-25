use crate::config::{SnowflakeAuth, SnowflakeConnectorConfig};
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database as _, Driver as _};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use nexus_core::NexusError;
use std::env;

/// Path to `libadbc_driver_snowflake.so` — see
/// `scripts/fetch-adbc-snowflake-driver.sh` (extracted prebuilt from the
/// official `adbc-driver-snowflake` PyPI wheel, not built from source —
/// unlike Postgres/SQLite's ADBC drivers, which are C++ and built via
/// cmake, this one is Go but ships as a ready binary).
pub const DRIVER_PATH_ENV: &str = "ADBC_DRIVER_SNOWFLAKE_PATH";

/// Option key names below are the real ones the official driver exposes —
/// confirmed by reading `adbc_driver_snowflake/__init__.py`'s
/// `DatabaseOptions`/`AuthType` enums straight out of the PyPI wheel
/// (2026-08-18), not guessed from generic ADBC docs.
mod snowflake_options {
    pub const ACCOUNT: &str = "adbc.snowflake.sql.account";
    pub const DATABASE: &str = "adbc.snowflake.sql.db";
    pub const SCHEMA: &str = "adbc.snowflake.sql.schema";
    pub const WAREHOUSE: &str = "adbc.snowflake.sql.warehouse";
    pub const ROLE: &str = "adbc.snowflake.sql.role";
    pub const AUTH_TYPE: &str = "adbc.snowflake.sql.auth_type";
    pub const JWT_PRIVATE_KEY_VALUE: &str =
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value";
    pub const JWT_PRIVATE_KEY_PASSWORD: &str =
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password";

    pub const AUTH_TYPE_SNOWFLAKE: &str = "auth_snowflake";
    pub const AUTH_TYPE_JWT: &str = "auth_jwt";
}

pub(crate) fn open_connection(
    cfg: &SnowflakeConnectorConfig,
) -> Result<ManagedConnection, NexusError> {
    let driver_path = env::var(DRIVER_PATH_ENV).map_err(|_| {
        NexusError::Connector(format!(
            "{DRIVER_PATH_ENV} not set — point it at libadbc_driver_snowflake.so \
             (run scripts/fetch-adbc-snowflake-driver.sh)"
        ))
    })?;

    let mut driver =
        ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::V110)
            .map_err(|e| NexusError::Connector(format!("failed to load ADBC driver: {e}")))?;

    let mut opts = vec![
        (
            OptionDatabase::Other(snowflake_options::ACCOUNT.into()),
            cfg.account.clone().into(),
        ),
        (
            OptionDatabase::Other(snowflake_options::DATABASE.into()),
            cfg.database.clone().into(),
        ),
        (
            OptionDatabase::Other(snowflake_options::SCHEMA.into()),
            cfg.schema.clone().into(),
        ),
        (
            OptionDatabase::Other(snowflake_options::WAREHOUSE.into()),
            cfg.warehouse.clone().into(),
        ),
    ];
    if let Some(role) = &cfg.role {
        opts.push((
            OptionDatabase::Other(snowflake_options::ROLE.into()),
            role.clone().into(),
        ));
    }
    match &cfg.auth {
        SnowflakeAuth::Password { username, password } => {
            opts.push((
                OptionDatabase::Other(snowflake_options::AUTH_TYPE.into()),
                snowflake_options::AUTH_TYPE_SNOWFLAKE.into(),
            ));
            opts.push((OptionDatabase::Username, username.clone().into()));
            opts.push((OptionDatabase::Password, password.clone().into()));
        }
        SnowflakeAuth::KeyPair {
            username,
            private_key,
            private_key_password,
        } => {
            opts.push((
                OptionDatabase::Other(snowflake_options::AUTH_TYPE.into()),
                snowflake_options::AUTH_TYPE_JWT.into(),
            ));
            opts.push((OptionDatabase::Username, username.clone().into()));
            opts.push((
                OptionDatabase::Other(snowflake_options::JWT_PRIVATE_KEY_VALUE.into()),
                private_key.clone().into(),
            ));
            if let Some(password) = private_key_password {
                opts.push((
                    OptionDatabase::Other(snowflake_options::JWT_PRIVATE_KEY_PASSWORD.into()),
                    password.clone().into(),
                ));
            }
        }
    }

    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| NexusError::Connector(format!("failed to open database: {e}")))?;

    database
        .new_connection()
        .map_err(|e| NexusError::Connector(format!("failed to open connection: {e}")))
}
