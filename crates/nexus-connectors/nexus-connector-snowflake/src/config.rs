use nexus_core::NexusError;
use serde::Deserialize;

/// How to authenticate to Snowflake. Option key names below are the real
/// ones the official ADBC driver exposes — confirmed by reading
/// `adbc_driver_snowflake/__init__.py`'s `DatabaseOptions`/`AuthType`
/// enums straight out of the `adbc-driver-snowflake` PyPI wheel
/// (2026-08-18), not guessed.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum SnowflakeAuth {
    /// `adbc.snowflake.sql.auth_type = "auth_snowflake"` — plain username/
    /// password. Simplest option; Snowflake recommends key-pair auth for
    /// automation/service accounts instead (see `KeyPair`).
    Password { username: String, password: String },
    /// `adbc.snowflake.sql.auth_type = "auth_jwt"` — RSA key-pair auth,
    /// Snowflake's recommended method for service accounts. `private_key`
    /// is the PKCS#8 DER-encoded private key, base64-encoded (maps to the
    /// driver's `client_option.jwt_private_key_pkcs8_value` option — no
    /// file path needed, unlike the file-based `jwt_private_key` option
    /// this connector doesn't use).
    KeyPair {
        username: String,
        /// Base64-encoded PKCS#8 DER private key.
        private_key: String,
        /// Passphrase for an encrypted private key, if any.
        #[serde(default)]
        private_key_password: Option<String>,
    },
}

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct SnowflakeConnectorConfig {
    /// Account identifier (e.g. `xy12345.us-east-1` or
    /// `orgname-accountname`) — the same value used in the Snowflake web
    /// UI's account URL.
    pub account: String,
    /// Warehouse to run queries on.
    pub warehouse: String,
    /// Database name.
    pub database: String,
    /// Schema name.
    pub schema: String,
    /// Role to assume for the session. Optional — falls back to the
    /// user's default role when unset.
    #[serde(default)]
    pub role: Option<String>,
    pub auth: SnowflakeAuth,
    /// Table to read from (source) or write to (sink).
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// side; ignored by the source. Upserts use `MERGE INTO` (Snowflake
    /// has no `ON CONFLICT`).
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for connecting and for each query. Defaults to
    /// `60` — Snowflake queries (especially first-run against a
    /// suspended warehouse, which auto-resumes) routinely take longer
    /// than the `30`s default most other connectors in this workspace
    /// use.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ADBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl SnowflakeConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.account.trim().is_empty() {
            return Err(NexusError::Connector("snowflake: account is required".to_string()));
        }
        if self.warehouse.trim().is_empty() {
            return Err(NexusError::Connector(
                "snowflake: warehouse is required".to_string(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(NexusError::Connector(
                "snowflake: database is required".to_string(),
            ));
        }
        if self.schema.trim().is_empty() {
            return Err(NexusError::Connector("snowflake: schema is required".to_string()));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector("snowflake: table is required".to_string()));
        }
        match &self.auth {
            SnowflakeAuth::Password { username, password } => {
                if username.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "snowflake: username is required for password auth".to_string(),
                    ));
                }
                if password.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "snowflake: password is required for password auth".to_string(),
                    ));
                }
            }
            SnowflakeAuth::KeyPair {
                username,
                private_key,
                ..
            } => {
                if username.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "snowflake: username is required for key-pair auth".to_string(),
                    ));
                }
                if private_key.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "snowflake: private_key is required for key-pair auth".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn default_timeout_seconds() -> u64 {
    60
}
