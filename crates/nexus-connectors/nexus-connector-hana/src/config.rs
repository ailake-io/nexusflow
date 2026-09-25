use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Connects via ODBC + the official SAP HANA Client ODBC driver
/// (`HDBODBC`) — no ADBC/pure-Rust HANA driver we can ship, same
/// story as Oracle (see README's "Config do conector SAP HANA"
/// section). v1 covers HANA (SQL) only — BAPI/IDoc via RFC is a
/// separate, much larger effort requiring SAP-licensed RFC SDKs that
/// can't be redistributed, out of scope here.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct HanaConnectorConfig {
    /// HANA server host.
    pub host: String,
    /// SQL port — no default, varies by instance number (typically
    /// `3<NN>15`); the caller supplies the real port directly.
    pub port: u16,
    /// Database name (`DATABASENAME`). Optional — single-container
    /// instances (common in dev/test) work without it.
    #[serde(default)]
    pub database: Option<String>,
    pub username: String,
    pub password: String,
    /// Table name, unquoted — HANA stores unquoted identifiers
    /// uppercase, same v1 assumption/limitation as the Oracle
    /// connector.
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// (`UPSERT ... WITH PRIMARY KEY` needs the PK in the column
    /// list). Ignored by the source.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for connecting and for each query/batch.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ODBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl HanaConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector("hana: host is required".to_string()));
        }
        if self.port == 0 {
            return Err(NexusError::Connector("hana: port is required".to_string()));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector(
                "hana: username is required".to_string(),
            ));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector(
                "hana: password is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "hana: table is required".to_string(),
            ));
        }
        if let Some(db) = self.database.as_deref() {
            if db.trim().is_empty() {
                return Err(NexusError::Connector(
                    "hana: database must not be empty when provided".to_string(),
                ));
            }
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "hana: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn primary_key_or_err(&self) -> Result<&str, NexusError> {
        self.primary_key
            .as_deref()
            .ok_or_else(|| NexusError::Schema("hana sink requires primary_key".into()))
    }
}

fn default_timeout_seconds() -> u64 {
    30
}
