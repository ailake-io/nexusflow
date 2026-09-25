use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Connects via ODBC + the official Vertica ODBC Driver — no ADBC/
/// pure-Rust Vertica driver we can ship, same story as
/// Oracle/HANA/Teradata (this repo).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct VerticaConnectorConfig {
    pub host: String,
    /// TCP port — Vertica's default is 5433, no default assumed here
    /// (every other ODBC connector in this repo requires it
    /// explicit too).
    pub port: u16,
    /// Database name (`Database=` ODBC option) — required, unlike
    /// HANA/Teradata's optional default database.
    pub database: String,
    pub username: String,
    pub password: String,
    /// Table name, unquoted.
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink.
    /// Ignored by the source.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for connecting and for each query/batch.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ODBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl VerticaConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector(
                "vertica: host is required".to_string(),
            ));
        }
        if self.port == 0 {
            return Err(NexusError::Connector(
                "vertica: port is required".to_string(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(NexusError::Connector(
                "vertica: database is required".to_string(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector(
                "vertica: username is required".to_string(),
            ));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector(
                "vertica: password is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "vertica: table is required".to_string(),
            ));
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "vertica: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn primary_key_or_err(&self) -> Result<&str, NexusError> {
        self.primary_key
            .as_deref()
            .ok_or_else(|| NexusError::Schema("vertica sink requires primary_key".into()))
    }
}

fn default_timeout_seconds() -> u64 {
    30
}
