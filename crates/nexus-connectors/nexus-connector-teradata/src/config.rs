use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Connects via ODBC + the official Teradata Database ODBC Driver
/// (part of Teradata Tools and Utilities / TTU) — no ADBC/pure-Rust
/// Teradata driver we can ship, same story as Oracle/HANA (this
/// repo).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct TeradataConnectorConfig {
    /// Teradata system name or IP (`DBCName`).
    pub host: String,
    /// TCP port. Most Teradata ODBC driver versions resolve the actual
    /// service port themselves via `DBCName` alone (default service is
    /// 1025) — kept explicit anyway for the same "no silent driver
    /// magic" reason every other connector in this repo requires a
    /// port, but some driver versions may ignore it in favor of
    /// `DBCName`'s own service-discovery. **Not confirmed against a
    /// real installation in this session** — same honesty flag the
    /// Oracle/HANA connectors' `driver.rs` carry for their own
    /// connection-string attributes.
    pub port: u16,
    /// Default database (`DATABASE=` ODBC option) — Teradata has no
    /// separate "schema" concept, `database` here doubles as both.
    #[serde(default)]
    pub database: Option<String>,
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

impl TeradataConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector(
                "teradata: host is required".to_string(),
            ));
        }
        if self.port == 0 {
            return Err(NexusError::Connector(
                "teradata: port is required".to_string(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector(
                "teradata: username is required".to_string(),
            ));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector(
                "teradata: password is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "teradata: table is required".to_string(),
            ));
        }
        if let Some(db) = self.database.as_deref() {
            if db.trim().is_empty() {
                return Err(NexusError::Connector(
                    "teradata: database must not be empty when provided".to_string(),
                ));
            }
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "teradata: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn primary_key_or_err(&self) -> Result<&str, NexusError> {
        self.primary_key
            .as_deref()
            .ok_or_else(|| NexusError::Schema("teradata sink requires primary_key".into()))
    }
}

fn default_timeout_seconds() -> u64 {
    30
}
