use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Connects via ODBC + the official Oracle Instant Client ODBC driver
/// (no ADBC Oracle driver we can ship — see README's "Config do
/// conector Oracle" section for why). v1 only supports Service Name
/// (not SID) and a plain unquoted (uppercase-stored) table name.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct OracleConnectorConfig {
    /// Oracle Listener host.
    pub host: String,
    /// Oracle Listener port. Defaults to `1521`.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Oracle Service Name (not SID — v1 doesn't support SID-style
    /// connections).
    pub service_name: String,
    pub username: String,
    pub password: String,
    /// Table name, unquoted — Oracle stores unquoted identifiers
    /// uppercase, and `describe_table` looks it up as
    /// `UPPER(table_name)`. A table created with a quoted lowercase
    /// name won't be found; known v1 limitation.
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// (`MERGE INTO`). Ignored by the source.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for connecting and for each query/batch.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ODBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl OracleConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector(
                "oracle: host is required".to_string(),
            ));
        }
        if self.port == 0 {
            return Err(NexusError::Connector(
                "oracle: port is required".to_string(),
            ));
        }
        if self.service_name.trim().is_empty() {
            return Err(NexusError::Connector(
                "oracle: service_name is required".to_string(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector(
                "oracle: username is required".to_string(),
            ));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector(
                "oracle: password is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "oracle: table is required".to_string(),
            ));
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "oracle: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn primary_key_or_err(&self) -> Result<&str, NexusError> {
        self.primary_key
            .as_deref()
            .ok_or_else(|| NexusError::Schema("oracle sink requires primary_key".into()))
    }
}

fn default_port() -> u16 {
    1521
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Config for the LogMiner-based CDC source (`oracle-cdc`). Same
/// connection fields as `OracleConnectorConfig` minus `primary_key`
/// (not needed — CDC is source-only), plus `poll_interval_seconds`.
///
/// `SEG_OWNER` for the `V$LOGMNR_CONTENTS` filter is derived from
/// `username.to_uppercase()` (the connecting user's own schema) —
/// same simplification `describe_table` (`schema.rs`) already makes
/// for `ALL_TAB_COLUMNS` (no separate schema/owner field in v1); a
/// service account whose table lives in a *different* schema than its
/// own login schema isn't supported yet.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct OracleCdcConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub service_name: String,
    pub username: String,
    pub password: String,
    pub table: String,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Caps how many change rows accumulate before the poll loop ends
    /// and lets `nexus-server`'s scheduler re-invoke — same role
    /// `mssql-cdc`/`mysql-cdc`'s `max_batch_events` play. Real fix, not
    /// cosmetic: without a cutoff, `cdc.rs`'s poll loop ran forever
    /// inside a single `spawn_blocking` call, and the checkpoint commit
    /// that only fires when a source's stream ends (`PipelineEngine::
    /// run_partition`) never got a chance to run.
    #[serde(default = "default_max_batch_events")]
    pub max_batch_events: u64,
    /// Resume position, an Oracle SCN — same format
    /// `Source::position_handle` reports back after this connector's
    /// stream ends. When set, `connect()` starts here instead of the
    /// current `V$DATABASE.CURRENT_SCN` (which would silently skip
    /// every change between a prior run's end and this one's start).
    /// Injected automatically by `nexus-server`'s `run_passthrough_pipeline`
    /// from the last committed checkpoint.
    #[serde(default)]
    pub start_scn: Option<i64>,
}

fn default_poll_interval_seconds() -> u64 {
    5
}

fn default_max_batch_events() -> u64 {
    1000
}
