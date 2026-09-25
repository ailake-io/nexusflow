use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Shared by `mssql` and `synapse` (Azure Synapse dedicated SQL pool
/// speaks the same TDS/T-SQL protocol, same driver — see `lib.rs`).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MssqlConnectorConfig {
    /// Server host — for Synapse, the workspace's dedicated SQL
    /// endpoint (`<workspace>.sql.azuresynapse.net`).
    pub host: String,
    /// Port. Defaults to `1433`, SQL Server's (and Synapse's) default.
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    /// Table name to read from (source) or write to (sink).
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// side; ignored by the source. Upserts use `MERGE INTO` (real
    /// T-SQL, GA on Synapse dedicated SQL pools too — see README for
    /// Synapse-specific `MERGE` limitations).
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for each ADBC call.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ADBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl MssqlConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector("mssql: host is required".to_string()));
        }
        if self.database.trim().is_empty() {
            return Err(NexusError::Connector("mssql: database is required".to_string()));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector("mssql: username is required".to_string()));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector("mssql: password is required".to_string()));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector("mssql: table is required".to_string()));
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "mssql: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn default_port() -> u16 {
    1433
}

fn default_timeout_seconds() -> u64 {
    30
}

impl MssqlConnectorConfig {
    /// Returns the `mssql://` connection URI — real, documented format
    /// confirmed against the ADBC Driver Foundry's own docs
    /// (docs.adbc-drivers.org/drivers/mssql/, 2026-08-19), not guessed.
    pub fn connection_string(&self) -> String {
        format!(
            "mssql://{}:{}@{}:{}?database={}",
            percent_encode(&self.username),
            percent_encode(&self.password),
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        )
    }
}

/// Native CDC source config — SQL Server only (Synapse dedicated SQL
/// pools have no `cdc.fn_cdc_get_all_changes_*` equivalent, confirmed
/// via research; the ADF-based "CDC into Synapse" pattern just reads a
/// source's own CDC and writes to Synapse as a destination — exactly
/// what `mssql-cdc` (source) + `synapse` (sink) already do together in
/// a normal NexusFlow pipeline, no separate integration needed).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MssqlCdcConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub table: String,
    /// CDC capture instance name. Defaults to `dbo_<table>` — SQL
    /// Server's own naming convention when `sys.sp_cdc_enable_table`
    /// is called without an explicit `@capture_instance` and the table
    /// lives in the `dbo` schema. A table in a different schema, or
    /// enabled with a custom capture instance name, needs this set
    /// explicitly — documented v1 assumption, not silently wrong.
    #[serde(default)]
    pub capture_instance: Option<String>,
    /// Delay between LSN polls, in seconds.
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    /// Caps how many change rows accumulate into one `RecordBatch`
    /// before it's flushed — same role `mysql-cdc`'s `max_batch_events`
    /// plays (public repo).
    #[serde(default = "default_max_batch_events")]
    pub max_batch_events: u64,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Resume position, as an uppercase hex-encoded LSN with a leading
    /// `0x` (e.g. `0x0000002B000018D80003`) — same format
    /// `Source::position_handle` reports back after this connector's
    /// stream ends. When set, `connect()` starts here instead of calling
    /// `sys.fn_cdc_get_min_lsn` for the earliest available LSN. Injected
    /// automatically by `nexus-server`'s `run_passthrough_pipeline` from
    /// the last committed checkpoint — not meant to be hand-typed in the
    /// UI on every run, though nothing stops a manual override for a
    /// deliberate replay-from-here.
    #[serde(default)]
    pub start_lsn: Option<String>,
}

impl MssqlCdcConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "mssql://{}:{}@{}:{}?database={}",
            percent_encode(&self.username),
            percent_encode(&self.password),
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        )
    }

    pub fn capture_instance(&self) -> String {
        self.capture_instance
            .clone()
            .unwrap_or_else(|| format!("dbo_{}", self.table))
    }
}

fn default_poll_interval_seconds() -> u64 {
    5
}

fn default_max_batch_events() -> u64 {
    500
}

/// Minimal percent-encoding helper for connection-string components —
/// same approach `nexus-connector-redshift`'s `config.rs` (this repo)
/// and `nexus-connector-postgres`'s `config.rs` (public repo) use.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '%' | '@' | ':' | '/' | '?' | '#' | '[' | ']' | ' ' => {
                for b in c.encode_utf8(&mut [0; 4]).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_instance_defaults_to_dbo_prefixed_table() {
        let cfg = MssqlCdcConfig {
            host: "h".into(),
            port: 1433,
            database: "d".into(),
            username: "u".into(),
            password: "p".into(),
            table: "Orders".into(),
            capture_instance: None,
            poll_interval_seconds: 5,
            max_batch_events: 500,
            timeout_seconds: 30,
            start_lsn: None,
        };
        assert_eq!(cfg.capture_instance(), "dbo_Orders");
    }

    #[test]
    fn capture_instance_respects_explicit_override() {
        let mut cfg = MssqlCdcConfig {
            host: "h".into(),
            port: 1433,
            database: "d".into(),
            username: "u".into(),
            password: "p".into(),
            table: "Orders".into(),
            capture_instance: None,
            poll_interval_seconds: 5,
            max_batch_events: 500,
            timeout_seconds: 30,
            start_lsn: None,
        };
        cfg.capture_instance = Some("sales_Orders".into());
        assert_eq!(cfg.capture_instance(), "sales_Orders");
    }
}
