use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresConnectorConfig {
    /// Full `postgresql://user:pass@host:port/db` URI. The role needs
    /// SELECT on the source table, or INSERT/UPDATE on the sink table.
    pub uri: String,
    /// Table name to read from (source) or write to (sink) — no schema
    /// prefix needed unless the table isn't in the connection's default
    /// `search_path`.
    pub table: String,
    /// Column used to partition reads by range and to upsert on write —
    /// must be an indexed, orderable column (integer/UUID/timestamp).
    pub primary_key: String,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — the
    /// driver is a blocking FFI call run via `spawn_blocking`, so a stalled
    /// connection would otherwise block that call forever (though the
    /// underlying blocking thread keeps running regardless — no
    /// cancellation for in-flight libpq/ADBC calls) (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}
