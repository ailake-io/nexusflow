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
}
