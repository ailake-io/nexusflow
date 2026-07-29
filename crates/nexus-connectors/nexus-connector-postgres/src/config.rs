use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConnectorConfig {
    /// Full `postgresql://user:pass@host:port/db` URI.
    pub uri: String,
    pub table: String,
    pub primary_key: String,
}
