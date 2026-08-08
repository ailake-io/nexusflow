use serde::Deserialize;

/// Static connector config resolved at node-configuration time. `uri` is a
/// file path or `:memory:` — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct SqliteConnectorConfig {
    /// File path to the `.db` file, or `:memory:` for an ephemeral database
    /// that only exists for this process's lifetime.
    pub uri: String,
    /// Table name to read from (source) or write to (sink) — created
    /// automatically on the sink side if it doesn't exist yet.
    pub table: String,
    /// Column used to upsert on write — should be an indexed, unique column
    /// (integer or text primary key).
    pub primary_key: String,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — a
    /// concurrent writer holding the SQLite file lock can otherwise stall a
    /// call indefinitely (though the underlying blocking thread keeps
    /// running regardless — no cancellation for in-flight ADBC calls) (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}
