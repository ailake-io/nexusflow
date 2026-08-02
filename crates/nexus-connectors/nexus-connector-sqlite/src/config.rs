use serde::Deserialize;

/// Static connector config resolved at node-configuration time. `uri` is a
/// file path or `:memory:` — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct SqliteConnectorConfig {
    pub uri: String,
    pub table: String,
    pub primary_key: String,
}
