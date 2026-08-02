use serde::Deserialize;

/// AI Lakehouse sink #3 (ROADMAP.md Fase 5 order). LanceDB is embedded —
/// `uri` is a local path (or object-store URI); no server to run. See
/// ARCHITECTURE.md §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct LanceDbConnectorConfig {
    pub uri: String,
    pub table: String,
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
}
