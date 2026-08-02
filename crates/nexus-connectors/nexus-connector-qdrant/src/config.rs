use serde::Deserialize;

/// AI Lakehouse sink #2 (ROADMAP.md Fase 5 order). See ARCHITECTURE.md
/// §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct QdrantConnectorConfig {
    pub url: String,
    pub collection: String,
    /// Must be an `Int64` column — Qdrant point IDs are unsigned integers
    /// or UUIDs; arbitrary string keys aren't supported.
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
}
