use serde::Deserialize;

/// AI Lakehouse sink #3 (ROADMAP.md Fase 5 order). LanceDB is embedded —
/// `uri` is a local path (or object-store URI); no server to run. See
/// ARCHITECTURE.md §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct LanceDbConnectorConfig {
    /// Local directory path (or object-store URI) where LanceDB stores its
    /// data — created if it doesn't exist yet, no server to run.
    pub uri: String,
    /// Table name within `uri` — created automatically on first write if
    /// it doesn't exist yet.
    pub table: String,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: usize,
}
