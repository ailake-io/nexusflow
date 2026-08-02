use serde::Deserialize;

/// AI Lakehouse sink #4 (ROADMAP.md Fase 5 order — more complex to operate
/// than pgvector/qdrant/lancedb). The collection must already exist (created
/// externally with the right schema) — the sink only writes. See
/// ARCHITECTURE.md §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MilvusConnectorConfig {
    /// Milvus server address, e.g. `"http://localhost:19530"`.
    pub url: String,
    /// Name of an existing collection — must already be created (with
    /// schema and index) on the Milvus server; this sink only writes rows.
    pub collection: String,
    /// Must be an `Int64` column — matches the primary key type this
    /// connector supports (Milvus also allows `VarChar` primary keys, not
    /// implemented here).
    pub primary_key: String,
    /// Name of the vector field in the collection the embedding is
    /// written to.
    pub embedding_column: String,
    /// Must match the vector field's declared dimension in the collection
    /// schema.
    pub dimension: usize,
}
