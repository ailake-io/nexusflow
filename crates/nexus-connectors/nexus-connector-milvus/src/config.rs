use serde::Deserialize;

/// AI Lakehouse sink #4 (ROADMAP.md Fase 5 order — more complex to operate
/// than pgvector/qdrant/lancedb). The collection must already exist (created
/// externally with the right schema) — the sink only writes. See
/// ARCHITECTURE.md §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize)]
pub struct MilvusConnectorConfig {
    pub url: String,
    pub collection: String,
    /// Must be an `Int64` column — matches the primary key type this
    /// connector supports (Milvus also allows `VarChar` primary keys, not
    /// implemented here).
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
}
