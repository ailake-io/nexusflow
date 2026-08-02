use serde::Deserialize;

/// AI Lakehouse sink #2 (ROADMAP.md Fase 5 order). See ARCHITECTURE.md
/// §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct QdrantConnectorConfig {
    /// Qdrant server address, e.g. `"http://localhost:6334"` (gRPC port).
    pub url: String,
    /// Name of an existing collection — must already be created (with the
    /// right vector size) on the Qdrant server; this sink only writes points.
    pub collection: String,
    /// Must be an `Int64` column — Qdrant point IDs are unsigned integers
    /// or UUIDs; arbitrary string keys aren't supported.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: usize,
}
