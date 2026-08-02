use serde::Deserialize;

/// AI-Lake sink/source config. AI-Lake (github.com/ailake-io/ai-lakehouse) is
/// a self-contained Parquet+HNSW vector-native Lakehouse format: tabular
/// data, embeddings, and the vector index all live in one Iceberg-compatible
/// `.parquet` file. `warehouse` is a local filesystem root — AI-Lake's
/// `HadoopCatalog`+`LocalStore` backend, no server/container required (same
/// embedded shape as LanceDB in Marco 5). `namespace`/`table` address one
/// table within it.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct AilakeConnectorConfig {
    /// Local filesystem root for the AI-Lake warehouse — created if it
    /// doesn't exist yet, no server/container required.
    pub warehouse: String,
    /// Namespace (like a database/schema) within the warehouse.
    pub namespace: String,
    /// Table name within `namespace` — created automatically on first
    /// write if it doesn't exist yet.
    pub table: String,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to — indexed with HNSW automatically.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: u32,
}
