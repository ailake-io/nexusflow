use serde::Deserialize;

/// pgvector sink config — the AI Lakehouse destination for Marco 5
/// (chunk → embedding → pgvector). See ARCHITECTURE.md §4.3/§8.
#[derive(Debug, Clone, Deserialize)]
pub struct PgVectorConnectorConfig {
    pub uri: String,
    pub table: String,
    pub primary_key: String,
    /// Name of the `vector(N)` column the embedding is written to.
    pub embedding_column: String,
    /// Must match the `vector(N)` column's declared width.
    pub dimension: usize,
}
