use serde::Deserialize;

/// pgvector sink config — the AI Lakehouse destination for Marco 5
/// (chunk → embedding → pgvector). See ARCHITECTURE.md §4.3/§8.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PgVectorConnectorConfig {
    /// Full `postgresql://user:pass@host:port/db` URI — the `pgvector`
    /// extension must already be enabled on this database
    /// (`CREATE EXTENSION vector`).
    pub uri: String,
    /// Table name — must already exist with a `vector(N)` column matching
    /// `embedding_column`/`dimension`; this sink only writes rows.
    pub table: String,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `vector(N)` column the embedding is written to.
    pub embedding_column: String,
    /// Must match the `vector(N)` column's declared width.
    pub dimension: usize,
}
