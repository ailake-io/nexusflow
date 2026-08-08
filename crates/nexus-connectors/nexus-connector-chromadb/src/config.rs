use serde::Deserialize;

/// AI Lakehouse sink #6 (ROADMAP.md Fase 5 order — last, most complex to
/// operate). Talks to ChromaDB's v2 REST API (`/api/v1` is deprecated).
/// Collection must already exist. See ARCHITECTURE.md §4.3/§8,
/// IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ChromaConnectorConfig {
    /// ChromaDB server address, e.g. `"http://localhost:8000"`.
    pub host: String,
    /// Tenant name — leave unset to use ChromaDB's default tenant.
    #[serde(default = "default_tenant")]
    pub tenant: String,
    /// Database name within the tenant — leave unset to use ChromaDB's
    /// default database.
    #[serde(default = "default_database")]
    pub database: String,
    /// Name of an existing collection — must already be created on the
    /// ChromaDB server; this sink only writes rows.
    pub collection: String,
    /// Column used as the Chroma document ID.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to.
    pub embedding_column: String,
    /// Vector size — must match the collection's configured dimension.
    pub dimension: usize,
    /// Per-request timeout in seconds — `reqwest::Client` has no timeout by
    /// default, so a stalled connection to ChromaDB would otherwise block
    /// the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_tenant() -> String {
    "default_tenant".to_string()
}

fn default_database() -> String {
    "default_database".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
