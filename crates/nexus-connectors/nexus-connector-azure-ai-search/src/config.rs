use serde::Deserialize;

/// Configuration for the Azure AI Search vector sink.
///
/// The index must already exist with the vector field pre-declared as
/// `Collection(Edm.Single)` with `dimensions`+`vectorSearchProfile`
/// set — this connector only writes documents, same "index/collection
/// already exists" contract every vector sink in this workspace
/// follows. Auth is the static `api-key` header (admin key), no OAuth
/// — simpler than the GA4/Vertex JWT flow.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct AzureAiSearchConnectorConfig {
    /// Search service endpoint, e.g.
    /// `https://<service-name>.search.windows.net`.
    pub endpoint: String,
    pub api_key: String,
    pub index_name: String,
    /// Column mapped to the index's key field — the key field's *name*
    /// in the index schema must match this column's name, since Azure
    /// AI Search documents are plain JSON keyed by field name (no
    /// separate `id`/`class` envelope like Weaviate).
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_api_version() -> String {
    "2024-07-01".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
