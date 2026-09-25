use serde::Deserialize;

/// Configuration for the Elasticsearch/OpenSearch vector sink.
///
/// The index must already exist with the right vector field mapping
/// (`dense_vector` for Elasticsearch, `knn_vector` for OpenSearch) —
/// this sink only writes rows, same "collection/index already
/// created" contract every vector sink in this repo/the public repo
/// (`nexus-connector-chromadb`/`pinecone`/`qdrant`) follows.
///
/// Confirmed real (2026-08-19): writing precomputed vectors into an
/// existing `dense_vector`/`knn_vector` field is Elasticsearch's
/// **Basic (free) tier** — what's gated behind Platinum/Enterprise is
/// Elastic's own automatic embedding generation (ELSER/semantic
/// search), not raw vector storage, which is all this sink does.
/// OpenSearch has no tier at all (Apache-2.0).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ElasticsearchConnectorConfig {
    /// Cluster endpoint(s) — the first entry is used as the base URL
    /// (e.g. `https://localhost:9200`).
    pub hosts: Vec<String>,
    /// Elasticsearch API key (`Authorization: ApiKey <key>`). Mutually
    /// exclusive with `username`/`password` in practice, but both are
    /// accepted — whichever is set is used.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Basic auth — works against both Elasticsearch and OpenSearch.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub index: String,
    /// Column used as the document `_id`.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to.
    pub embedding_column: String,
    /// Vector size — must match the index's configured
    /// `dense_vector`/`knn_vector` field dimension.
    pub dimension: usize,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl ElasticsearchConnectorConfig {
    pub(crate) fn base_url(&self) -> Result<&str, nexus_core::NexusError> {
        self.hosts
            .first()
            .map(String::as_str)
            .ok_or_else(|| nexus_core::NexusError::Schema("elasticsearch: hosts must not be empty".into()))
    }
}

fn default_timeout_seconds() -> u64 {
    30
}
