use serde::Deserialize;

/// Configuration for the Vertex AI Vector Search sink.
///
/// Auth is a Google service-account JWT assertion — same mechanism
/// `nexus-connector-ga4` (this repo) already uses, just a different
/// scope (`cloud-platform` instead of `analytics.readonly`).
///
/// **Real prerequisite, not a v1 simplification**: the index must be
/// created in `STREAM_UPDATE` mode and deployed to an `IndexEndpoint`
/// (a separate resource, its own create+deploy step) before
/// `upsertDatapoints` works — same "index already exists" contract
/// every vector sink in this workspace has, just with a heavier real
/// prerequisite here.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct VertexVectorSearchConnectorConfig {
    pub project_id: String,
    /// GCP region, e.g. `us-central1` — also used to build the
    /// regional API host.
    pub region: String,
    /// Index resource ID (not the full resource name).
    pub index_id: String,
    /// Service account email — the JWT `iss` claim.
    pub client_email: String,
    /// PEM-encoded RSA private key from the service account JSON key
    /// file. Signs the JWT (RS256).
    pub private_key: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    /// Base URL for the Vertex AI API — field, not hardcoded, so
    /// tests can point it at a mock server. Defaults to the real
    /// regional host derived from `region`.
    #[serde(default)]
    pub api_base_url: Option<String>,
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl VertexVectorSearchConnectorConfig {
    pub(crate) fn base_url(&self) -> String {
        self.api_base_url
            .clone()
            .unwrap_or_else(|| format!("https://{}-aiplatform.googleapis.com", self.region))
    }

    pub(crate) fn index_url(&self) -> String {
        format!(
            "{}/v1/projects/{}/locations/{}/indexes/{}",
            self.base_url(),
            self.project_id,
            self.region,
            self.index_id
        )
    }
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
