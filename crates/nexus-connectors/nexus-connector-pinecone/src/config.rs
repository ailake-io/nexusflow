use serde::Deserialize;

/// AI Lakehouse sink #5 (ROADMAP.md Fase 5 order). Pinecone has no
/// self-hosted/Docker option — this connector talks to the real managed
/// service's data-plane REST API, so it's only mockable via `wiremock`
/// (no real end-to-end integration test, unlike the other 5 vector sinks —
/// user decision 2026-07-31). See ARCHITECTURE.md §4.3/§8,
/// IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize)]
pub struct PineconeConnectorConfig {
    /// Index-specific data-plane host, e.g.
    /// `https://my-index-xxxx.svc.us-east1-aws.pinecone.io` (from Pinecone's
    /// `describe_index` control-plane response — not built from `index` +
    /// `environment` here, that addressing scheme is deprecated).
    pub host: String,
    pub api_key: String,
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
    #[serde(default)]
    pub namespace: Option<String>,
}
