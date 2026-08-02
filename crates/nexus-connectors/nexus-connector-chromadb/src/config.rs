use serde::Deserialize;

/// AI Lakehouse sink #6 (ROADMAP.md Fase 5 order — last, most complex to
/// operate). Talks to ChromaDB's v2 REST API (`/api/v1` is deprecated).
/// Collection must already exist. See ARCHITECTURE.md §4.3/§8,
/// IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ChromaConnectorConfig {
    pub host: String,
    #[serde(default = "default_tenant")]
    pub tenant: String,
    #[serde(default = "default_database")]
    pub database: String,
    pub collection: String,
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
}

fn default_tenant() -> String {
    "default_tenant".to_string()
}

fn default_database() -> String {
    "default_database".to_string()
}
