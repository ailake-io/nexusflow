use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is a Google service-account JWT assertion (server-to-server,
/// no domain-wide delegation / user impersonation) — confirmed real
/// flow, same shape as `nexus-connector-salesforce`'s JWT Bearer Flow
/// (this repo) but with a `scope` claim instead of `sub`, matching
/// Google's own OAuth2 service-account JWT format.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct Ga4ConnectorConfig {
    /// GA4 property ID (numeric, no `properties/` prefix).
    pub property_id: String,
    /// Service account email — the JWT `iss` claim.
    pub client_email: String,
    /// PEM-encoded RSA private key from the service account JSON key
    /// file. Signs the JWT (RS256).
    pub private_key: String,
    /// Google's token endpoint. Field, not hardcoded, so a test double
    /// can override it — same reasoning `nexus-connector-salesforce`'s
    /// `login_url` documents.
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    /// Base URL for the Data API — field, not hardcoded, so tests can
    /// point it at a mock server the same way `token_uri` does.
    #[serde(default = "default_data_api_base_url")]
    pub data_api_base_url: String,
    /// Dimension names to request (e.g. `date`, `country`).
    pub dimensions: Vec<String>,
    /// Metric names to request (e.g. `activeUsers`, `sessions`).
    pub metrics: Vec<String>,
    /// Report date range start — GA4 accepts both literal dates
    /// (`2026-01-01`) and relative strings (`7daysAgo`, `today`).
    pub start_date: String,
    pub end_date: String,
    /// Rows requested per `runReport` call — GA4 paginates via
    /// `offset`/`limit`, not a cursor. Defaults to `100000`, comfortably
    /// under the API's own per-request row ceiling.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// Timeout in seconds for each HTTP call.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient HTTP failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl Ga4ConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.property_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "ga4: property_id is required".to_string(),
            ));
        }
        if self.client_email.trim().is_empty() {
            return Err(NexusError::Connector(
                "ga4: client_email is required".to_string(),
            ));
        }
        if self.private_key.trim().is_empty() {
            return Err(NexusError::Connector(
                "ga4: private_key is required".to_string(),
            ));
        }
        if self.dimensions.is_empty() {
            return Err(NexusError::Connector(
                "ga4: at least one dimension is required".to_string(),
            ));
        }
        if self.metrics.is_empty() {
            return Err(NexusError::Connector(
                "ga4: at least one metric is required".to_string(),
            ));
        }
        if self.start_date.trim().is_empty() {
            return Err(NexusError::Connector(
                "ga4: start_date is required".to_string(),
            ));
        }
        if self.end_date.trim().is_empty() {
            return Err(NexusError::Connector(
                "ga4: end_date is required".to_string(),
            ));
        }
        if self.page_size == 0 {
            return Err(NexusError::Connector(
                "ga4: page_size must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

fn default_data_api_base_url() -> String {
    "https://analyticsdata.googleapis.com".to_string()
}

fn default_page_size() -> u32 {
    100_000
}

fn default_timeout_seconds() -> u64 {
    30
}
