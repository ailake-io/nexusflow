use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is a standard OAuth2 refresh-token exchange — simpler than
/// GA4's JWT service-account flow, and the real mechanism this API
/// uses (confirmed via Google Ads API docs). A `developer-token`
/// header is also required on every API call — obtained by applying
/// for Google Ads API access on a manager (MCC) account; Explorer
/// tier auto-approves (enough to validate this connector), Basic/
/// Standard go through a review queue (confirmed real backlog as of
/// 2026, see README).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct GoogleAdsConnectorConfig {
    pub developer_token: String,
    /// Manager (MCC) account ID — the `login-customer-id` header.
    pub login_customer_id: String,
    /// Target customer (ad account) ID being queried.
    pub customer_id: String,
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    /// Raw GAQL query — no describe/catalog exists for this API to
    /// build a default from, so the user writes it directly (same
    /// spirit as `nexus-connector-salesforce`'s optional `soql`, but
    /// mandatory here).
    pub gaql_query: String,
    /// Dotted field paths matching the GAQL `SELECT` clause (e.g.
    /// `campaign.name`, `metrics.clicks`) — used both to build the
    /// Arrow schema and to flatten each nested JSON result row into
    /// columns, since `searchStream` returns deeply nested objects
    /// shaped by whatever resource/field paths were selected.
    pub fields: Vec<String>,
    /// API version path segment. Google deprecates versions on a
    /// rolling cadence — this needs bumping periodically, not a
    /// permanent default.
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    /// Base URL for the Ads API — field, not hardcoded, so tests can
    /// point it at a mock server.
    #[serde(default = "default_ads_api_base_url")]
    pub ads_api_base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_api_version() -> String {
    "v25".to_string()
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

fn default_ads_api_base_url() -> String {
    "https://googleads.googleapis.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
