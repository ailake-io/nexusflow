use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is **not** a pure service-account JWT flow like
/// `nexus-connector-ga4` — confirmed the YouTube Analytics API
/// requires OAuth2 user consent from the channel owner (`yt-
/// analytics.readonly` scope), no service-account-only path. Same
/// practical pattern `nexus-connector-linkedin-ads` already uses:
/// `access_token` obtained externally, with optional
/// `refresh_token`/`client_id`/`client_secret` enabling real
/// automatic renewal via the standard OAuth2 refresh grant — unlike
/// LinkedIn, there's no partner-approval gate blocking that refresh
/// from working.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct YoutubeAnalyticsConnectorConfig {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    /// Target channel — `channel==MINE` (the authenticated user's own
    /// channel) or `channel==CHANNEL_ID`.
    #[serde(default = "default_ids")]
    pub ids: String,
    pub dimensions: Vec<String>,
    pub metrics: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub filters: Option<String>,
    /// Rows per page — real index-based pagination
    /// (`startIndex`/`maxResults`), confirmed via the API reference.
    #[serde(default = "default_max_results")]
    pub max_results: u32,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    /// Base URL for the Analytics API — field, not hardcoded, so
    /// tests can point it at a mock server.
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_ids() -> String {
    "channel==MINE".to_string()
}

fn default_max_results() -> u32 {
    200
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

fn default_api_base_url() -> String {
    "https://youtubeanalytics.googleapis.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
