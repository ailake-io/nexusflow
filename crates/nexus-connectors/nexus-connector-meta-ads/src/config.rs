use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is a **System User access token** (generated manually in Meta
/// Business Manager) — confirmed real mechanism for unattended
/// server-to-server access, doesn't expire on a timer the way regular
/// OAuth user tokens do (1-2h short-lived, 60-day long-lived). No
/// OAuth flow needed at runtime, same simplicity Shopify/Stripe have:
/// the token is generated once outside this connector and handed in
/// via config, revoked only manually.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MetaAdsConnectorConfig {
    /// Ad account ID, without the `act_` prefix — this connector adds
    /// it when building the URL.
    pub ad_account_id: String,
    pub access_token: String,
    /// Insights fields to request (e.g. `impressions`, `clicks`,
    /// `spend`).
    pub fields: Vec<String>,
    /// Aggregation level.
    pub level: MetaAdsLevel,
    /// Optional breakdown dimensions (e.g. `age`, `gender`,
    /// `placement`).
    #[serde(default)]
    pub breakdowns: Vec<String>,
    /// Relative date preset (e.g. `last_7d`) — takes precedence over
    /// `since`/`until` when set.
    #[serde(default)]
    pub date_preset: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub until: Option<String>,
    /// Graph API version path segment. Meta retires each version
    /// roughly 2 years after release — needs periodic bumping.
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Base URL for the Graph API — field, not hardcoded, so tests can
    /// point it at a mock server.
    #[serde(default = "default_graph_api_base_url")]
    pub graph_api_base_url: String,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    /// Timeout for the whole connect+job+poll+download flow. Defaults
    /// high — Meta's own async insights jobs can queue behind other
    /// work and time out at 60 minutes on Meta's side, same "job can
    /// genuinely take a while" reasoning `nexus-connector-salesforce`
    /// documents for its own Bulk API jobs.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetaAdsLevel {
    Ad,
    Adset,
    Campaign,
    Account,
}

impl MetaAdsLevel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            MetaAdsLevel::Ad => "ad",
            MetaAdsLevel::Adset => "adset",
            MetaAdsLevel::Campaign => "campaign",
            MetaAdsLevel::Account => "account",
        }
    }
}

fn default_api_version() -> String {
    "v23.0".to_string()
}

fn default_graph_api_base_url() -> String {
    "https://graph.facebook.com".to_string()
}

fn default_poll_interval_seconds() -> u64 {
    5
}

fn default_timeout_seconds() -> u64 {
    3600
}
