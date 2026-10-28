use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth: `access_token` obtained externally via TikTok's one-time
/// OAuth2 authorization-code consent flow — confirmed the token is
/// long-lived with no refresh cycle needed for server-to-server use,
/// same practical shape as `nexus-connector-meta-ads`'s System User
/// token (this repo). App review is required to get Marketing API
/// access but is self-serve/timeboxed (2-7 business days, confirmed
/// via research), not a discretionary business gate like LinkedIn
/// Ads' MDP approval.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct TiktokAdsConnectorConfig {
    pub access_token: String,
    pub advertiser_id: String,
    /// Report type, e.g. `BASIC`.
    pub report_type: String,
    /// Aggregation level, e.g. `AUCTION_CAMPAIGN`, `AUCTION_AD`.
    pub data_level: String,
    /// Dimension field names (e.g. `campaign_id`, `stat_time_day`).
    pub dimensions: Vec<String>,
    /// Metric field names (e.g. `impressions`, `clicks`, `spend`).
    pub metrics: Vec<String>,
    /// `YYYY-MM-DD`.
    pub start_date: String,
    pub end_date: String,
    /// Rows per page — real page-based pagination (confirmed via the
    /// official SDK docs), not offset/cursor.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// Base URL for the Business API — field, not hardcoded, so tests
    /// can point it at a mock server.
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_page_size() -> u32 {
    200
}

fn default_api_base_url() -> String {
    "https://business-api.tiktok.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
