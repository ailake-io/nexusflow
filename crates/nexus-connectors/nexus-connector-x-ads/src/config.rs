use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is OAuth 1.0a (`src/oauth1.rs`) — the 4 credentials below are all
/// generated once outside this connector (X's developer portal, an
/// approved Ads API app under Standard/Conversion-Only Access), same
/// "obtained externally, handed in via config" model
/// `nexus-connector-linkedin-ads` already documents for its own
/// externally-obtained access token.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct XAdsConnectorConfig {
    pub consumer_key: String,
    pub consumer_secret: String,
    pub access_token: String,
    pub access_token_secret: String,
    /// Ads account ID (not the same as a regular X user id).
    pub account_id: String,
    /// Entity type the stats are scoped to.
    pub entity: XAdsEntity,
    /// Up to 20 entity ids of the type above — confirmed API limit.
    pub entity_ids: Vec<String>,
    /// Metric groups to request (e.g. `engagement`, `billing`).
    pub metric_groups: Vec<String>,
    #[serde(default)]
    pub granularity: XAdsGranularity,
    /// ISO 8601, on-the-hour (e.g. `2026-08-01T00:00:00Z`) — confirmed
    /// API requirement.
    pub start_time: String,
    pub end_time: String,
    /// Restrict to a specific placement (e.g. `ALL_ON_TWITTER`).
    #[serde(default)]
    pub placement: Option<String>,
    /// Metric field names to project into the output schema — every
    /// column comes back as `Utf8`, same convention this workspace's
    /// other report connectors (Meta/Google/TikTok Ads, GA4) use, since
    /// the API returns even numeric metrics inside a nested JSON shape
    /// that isn't uniformly typed across metric groups.
    pub fields: Vec<String>,
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    /// Timeout for the whole create-job+poll+download flow. Defaults
    /// high, same reasoning `nexus-connector-meta-ads` documents for its
    /// own async Insights jobs — X Ads doesn't document its own job
    /// timeout, so this keeps the same conservative ceiling.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum XAdsEntity {
    Account,
    Campaign,
    LineItem,
    PromotedTweet,
    Media,
}

impl XAdsEntity {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            XAdsEntity::Account => "ACCOUNT",
            XAdsEntity::Campaign => "CAMPAIGN",
            XAdsEntity::LineItem => "LINE_ITEM",
            XAdsEntity::PromotedTweet => "PROMOTED_TWEET",
            XAdsEntity::Media => "MEDIA",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum XAdsGranularity {
    #[default]
    Day,
    Hour,
    Total,
}

impl XAdsGranularity {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            XAdsGranularity::Day => "DAY",
            XAdsGranularity::Hour => "HOUR",
            XAdsGranularity::Total => "TOTAL",
        }
    }
}

fn default_api_base_url() -> String {
    "https://ads-api.x.com".to_string()
}

fn default_poll_interval_seconds() -> u64 {
    5
}

fn default_timeout_seconds() -> u64 {
    3600
}
