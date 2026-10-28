use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// **Real limitation, documented in
/// `docs/PLANNED_CONNECTORS.md`, not hidden here**: programmatic
/// refresh tokens for the LinkedIn Marketing API require Marketing
/// Developer Platform (MDP) partner approval (discretionary, 4 weeks
/// best case / 4 months average). Without MDP status, `access_token`
/// is a token the operator obtained externally (LinkedIn's 3-legged
/// OAuth flow needs a browser redirect, not something this connector
/// can drive at runtime) and it simply **expires after 60 days with
/// no automatic renewal** — the operator has to manually mint a new
/// one and update the config. `refresh_token`/`client_id`/
/// `client_secret` are optional and only useful for MDP-approved
/// apps, which get a 365-day refresh token that can programmatically
/// mint new 60-day access tokens.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct LinkedinAdsConnectorConfig {
    /// Externally obtained OAuth2 access token. Expires in 60 days
    /// with no automatic renewal unless `refresh_token` (MDP-only) is
    /// also set.
    pub access_token: String,
    /// MDP-partner-only: enables automatic renewal via the standard
    /// OAuth2 refresh-token exchange.
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    /// Sponsored ad account URN(s), e.g.
    /// `urn:li:sponsoredAccount:123456`.
    pub account_urns: Vec<String>,
    /// Analytics pivot — grouping dimension (e.g. `CAMPAIGN`,
    /// `CREATIVE`, `MEMBER_COMPANY`). Confirmed real query param.
    pub pivot: String,
    /// Fields to request (e.g. `impressions`, `clicks`,
    /// `costInLocalCurrency`).
    pub fields: Vec<String>,
    /// Report date range — `(year, month, day)`.
    pub start_date: (u16, u8, u8),
    pub end_date: (u16, u8, u8),
    /// `q=analytics` (1 pivot) or `q=statistics` (up to 3 pivots) —
    /// confirmed real finder methods. Defaults to `analytics`.
    #[serde(default = "default_finder")]
    pub finder: String,
    /// LinkedIn REST API version header (`LinkedIn-Version`), e.g.
    /// `202605` — versioned REST APIs need this on every call.
    #[serde(default = "default_linkedin_version")]
    pub linkedin_version: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
    /// Base URL for the REST API — field, not hardcoded, so tests can
    /// point it at a mock server.
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_finder() -> String {
    "analytics".to_string()
}

fn default_linkedin_version() -> String {
    "202605".to_string()
}

fn default_token_uri() -> String {
    "https://www.linkedin.com/oauth/v2/accessToken".to_string()
}

fn default_api_base_url() -> String {
    "https://api.linkedin.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}
