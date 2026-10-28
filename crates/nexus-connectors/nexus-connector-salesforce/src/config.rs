use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is OAuth 2.0 JWT Bearer Flow — the server-to-server flow
/// Salesforce itself recommends for integrations with no UI/user
/// interaction, confirmed against Salesforce Help
/// (help.salesforce.com/s/articleView?id=remoteaccess_oauth_jwt_flow.htm,
/// 2026-08-19). Requires a Connected App with an uploaded X.509
/// certificate whose private key is `private_key` below.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct SalesforceConnectorConfig {
    /// OAuth token endpoint host. `https://login.salesforce.com` for
    /// production/Developer Edition orgs, `https://test.salesforce.com`
    /// for sandboxes, or an org's My Domain URL.
    #[serde(default = "default_login_url")]
    pub login_url: String,
    /// Connected App Consumer Key — the JWT `iss` claim.
    pub client_id: String,
    /// Integration user's username — the JWT `sub` claim.
    pub username: String,
    /// PEM-encoded RSA private key matching the certificate uploaded to
    /// the Connected App. Signs the JWT (RS256).
    pub private_key: String,
    /// REST API version, e.g. `v62.0`. Orgs can pin a different one.
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// sObject API name to read from / write to, e.g. `Account`.
    pub sobject: String,
    /// Custom SOQL for the source. When unset, the source builds
    /// `SELECT <fields> FROM <sobject>` from the sObject's own
    /// `describe()` field list — SOQL has no `SELECT *`.
    #[serde(default)]
    pub soql: Option<String>,
    /// Field used to upsert on write — required for the sink (Bulk API
    /// 2.0 upsert jobs need `externalIdFieldName`; v1 doesn't support a
    /// plain insert-only sink). Ignored by the source.
    #[serde(default)]
    pub external_id_field: Option<String>,
    /// Timeout in seconds for the whole connect+job+poll+download flow.
    /// Defaults to `300` — much higher than the ADBC connectors in this
    /// workspace, since Bulk API jobs are real asynchronous jobs that
    /// can take minutes, not a synchronous query.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Delay between job-status polls, in seconds.
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
}

impl SalesforceConnectorConfig {
    pub(crate) fn external_id_field_or_err(&self) -> Result<&str, nexus_core::NexusError> {
        self.external_id_field.as_deref().ok_or_else(|| {
            nexus_core::NexusError::Schema("salesforce sink requires external_id_field".into())
        })
    }
}

fn default_login_url() -> String {
    "https://login.salesforce.com".to_string()
}

fn default_api_version() -> String {
    "v62.0".to_string()
}

fn default_timeout_seconds() -> u64 {
    300
}

fn default_poll_interval_seconds() -> u64 {
    5
}
