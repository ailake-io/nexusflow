use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Reads via SuiteQL (`/services/rest/query/v1/suiteql`, NetSuite's
/// real SQL-like query endpoint over REST — chosen over the older
/// SOAP-based SuiteTalk, which is a much larger, separate protocol
/// surface, same "REST over SOAP where a real REST alternative
/// exists" precedent `nexus-connector-servicenow`'s Table API over
/// SOAP web services follows). Writes via the REST Record API
/// (`/services/rest/record/v1/{record_type}`).
///
/// Auth is a pre-obtained OAuth2 access token (NetSuite's REST
/// endpoints support OAuth 2.0 client-credentials in modern
/// accounts, alongside the older TBA/OAuth 1.0a signing scheme this
/// connector doesn't implement — same "credential in config, token
/// refresh out of scope for v1" contract every OAuth2 connector in
/// this repo documents). `account_url` is the full account-specific
/// REST domain (`https://{account-id}.suitetalk.api.netsuite.com`) —
/// a field, not assembled from a separate account-id key, same
/// testability contract every REST connector here follows.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct NetsuiteConnectorConfig {
    pub account_url: String,
    pub access_token: String,
    /// Record type for the sink (create/update), e.g. `customer`,
    /// `salesorder`, `invoice` — the REST Record API's URL segment.
    /// Ignored by the source (SuiteQL's `query` already names the
    /// table).
    #[serde(default)]
    pub record_type: Option<String>,
    /// Raw SuiteQL query, e.g. `"SELECT id, entityid, email FROM
    /// customer"` — source only, ignored by the sink. SuiteQL is a
    /// real subset of SQL NetSuite documents; this connector doesn't
    /// build or validate the query itself (unlike the ODBC connectors
    /// in this repo, which own their SQL) — the caller writes it.
    #[serde(default)]
    pub query: Option<String>,
    /// Page size (SuiteQL's implicit page size via `Prefer:
    /// transient` + this connector's own limit/offset pagination
    /// added to the query — see `source.rs` for how).
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl NetsuiteConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.account_url.trim().is_empty() {
            return Err(NexusError::Connector(
                "netsuite: account_url is required".to_string(),
            ));
        }
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "netsuite: access_token is required".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn query_or_err(&self) -> Result<&str, NexusError> {
        self.query
            .as_deref()
            .filter(|q| !q.trim().is_empty())
            .ok_or_else(|| NexusError::Connector("netsuite source requires query".into()))
    }

    pub(crate) fn record_type_or_err(&self) -> Result<&str, NexusError> {
        self.record_type
            .as_deref()
            .filter(|t| !t.trim().is_empty())
            .ok_or_else(|| NexusError::Connector("netsuite sink requires record_type".into()))
    }
}

fn default_page_size() -> u32 {
    100
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> NetsuiteConnectorConfig {
        NetsuiteConnectorConfig {
            account_url: "https://1234567.suitetalk.api.netsuite.com".into(),
            access_token: "eyJ.test".into(),
            record_type: Some("customer".into()),
            query: Some("SELECT id, entityid FROM customer".into()),
            page_size: 100,
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_account_url() {
        let mut cfg = base_config();
        cfg.account_url = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn query_or_err_fails_when_unset() {
        let mut cfg = base_config();
        cfg.query = None;
        assert!(cfg.query_or_err().is_err());
    }

    #[test]
    fn record_type_or_err_fails_when_unset() {
        let mut cfg = base_config();
        cfg.record_type = None;
        assert!(cfg.record_type_or_err().is_err());
    }
}
