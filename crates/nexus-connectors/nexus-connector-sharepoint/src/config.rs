use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Reads/writes items in a SharePoint **List** via Microsoft Graph
/// (`/sites/{site_id}/lists/{list_id}/items`) — genuinely tabular data
/// (unlike a SharePoint/OneDrive document library, which is files —
/// see `nexus-connector-dropbox`/`google-drive` for that shape
/// instead). Auth is a pre-obtained OAuth2 access token (Azure AD/
/// Entra ID), same "credential in config, not ambient environment,
/// token refresh out of scope for v1" contract every Microsoft/Google
/// cloud connector in this repo documents.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct SharepointConnectorConfig {
    pub access_token: String,
    pub site_id: String,
    pub list_id: String,
    /// Column (internal) names to fetch/write — these live under each
    /// item's `fields` sub-resource in the Graph API, not at the
    /// item's top level (real API shape).
    pub fields: Vec<String>,
    /// Page size (`$top`) for list requests.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// API base URL — field, not hardcoded, so tests can point it at
    /// a mock server. Real default is Graph's global endpoint.
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl SharepointConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "sharepoint: access_token is required".to_string(),
            ));
        }
        if self.site_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "sharepoint: site_id is required".to_string(),
            ));
        }
        if self.list_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "sharepoint: list_id is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "sharepoint: fields must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_page_size() -> u32 {
    100
}

fn default_base_url() -> String {
    "https://graph.microsoft.com/v1.0".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> SharepointConnectorConfig {
        SharepointConnectorConfig {
            access_token: "eyJ.test".into(),
            site_id: "site123".into(),
            list_id: "list456".into(),
            fields: vec!["Title".into(), "Status".into()],
            page_size: 100,
            base_url: "https://graph.microsoft.com/v1.0".into(),
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_site_id() {
        let mut cfg = base_config();
        cfg.site_id = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_fields() {
        let mut cfg = base_config();
        cfg.fields = Vec::new();
        assert!(cfg.validate().is_err());
    }
}
