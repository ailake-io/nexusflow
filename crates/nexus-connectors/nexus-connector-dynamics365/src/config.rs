use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is a pre-obtained OAuth2 access token (Azure AD/Entra ID —
/// Dataverse requires it, no Basic-auth fallback), same "credential
/// in config, not ambient environment, token refresh out of scope
/// for v1" contract `nexus-connector-google-sheets`/`dropbox`/
/// `google-drive` document. `base_url` is the organization's full
/// Dataverse URL (`https://{org}.crm.dynamics.com`) — a field, not
/// assembled from a separate `org` key, same testability contract
/// every REST connector in this repo follows.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct Dynamics365ConnectorConfig {
    pub base_url: String,
    pub access_token: String,
    /// Entity set (plural, lowercase) name, e.g. `accounts`,
    /// `contacts`, `opportunities` — real Dataverse Web API URL
    /// segment (`/api/data/v9.2/{entity_set}`).
    pub entity_set: String,
    /// Primary key column name for this entity, e.g. `accountid` for
    /// `accounts` — Dataverse's primary key is always
    /// `{entity}id` by convention, but not derivable purely from
    /// `entity_set`'s plural form (e.g. `opportunities` →
    /// `opportunityid`, not `opportunitiid`), so it's explicit here.
    /// Required for the sink (identifies the record to `PATCH` via
    /// `{entity_set}({id})`); ignored by the source.
    #[serde(default)]
    pub id_field: Option<String>,
    /// Columns to fetch (`$select`) — Dataverse returns every column
    /// by default without it, which is expensive on wide entities.
    pub fields: Vec<String>,
    /// Page size (`$top`) — Dataverse's own max page size varies by
    /// entity/tenant config, kept modest by default.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl Dynamics365ConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.base_url.trim().is_empty() {
            return Err(NexusError::Connector(
                "dynamics365: base_url is required".to_string(),
            ));
        }
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "dynamics365: access_token is required".to_string(),
            ));
        }
        if self.entity_set.trim().is_empty() {
            return Err(NexusError::Connector(
                "dynamics365: entity_set is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "dynamics365: fields must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn id_field_or_err(&self) -> Result<&str, NexusError> {
        self.id_field
            .as_deref()
            .ok_or_else(|| NexusError::Schema("dynamics365 sink requires id_field".into()))
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

    fn base_config() -> Dynamics365ConnectorConfig {
        Dynamics365ConnectorConfig {
            base_url: "https://example.crm.dynamics.com".into(),
            access_token: "eyJ.test".into(),
            entity_set: "accounts".into(),
            id_field: Some("accountid".into()),
            fields: vec!["accountid".into(), "name".into()],
            page_size: 100,
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_base_url() {
        let mut cfg = base_config();
        cfg.base_url = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_fields() {
        let mut cfg = base_config();
        cfg.fields = Vec::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn id_field_or_err_fails_when_unset() {
        let mut cfg = base_config();
        cfg.id_field = None;
        assert!(cfg.id_field_or_err().is_err());
    }
}
