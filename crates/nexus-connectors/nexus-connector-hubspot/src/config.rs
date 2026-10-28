use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is a Private App access token (`Authorization: Bearer
/// pat-...`) — HubSpot's current recommended auth method, replacing
/// the deprecated API-key scheme. CRM v3 Objects API
/// (`/crm/v3/objects/{object_type}`), same category as Salesforce's
/// generic-object approach but REST+JSON pagination instead of a bulk
/// CSV export job — HubSpot has no bulk export API for arbitrary
/// objects the way Salesforce does.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct HubspotConnectorConfig {
    pub access_token: String,
    /// CRM object type, e.g. `contacts`, `companies`, `deals`,
    /// `tickets`, or a custom object's internal name.
    pub object_type: String,
    /// Properties to fetch/write — HubSpot only returns a small
    /// default set unless explicitly requested via `?properties=`.
    pub properties: Vec<String>,
    /// Property used as the upsert key for the sink (HubSpot's Batch
    /// Upsert API's `idProperty`) — e.g. `email` for contacts. Required
    /// for the sink, ignored by the source. Without a real unique
    /// property to upsert on, every write would just create
    /// duplicates.
    #[serde(default)]
    pub id_property: Option<String>,
    /// Page size for list requests — HubSpot allows up to 100.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// API base URL — field, not hardcoded, so tests can point it at
    /// a mock server.
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl HubspotConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "hubspot: access_token is required".to_string(),
            ));
        }
        if self.object_type.trim().is_empty() {
            return Err(NexusError::Connector(
                "hubspot: object_type is required".to_string(),
            ));
        }
        if self.properties.is_empty() {
            return Err(NexusError::Connector(
                "hubspot: properties must not be empty".to_string(),
            ));
        }
        if self.page_size == 0 || self.page_size > 100 {
            return Err(NexusError::Connector(
                "hubspot: page_size must be between 1 and 100".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn id_property_or_err(&self) -> Result<&str, NexusError> {
        self.id_property
            .as_deref()
            .ok_or_else(|| NexusError::Schema("hubspot sink requires id_property".into()))
    }
}

fn default_page_size() -> u32 {
    100
}

fn default_base_url() -> String {
    "https://api.hubapi.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> HubspotConnectorConfig {
        HubspotConnectorConfig {
            access_token: "pat-test".into(),
            object_type: "contacts".into(),
            properties: vec!["email".into(), "firstname".into()],
            id_property: None,
            page_size: 100,
            base_url: "https://api.hubapi.com".into(),
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_access_token() {
        let mut cfg = base_config();
        cfg.access_token = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_properties() {
        let mut cfg = base_config();
        cfg.properties = Vec::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_page_size_over_100() {
        let mut cfg = base_config();
        cfg.page_size = 101;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn id_property_or_err_fails_when_unset() {
        assert!(base_config().id_property_or_err().is_err());
    }
}
