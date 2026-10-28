use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is Basic (username/password) — the real, documented default
/// for ServiceNow's Table API, simpler than OAuth for a direct
/// integration, same rationale `nexus-connector-zendesk`'s API-token
/// Basic auth gives. `base_url` is the full instance URL
/// (`https://{instance}.service-now.com`) — a field, not assembled
/// from a separate `instance` config key, same testability contract
/// every REST connector in this repo follows.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ServicenowConnectorConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    /// Table name, e.g. `incident`, `problem`, `change_request`, or a
    /// custom table's internal name.
    pub table: String,
    /// Columns to fetch/write — the Table API returns every column by
    /// default unless `sysparm_fields` narrows it, same rationale
    /// `nexus-connector-hubspot`'s `properties` gives.
    pub fields: Vec<String>,
    /// Page size for list requests (`sysparm_limit`) — ServiceNow's
    /// real Table API default cap is 10,000, but a smaller page keeps
    /// each response manageable.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl ServicenowConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.base_url.trim().is_empty() {
            return Err(NexusError::Connector(
                "servicenow: base_url is required".to_string(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector(
                "servicenow: username is required".to_string(),
            ));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector(
                "servicenow: password is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "servicenow: table is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "servicenow: fields must not be empty".to_string(),
            ));
        }
        Ok(())
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

    fn base_config() -> ServicenowConnectorConfig {
        ServicenowConnectorConfig {
            base_url: "https://example.service-now.com".into(),
            username: "admin".into(),
            password: "secret".into(),
            table: "incident".into(),
            fields: vec!["sys_id".into(), "short_description".into()],
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
}
