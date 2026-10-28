use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is an API token (`email/token:api_token`, Basic auth — real,
/// documented Zendesk auth scheme, simpler than OAuth for a direct
/// integration, same rationale `nexus-connector-stripe`'s plain
/// secret-key auth gives). `base_url` is the full subdomain URL
/// (`https://{subdomain}.zendesk.com`) — a field, not assembled from a
/// separate `subdomain` config key, so tests can point it at a mock
/// server the same way `nexus-connector-hubspot`/`stripe` do.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ZendeskConnectorConfig {
    pub base_url: String,
    pub email: String,
    pub api_token: String,
    pub resource: ZendeskResource,
    /// Top-level JSON keys to project as columns — same "nested
    /// objects rendered as JSON text" simplification
    /// `nexus-connector-stripe`'s `fields` documents.
    pub fields: Vec<String>,
    /// Page size for list requests — Zendesk's cursor pagination
    /// allows up to 100.
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

/// Zendesk Support API v2 resources this connector supports —
/// `tickets`, `users`, `organizations` are the three most commonly
/// integrated, real endpoint names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ZendeskResource {
    Tickets,
    Users,
    Organizations,
}

impl ZendeskResource {
    /// Plural form — the list endpoint path and the JSON array key in
    /// its response (e.g. `GET /api/v2/tickets.json` → `{"tickets":
    /// [...]}`), both real and identical by Zendesk convention.
    pub(crate) fn plural(self) -> &'static str {
        match self {
            ZendeskResource::Tickets => "tickets",
            ZendeskResource::Users => "users",
            ZendeskResource::Organizations => "organizations",
        }
    }

    /// Singular form — the JSON request/response body wrapper key for
    /// create/update calls (e.g. `POST /api/v2/tickets.json` body
    /// `{"ticket": {...}}`), real Zendesk convention.
    pub(crate) fn singular(self) -> &'static str {
        match self {
            ZendeskResource::Tickets => "ticket",
            ZendeskResource::Users => "user",
            ZendeskResource::Organizations => "organization",
        }
    }
}

impl ZendeskConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.base_url.trim().is_empty() {
            return Err(NexusError::Connector(
                "zendesk: base_url is required".to_string(),
            ));
        }
        if self.email.trim().is_empty() {
            return Err(NexusError::Connector(
                "zendesk: email is required".to_string(),
            ));
        }
        if self.api_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "zendesk: api_token is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "zendesk: fields must not be empty".to_string(),
            ));
        }
        if self.page_size == 0 || self.page_size > 100 {
            return Err(NexusError::Connector(
                "zendesk: page_size must be between 1 and 100".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn basic_auth_username(&self) -> String {
        format!("{}/token", self.email)
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

    fn base_config() -> ZendeskConnectorConfig {
        ZendeskConnectorConfig {
            base_url: "https://example.zendesk.com".into(),
            email: "agent@example.com".into(),
            api_token: "tok".into(),
            resource: ZendeskResource::Tickets,
            fields: vec!["id".into(), "subject".into()],
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
    fn basic_auth_username_appends_token_suffix() {
        let cfg = base_config();
        assert_eq!(cfg.basic_auth_username(), "agent@example.com/token");
    }

    #[test]
    fn resource_singular_and_plural_forms() {
        assert_eq!(ZendeskResource::Tickets.plural(), "tickets");
        assert_eq!(ZendeskResource::Tickets.singular(), "ticket");
        assert_eq!(ZendeskResource::Organizations.plural(), "organizations");
        assert_eq!(ZendeskResource::Organizations.singular(), "organization");
    }
}
