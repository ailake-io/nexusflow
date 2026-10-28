use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Targets the **GraphQL** Admin API only — confirmed the REST Admin
/// API is legacy (frozen for new apps, GraphQL is Shopify's stated
/// direction), so building against REST here would be building on a
/// dead path.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ShopifyConnectorConfig {
    /// Shop domain, e.g. `my-shop.myshopify.com`.
    pub shop_domain: String,
    /// Admin API access token from a custom app (`X-Shopify-Access-
    /// Token` header) — no OAuth2 dance needed for this
    /// server-to-server case.
    pub access_token: String,
    /// API version, date-stamped (e.g. `2026-01`) — Shopify versions
    /// by release date, not a semantic number.
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Overrides the base URL derived from `shop_domain`
    /// (`https://{shop_domain}`) — unset in real use, set by tests to
    /// point at a mock server.
    #[serde(default)]
    pub api_base_url: Option<String>,
    /// Source: the GraphQL query body wrapped in
    /// `bulkOperationRunQuery`. Required for the source, ignored by
    /// the sink.
    #[serde(default)]
    pub graphql_query: Option<String>,
    /// Source: JSONL keys to project as columns from the bulk query's
    /// result — same "explicit `fields`, no fragile introspection"
    /// simplification `nexus-connector-stripe`'s config already
    /// documents.
    #[serde(default)]
    pub fields: Vec<String>,
    /// Sink: the full GraphQL mutation definition string, with an
    /// `$input` variable (e.g. `mutation call($input: ProductInput!) {
    /// productCreate(input: $input) { product { id } userErrors {
    /// field message } } }`) — `bulkOperationRunMutation` takes the
    /// real mutation text, not just a name, so this can't be
    /// auto-generated from a bare mutation name without per-mutation
    /// input-type knowledge this connector doesn't have. Required for
    /// the sink, ignored by the source.
    #[serde(default)]
    pub sink_mutation: Option<String>,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    /// Timeout for the whole job+poll+download flow. Defaults high —
    /// Shopify bulk operations can run up to 24h on Shopify's side for
    /// genuinely large jobs, same "job can take a while" reasoning
    /// `nexus-connector-meta-ads` documents for its own async jobs.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient HTTP failures
    /// and Shopify's GraphQL rate limits.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl ShopifyConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.shop_domain.trim().is_empty() {
            return Err(NexusError::Connector(
                "shopify: shop_domain is required".to_string(),
            ));
        }
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "shopify: access_token is required".to_string(),
            ));
        }
        if self.api_version.trim().is_empty() {
            return Err(NexusError::Connector(
                "shopify: api_version is required".to_string(),
            ));
        }
        if self.poll_interval_seconds == 0 {
            return Err(NexusError::Connector(
                "shopify: poll_interval_seconds must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn graphql_query_or_err(&self) -> Result<&str, nexus_core::NexusError> {
        self.graphql_query.as_deref().ok_or_else(|| {
            nexus_core::NexusError::Schema("shopify source requires graphql_query".into())
        })
    }

    pub(crate) fn sink_mutation_or_err(&self) -> Result<&str, nexus_core::NexusError> {
        self.sink_mutation.as_deref().ok_or_else(|| {
            nexus_core::NexusError::Schema("shopify sink requires sink_mutation".into())
        })
    }

    pub(crate) fn graphql_url(&self) -> String {
        let base = self
            .api_base_url
            .clone()
            .unwrap_or_else(|| format!("https://{}", self.shop_domain));
        format!("{base}/admin/api/{}/graphql.json", self.api_version)
    }
}

fn default_api_version() -> String {
    "2026-01".to_string()
}

fn default_poll_interval_seconds() -> u64 {
    5
}

fn default_timeout_seconds() -> u64 {
    3600
}
