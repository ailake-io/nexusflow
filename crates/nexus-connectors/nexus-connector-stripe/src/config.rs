use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// **Read-only by design, permanently** — this connector never gains
/// a sink. Executing a financial charge/refund/transfer is out of
/// scope by standing instruction, not a v1 simplification to revisit
/// later.
///
/// Auth is a plain secret key (`Authorization: Bearer sk_...`), no
/// OAuth2 — Stripe Connect's OAuth exists only for platforms acting on
/// behalf of other Stripe accounts, not relevant to a direct
/// integration like this one.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct StripeConnectorConfig {
    pub secret_key: String,
    pub resource: StripeResource,
    /// Top-level JSON keys to project as columns (e.g. `id`, `amount`,
    /// `customer`, `status`, `created`). Nested objects/arrays are
    /// rendered as their JSON text form rather than flattened —
    /// simpler than the dotted-path flattening
    /// `nexus-connector-google-ads` needs, since Stripe's fields of
    /// interest are typically already top-level.
    pub fields: Vec<String>,
    /// Incremental read filter — `created[gte]` (Unix epoch seconds),
    /// confirmed real query param on Stripe's list endpoints. Omit for
    /// a full backfill.
    #[serde(default)]
    pub created_gte: Option<i64>,
    /// Optional upper bound for the incremental window — `created[lt]`.
    /// When both `created_gte` and `created_lt` are provided, the
    /// connector may fetch non-overlapping time windows in parallel
    /// (up to `max_concurrent_requests`).
    #[serde(default)]
    pub created_lt: Option<i64>,
    /// `Stripe-Version` header — required on direct (non-SDK) API
    /// calls. Pinned to a real, dated version rather than "latest" so
    /// response shape doesn't shift under this connector without
    /// notice.
    #[serde(default = "default_stripe_version")]
    pub stripe_version: String,
    /// API base URL — field, not hardcoded, so tests can point it at
    /// a mock server.
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    /// Page size for list requests. Stripe allows up to 100.
    #[serde(default = "default_page_limit")]
    pub page_limit: u32,
    /// Shared retry/backoff configuration for transient HTTP failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
    /// Timeout in seconds for each HTTP call.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl StripeConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.secret_key.trim().is_empty() {
            return Err(NexusError::Connector(
                "stripe: secret_key is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "stripe: fields must not be empty".to_string(),
            ));
        }
        if self.page_limit == 0 || self.page_limit > 100 {
            return Err(NexusError::Connector(
                "stripe: page_limit must be between 1 and 100".to_string(),
            ));
        }
        if let (Some(gte), Some(lt)) = (self.created_gte, self.created_lt) {
            if gte >= lt {
                return Err(NexusError::Connector(
                    "stripe: created_gte must be less than created_lt".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StripeResource {
    Charges,
    Invoices,
    Customers,
    Subscriptions,
}

impl StripeResource {
    pub(crate) fn path(self) -> &'static str {
        match self {
            StripeResource::Charges => "charges",
            StripeResource::Invoices => "invoices",
            StripeResource::Customers => "customers",
            StripeResource::Subscriptions => "subscriptions",
        }
    }
}

fn default_stripe_version() -> String {
    "2025-11-17".to_string()
}

fn default_api_base_url() -> String {
    "https://api.stripe.com".to_string()
}

fn default_page_limit() -> u32 {
    100
}

fn default_timeout_seconds() -> u64 {
    30
}
