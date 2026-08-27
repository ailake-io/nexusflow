use nexus_core::NexusError;
use serde::Deserialize;

/// Configuration for the Apache Pulsar source. Auth is an explicit
/// optional token in the config, same "credential in config, not
/// ambient environment" contract every cloud/streaming connector in
/// this workspace already follows.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PulsarConnectorConfig {
    /// Broker URL, e.g. `pulsar://localhost:6650` or
    /// `pulsar+ssl://...`.
    pub service_url: String,
    pub topic: String,
    pub subscription_name: String,
    #[serde(default)]
    pub subscription_type: SubscriptionType,
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Explicit column projection — Pulsar messages are an opaque byte
    /// payload (assumed JSON in v1), same contract as `kinesis`'s
    /// `fields`.
    pub fields: Vec<PulsarFieldSpec>,
    /// Each broker read is wrapped in a timeout so an idle topic
    /// doesn't block forever — a timeout just means "no new message
    /// yet, try again", not an error.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl PulsarConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.service_url.trim().is_empty() {
            return Err(NexusError::Connector(
                "pulsar: service_url is required".to_string(),
            ));
        }
        if self.topic.trim().is_empty() {
            return Err(NexusError::Connector(
                "pulsar: topic is required".to_string(),
            ));
        }
        if self.subscription_name.trim().is_empty() {
            return Err(NexusError::Connector(
                "pulsar: subscription_name is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "pulsar: at least one field is required".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PulsarFieldSpec {
    pub name: String,
    /// One of `int64`, `float64`, `boolean`, `utf8` — the same four
    /// primitives `RecordBatchBuilder::from_json_rows` (`nexus-core`)
    /// supports.
    pub r#type: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionType {
    #[default]
    Exclusive,
    Shared,
    Failover,
    KeyShared,
}

fn default_idle_timeout_ms() -> u64 {
    5000
}

fn default_batch_size() -> usize {
    500
}

fn default_timeout_seconds() -> u64 {
    30
}
