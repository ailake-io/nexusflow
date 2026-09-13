use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3.
///
/// Generic bridging connector for RabbitMQ (AMQP 0-9-1): each message
/// payload is JSON, projected onto `fields`, same contract as
/// `nexus-connector-kafka`/`nexus-connector-nats`. v1 always
/// auto-acks (`no_ack`) on the source side — no manual ack/redelivery
/// handling — same "at-most-once, documented v1 simplification"
/// precedent as `nats`'s lack of JetStream.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RabbitmqConnectorConfig {
    /// AMQP URI, e.g. `"amqp://user:pass@host:5672/%2f"`.
    pub url: String,
    /// Queue name — declared durable if it doesn't already exist
    /// (both source and sink do this independently, idempotently).
    pub queue: String,
    /// Exchange to publish to (sink) / that routes to `queue` (source
    /// declares the queue directly, exchange routing is the
    /// operator's own responsibility via broker config). Empty string
    /// (the default/"direct" exchange) publishes straight to `queue`
    /// by name, no exchange setup needed — the common case for a
    /// simple point-to-point pipeline.
    #[serde(default)]
    pub exchange: String,
    /// Routing key for publishes — defaults to `queue`'s name, which
    /// is what routes correctly against the default exchange.
    #[serde(default)]
    pub routing_key: Option<String>,
    /// Explicit column projection — a message payload is an opaque
    /// byte blob (assumed JSON), same contract as `kafka`/`nats`'s
    /// `fields`.
    pub fields: Vec<RabbitmqFieldSpec>,
    /// How many decoded messages to fold into a single `RecordBatch`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// How long to wait for a new message before returning what's
    /// been buffered so far — same "idle means try again" contract as
    /// Kafka/NATS.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Timeout in seconds for connecting.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RabbitmqFieldSpec {
    pub name: String,
    pub data_type: RabbitmqDataType,
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a payload field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RabbitmqDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl RabbitmqConnectorConfig {
    pub fn validate(&self) -> Result<(), nexus_core::NexusError> {
        if self.url.trim().is_empty() {
            return Err(nexus_core::NexusError::Connector(
                "rabbitmq: url is required".to_string(),
            ));
        }
        if self.queue.trim().is_empty() {
            return Err(nexus_core::NexusError::Connector(
                "rabbitmq: queue is required".to_string(),
            ));
        }
        Ok(())
    }

    pub fn routing_key(&self) -> &str {
        self.routing_key.as_deref().unwrap_or(&self.queue)
    }
}

fn default_batch_size() -> usize {
    500
}

fn default_idle_timeout_ms() -> u64 {
    2000
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> RabbitmqConnectorConfig {
        RabbitmqConnectorConfig {
            url: "amqp://localhost:5672/%2f".into(),
            queue: "events".into(),
            exchange: String::new(),
            routing_key: None,
            fields: Vec::new(),
            batch_size: 500,
            idle_timeout_ms: 2000,
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_url() {
        let mut cfg = base_config();
        cfg.url = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_queue() {
        let mut cfg = base_config();
        cfg.queue = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn routing_key_defaults_to_queue_name() {
        let cfg = base_config();
        assert_eq!(cfg.routing_key(), "events");
    }

    #[test]
    fn explicit_routing_key_overrides_default() {
        let mut cfg = base_config();
        cfg.routing_key = Some("custom.key".into());
        assert_eq!(cfg.routing_key(), "custom.key");
    }
}
