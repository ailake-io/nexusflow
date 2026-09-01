use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3.
///
/// Generic bridging connector for core NATS pub/sub (not JetStream) —
/// each message payload is JSON, projected onto `fields`, same
/// contract as `nexus-connector-kafka`/`nexus-connector-mqtt`. Core
/// NATS has no persistence/replay: a subscription only sees messages
/// published while it's connected, and delivery is at-most-once (no
/// ack, no redelivery) — same real limitation every core-NATS client
/// has, not a NexusFlow gap. JetStream (NATS's persistent/replayable
/// layer) is a much larger feature (streams, consumers, acks) left
/// for a future iteration if there's demand, same "v1 simplification,
/// documented" precedent as `kinesis`/`pulsar`'s in-memory cursors.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct NatsConnectorConfig {
    /// Server URL, e.g. `"nats://localhost:4222"` or
    /// `"tls://localhost:4222"`.
    pub server_url: String,
    /// Subject to subscribe to (source) or publish to (sink). Supports
    /// NATS wildcards on the source side (`*` for one token, `>` for
    /// the rest) — a wildcard subscription blends many logical
    /// subjects into one read, so every output row also carries the
    /// concrete subject it arrived on, same precedent as MQTT's
    /// `MQTT_TOPIC_COLUMN`.
    pub subject: String,
    /// Optional queue group — when set, only one subscriber in the
    /// group receives each message (load-balanced fan-out), same
    /// semantic as a Kafka consumer group but without offset
    /// tracking. Ignored by the sink.
    #[serde(default)]
    pub queue_group: Option<String>,
    /// Optional bearer token for authentication.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Optional username/password authentication — ignored if
    /// `auth_token` is set.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Explicit column projection — a NATS message payload is an
    /// opaque byte blob (assumed JSON), same contract as
    /// `kafka`/`mqtt`'s `fields`.
    pub fields: Vec<NatsFieldSpec>,
    /// How many decoded messages to fold into a single `RecordBatch`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// How long to wait for a new message before returning what's
    /// been buffered so far — a subject has no natural end, same
    /// "idle means try again" contract as Kafka/MQTT.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Timeout in seconds for connecting.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct NatsFieldSpec {
    pub name: String,
    pub data_type: NatsDataType,
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a payload field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NatsDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl NatsConnectorConfig {
    pub fn validate(&self) -> Result<(), nexus_core::NexusError> {
        if self.server_url.trim().is_empty() {
            return Err(nexus_core::NexusError::Connector(
                "nats: server_url is required".to_string(),
            ));
        }
        if self.subject.trim().is_empty() {
            return Err(nexus_core::NexusError::Connector(
                "nats: subject is required".to_string(),
            ));
        }
        Ok(())
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

    fn base_config() -> NatsConnectorConfig {
        NatsConnectorConfig {
            server_url: "nats://localhost:4222".into(),
            subject: "events".into(),
            queue_group: None,
            auth_token: None,
            username: None,
            password: None,
            fields: Vec::new(),
            batch_size: 500,
            idle_timeout_ms: 2000,
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_server_url() {
        let mut cfg = base_config();
        cfg.server_url = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_subject() {
        let mut cfg = base_config();
        cfg.subject = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_valid_config() {
        assert!(base_config().validate().is_ok());
    }
}
