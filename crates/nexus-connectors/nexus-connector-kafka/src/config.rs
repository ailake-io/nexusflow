use serde::Deserialize;
use std::collections::HashMap;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// Generic Kafka consumer: each message's payload is decoded as JSON and
/// projected onto `fields`. CDC is covered natively per-database instead
/// (`postgres-cdc`/`mongodb-cdc`/`mysql-cdc`) — see ARCHITECTURE.md §7.
///
/// The broker list can be supplied either as a complete comma-separated string
/// through `bootstrap_servers` (which takes precedence) or through the
/// individual `brokers` array and optional `port`, which are assembled when no
/// explicit string is given.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct KafkaConnectorConfig {
    /// Comma-separated `host:port` list of Kafka brokers to bootstrap from,
    /// e.g. `"broker1:9092,broker2:9092"`. When provided, this overrides any
    /// separate `brokers`/`port` entries and is used verbatim in the Kafka
    /// client configuration.
    #[serde(default)]
    pub bootstrap_servers: Option<String>,

    /// List of Kafka broker hosts or `host:port` pairs. Used only when
    /// `bootstrap_servers` is empty. Entries without an explicit port get
    /// `:9092` appended automatically. Example: `["kafka-1", "kafka-2:9093"]`.
    #[serde(default)]
    pub brokers: Vec<String>,

    /// Default port appended to `brokers` entries that do not already contain
    /// a colon. Ignored when `bootstrap_servers` is provided or when every
    /// broker entry already includes a port.
    #[serde(default = "default_broker_port")]
    pub port: u16,

    /// Topic to consume from.
    pub topic: String,

    /// Consumer group id — controls offset tracking on the broker side;
    /// reuse the same id across runs of the same pipeline to resume from
    /// where the group last committed.
    pub group_id: String,

    /// Optional client id sent to Kafka in the client metadata. Useful for
    /// observability and broker-side logging. When omitted, the Kafka driver
    /// generates an id automatically.
    #[serde(default)]
    pub client_id: Option<String>,

    /// Security protocol used to communicate with the brokers. Defaults to
    /// plaintext when not set. `SaslSsl` and `SaslPlaintext` enable SASL
    /// authentication; `Ssl` enables TLS without SASL.
    #[serde(default)]
    pub security_protocol: Option<KafkaSecurityProtocol>,

    /// SASL mechanism for broker authentication. Required when
    /// `security_protocol` is `sasl_plaintext` or `sasl_ssl`. Ignored for
    /// plaintext or SSL-only connections.
    #[serde(default)]
    pub sasl_mechanism: Option<KafkaSaslMechanism>,

    /// SASL username for broker authentication. Required together with
    /// `sasl_password` when SASL is enabled.
    #[serde(default)]
    pub sasl_username: Option<String>,

    /// SASL password or secret for broker authentication. Stored in plain text
    /// in the node config; the frontend masks it as a secret input field.
    #[serde(default)]
    pub sasl_password: Option<String>,

    /// Explicit target schema — a JSON message payload carries no fixed
    /// schema of its own. Left empty, the connector samples up to
    /// `schema_sample_rows` messages via a throwaway consumer group (never
    /// committed, doesn't touch `group_id`'s offsets) and infers one —
    /// union of keys across the sample, typed by first non-null value, see
    /// `nexus_core::RecordBatchBuilder::infer_schema`.
    #[serde(default)]
    pub fields: Vec<KafkaFieldSpec>,

    /// How many messages to sample when inferring `fields` (only used when
    /// `fields` is empty). Defaults to 1000.
    #[serde(default = "default_schema_sample_rows")]
    pub schema_sample_rows: usize,

    /// How many decoded messages to fold into a single `RecordBatch`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// How long to wait for a new message before treating the topic as
    /// drained for this read — a Kafka topic has no natural end, so a
    /// bridging/bounded read needs an idle cutoff.
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,

    /// Hard cap on messages consumed per `read_batches` call.
    #[serde(default = "default_max_messages")]
    pub max_messages: usize,

    /// Explicit per-partition start offsets for resume (checkpoint replay) —
    /// see `checkpoint_store` (already generic per `(pipeline_id,
    /// partition_id)` since Marco 1). Absent partitions fall back to
    /// `auto.offset.reset = earliest`.
    #[serde(default)]
    pub start_offsets: HashMap<i32, i64>,
}

impl KafkaConnectorConfig {
    /// Resolves the broker list that should be passed to `bootstrap.servers`.
    ///
    /// If `bootstrap_servers` is set, it is returned verbatim. Otherwise the
    /// `brokers` array is normalized: entries without a port get
    /// `:{port}` appended, and the result is joined with commas.
    pub fn bootstrap_servers(&self) -> String {
        if let Some(servers) = &self.bootstrap_servers {
            if !servers.trim().is_empty() {
                return servers.clone();
            }
        }

        if self.brokers.is_empty() {
            return format!("localhost:{}", self.port);
        }

        self.brokers
            .iter()
            .map(|b| {
                let trimmed = b.trim();
                if trimmed.contains(':') {
                    trimmed.to_string()
                } else {
                    format!("{}:{}", trimmed, self.port)
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Builds a preconfigured `rdkafka::ClientConfig` from this connector
    /// config. The returned config already contains `bootstrap.servers`,
    /// `group.id`, `client.id` (when present), `enable.auto.commit=false`,
    /// `auto.offset.reset=earliest`, and any SASL/SSL security settings.
    #[cfg(any(feature = "consumer", feature = "producer"))]
    pub fn client_config(&self) -> rdkafka::ClientConfig {
        use rdkafka::ClientConfig;

        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", self.bootstrap_servers());
        config.set("group.id", &self.group_id);
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "earliest");

        if let Some(client_id) = self.client_id.as_ref().filter(|s| !s.is_empty()) {
            config.set("client.id", client_id);
        }

        if let Some(protocol) = &self.security_protocol {
            config.set("security.protocol", protocol.as_str());
        }

        if let Some(mechanism) = &self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism.as_str());
        }

        if let Some(username) = self.sasl_username.as_ref().filter(|s| !s.is_empty()) {
            config.set("sasl.username", username);
        }

        if let Some(password) = self.sasl_password.as_ref().filter(|s| !s.is_empty()) {
            config.set("sasl.password", password);
        }

        config
    }
}

/// Security protocol for the Kafka client-broker connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaSecurityProtocol {
    /// Unencrypted plaintext connection.
    Plaintext,
    /// SASL-authenticated plaintext connection.
    SaslPlaintext,
    /// TLS-encrypted connection without SASL.
    Ssl,
    /// TLS-encrypted connection with SASL authentication.
    SaslSsl,
}

impl KafkaSecurityProtocol {
    /// Returns the `security.protocol` string expected by librdkafka.
    pub fn as_str(&self) -> &'static str {
        match self {
            KafkaSecurityProtocol::Plaintext => "PLAINTEXT",
            KafkaSecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
            KafkaSecurityProtocol::Ssl => "SSL",
            KafkaSecurityProtocol::SaslSsl => "SASL_SSL",
        }
    }
}

/// SASL mechanism for Kafka broker authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaSaslMechanism {
    /// Plain username/password authentication.
    Plain,
    /// SCRAM-SHA-256 authentication.
    ScramSha256,
    /// SCRAM-SHA-512 authentication.
    ScramSha512,
}

impl KafkaSaslMechanism {
    /// Returns the `sasl.mechanism` string expected by librdkafka.
    pub fn as_str(&self) -> &'static str {
        match self {
            KafkaSaslMechanism::Plain => "PLAIN",
            KafkaSaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            KafkaSaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct KafkaFieldSpec {
    /// JSON field name to project from the decoded payload.
    pub name: String,
    /// Arrow type this field's value gets converted to.
    pub data_type: KafkaDataType,
    /// Whether a missing/null value for this field is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a payload field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_broker_port() -> u16 {
    9092
}

fn default_batch_size() -> usize {
    500
}

fn default_poll_timeout_ms() -> u64 {
    2000
}

fn default_max_messages() -> usize {
    100_000
}

fn default_schema_sample_rows() -> usize {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> KafkaConnectorConfig {
        KafkaConnectorConfig {
            bootstrap_servers: None,
            brokers: Vec::new(),
            port: 9092,
            topic: "events".to_string(),
            group_id: "nexus".to_string(),
            client_id: None,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            fields: Vec::new(),
            schema_sample_rows: 1000,
            batch_size: 500,
            poll_timeout_ms: 2000,
            max_messages: 100_000,
            start_offsets: HashMap::new(),
        }
    }

    #[test]
    fn bootstrap_servers_string_takes_priority() {
        let mut cfg = base_config();
        cfg.bootstrap_servers = Some("broker1:9092,broker2:9092".to_string());
        cfg.brokers = vec!["ignored:9092".to_string()];
        assert_eq!(cfg.bootstrap_servers(), "broker1:9092,broker2:9092");
    }

    #[test]
    fn empty_bootstrap_servers_falls_back_to_brokers() {
        let mut cfg = base_config();
        cfg.bootstrap_servers = Some("".to_string());
        cfg.brokers = vec!["kafka-1".to_string(), "kafka-2:9093".to_string()];
        assert_eq!(cfg.bootstrap_servers(), "kafka-1:9092,kafka-2:9093");
    }

    #[test]
    fn defaults_to_localhost_when_no_brokers() {
        let cfg = base_config();
        assert_eq!(cfg.bootstrap_servers(), "localhost:9092");
    }

    #[test]
    fn custom_port_applies_to_host_only_brokers() {
        let mut cfg = base_config();
        cfg.port = 9093;
        cfg.brokers = vec!["kafka-1".to_string(), "kafka-2:9094".to_string()];
        assert_eq!(cfg.bootstrap_servers(), "kafka-1:9093,kafka-2:9094");
    }

    #[cfg(feature = "consumer")]
    #[test]
    fn client_config_can_be_created_with_sasl_ssl() {
        let mut cfg = base_config();
        cfg.brokers = vec!["kafka:9092".to_string()];
        cfg.client_id = Some("nexus-test".to_string());
        cfg.security_protocol = Some(KafkaSecurityProtocol::SaslSsl);
        cfg.sasl_mechanism = Some(KafkaSaslMechanism::ScramSha256);
        cfg.sasl_username = Some("user".to_string());
        cfg.sasl_password = Some("secret".to_string());

        // The helper must produce a valid config object without panicking.
        // Actual librdkafka validation happens when a consumer/producer is
        // created from it (integration tests cover that path).
        let _client_config = cfg.client_config();
    }
}
