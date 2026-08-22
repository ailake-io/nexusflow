use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3.
///
/// Generic MQTT subscriber: each message's payload is decoded as JSON and
/// projected onto `fields`. `topic_filter` accepts MQTT wildcards (`+`,
/// `#`) — a subscription can blend many logical sensors into one read, so
/// every output row also carries the concrete topic it arrived on (see
/// `MQTT_TOPIC_COLUMN`).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MqttConnectorConfig {
    /// Broker address, e.g. `"mqtt://broker.example.com:1883"` or
    /// `"mqtts://broker.example.com:8883"` for TLS. Scheme decides
    /// plaintext vs TLS; a missing port defaults to 1883 (plaintext) or
    /// 8883 (TLS).
    pub broker_url: String,

    /// MQTT client id. Not generated randomly on purpose: reusing the same
    /// id across runs (together with `clean_session: false`, always set)
    /// is what lets the broker's persistent session redeliver QoS 1/2
    /// messages published while this connector was offline — no
    /// checkpoint/cursor needed on the NexusFlow side. See ARCHITECTURE.md
    /// §7.
    pub client_id: String,

    /// Topic filter to subscribe to. Supports MQTT wildcards: `+` for one
    /// level (`sensors/+/temperature`), `#` for the remaining levels
    /// (`sensors/#`).
    pub topic_filter: String,

    /// Quality of service for the subscription. `at_least_once` (default)
    /// is the sane default for telemetry; `at_most_once` trades delivery
    /// guarantees for throughput.
    #[serde(default)]
    pub qos: MqttQos,

    /// Username for broker authentication, if required.
    #[serde(default)]
    pub username: Option<String>,

    /// Password for broker authentication, if required. Stored in plain
    /// text in the node config; the frontend masks it as a secret input
    /// field.
    #[serde(default)]
    pub password: Option<String>,

    /// PEM-encoded CA certificate path, for TLS brokers using a private CA
    /// (self-signed or internal). Omit to trust the platform's default
    /// root store.
    #[serde(default)]
    pub ca_cert_path: Option<String>,

    /// PEM-encoded client certificate path, for brokers requiring mutual
    /// TLS (e.g. AWS IoT Core, which always requires client-cert auth).
    /// Must be set together with `client_key_path`.
    #[serde(default)]
    pub client_cert_path: Option<String>,

    /// PEM-encoded client private key path, paired with
    /// `client_cert_path`.
    #[serde(default)]
    pub client_key_path: Option<String>,

    /// Explicit target schema — a JSON message payload carries no fixed
    /// schema of its own, so the node config must say what to project each
    /// field to.
    pub fields: Vec<MqttFieldSpec>,

    /// How many decoded messages to fold into a single `RecordBatch`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// How long to wait for a new message before treating the subscription
    /// as drained for this read — MQTT telemetry has no natural end, so a
    /// bridging/bounded read needs an idle cutoff.
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,

    /// Hard cap on messages consumed per `read_batches` call.
    #[serde(default = "default_max_messages")]
    pub max_messages: usize,
}

impl MqttConnectorConfig {
    /// Splits `broker_url` into `(host, port, use_tls)`. Falls back to the
    /// MQTT-standard default port for the resolved scheme when none is
    /// given.
    pub fn host_port_tls(&self) -> Result<(String, u16, bool), String> {
        let (scheme, rest) = self
            .broker_url
            .split_once("://")
            .ok_or_else(|| format!("broker_url missing scheme: {}", self.broker_url))?;
        let use_tls = match scheme {
            "mqtt" => false,
            "mqtts" => true,
            other => return Err(format!("unsupported broker_url scheme: {other}")),
        };
        let default_port = if use_tls { 8883 } else { 1883 };
        let (host, port) = match rest.split_once(':') {
            Some((host, port_str)) => {
                let port = port_str
                    .parse::<u16>()
                    .map_err(|e| format!("invalid port in broker_url: {e}"))?;
                (host.to_string(), port)
            }
            None => (rest.to_string(), default_port),
        };
        Ok((host, port, use_tls))
    }
}

/// Subscription QoS level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MqttQos {
    AtMostOnce,
    #[default]
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MqttFieldSpec {
    /// JSON field name to project from the decoded payload.
    pub name: String,
    /// Arrow type this field's value gets converted to.
    pub data_type: MqttDataType,
    /// Whether a missing/null value for this field is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a payload field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MqttDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> MqttConnectorConfig {
        MqttConnectorConfig {
            broker_url: "mqtt://localhost:1883".to_string(),
            client_id: "nexus-mqtt".to_string(),
            topic_filter: "sensors/#".to_string(),
            qos: MqttQos::AtLeastOnce,
            username: None,
            password: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            fields: Vec::new(),
            batch_size: 500,
            poll_timeout_ms: 2000,
            max_messages: 100_000,
        }
    }

    #[test]
    fn plaintext_scheme_defaults_to_port_1883() {
        let mut cfg = base_config();
        cfg.broker_url = "mqtt://broker.example.com".to_string();
        assert_eq!(
            cfg.host_port_tls().unwrap(),
            ("broker.example.com".to_string(), 1883, false)
        );
    }

    #[test]
    fn tls_scheme_defaults_to_port_8883() {
        let mut cfg = base_config();
        cfg.broker_url = "mqtts://broker.example.com".to_string();
        assert_eq!(
            cfg.host_port_tls().unwrap(),
            ("broker.example.com".to_string(), 8883, true)
        );
    }

    #[test]
    fn explicit_port_overrides_default() {
        let mut cfg = base_config();
        cfg.broker_url = "mqtt://broker.example.com:11883".to_string();
        assert_eq!(
            cfg.host_port_tls().unwrap(),
            ("broker.example.com".to_string(), 11883, false)
        );
    }

    #[test]
    fn missing_scheme_errors() {
        let mut cfg = base_config();
        cfg.broker_url = "broker.example.com:1883".to_string();
        assert!(cfg.host_port_tls().is_err());
    }

    #[test]
    fn unsupported_scheme_errors() {
        let mut cfg = base_config();
        cfg.broker_url = "http://broker.example.com".to_string();
        assert!(cfg.host_port_tls().is_err());
    }
}
