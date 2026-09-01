use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3.
///
/// Reads/writes a Redis Stream (`XREAD`/`XADD`) — not a generic
/// key/value dump (`SCAN`+`GET`/`SET`). Streams are the natural
/// "ordered, resumable" analog to Kafka/Pulsar topics; a plain KV
/// store has no ordering or replay concept to build a `Source`
/// around. Each stream entry is a flat field→value map already (no
/// JSON envelope needed, unlike Kafka/MQTT's opaque byte payload) —
/// `fields` projects that map onto Arrow columns, same 4-primitive
/// contract (`int64`/`float64`/`boolean`/`utf8`) every bridging
/// connector in this repo uses.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RedisConnectorConfig {
    /// Connection URL, e.g. `redis://[:password@]host:port[/db]` or
    /// `rediss://...` for TLS.
    pub url: String,
    /// Stream key (`XADD`/`XREAD` target).
    pub stream_key: String,
    /// Where to start reading from on the first run — `latest` (only
    /// entries added after the connector starts, Redis's `$`) or
    /// `earliest` (from the very first entry, `0`). No consumer-group
    /// support in v1 (no `XREADGROUP`/`XACK`) — same in-memory-cursor
    /// limitation `kinesis`/`pulsar`'s v1 accepts, see their own doc
    /// comments; a restart re-reads from `starting_position`, not
    /// from where it left off.
    #[serde(default)]
    pub starting_position: RedisStartingPosition,
    /// Column projection for the source — ignored by the sink, which
    /// just writes every column of the incoming `RecordBatch` as its
    /// own stream-entry field.
    #[serde(default)]
    pub fields: Vec<RedisFieldSpec>,
    /// How long `XREAD BLOCK` waits for a new entry before returning
    /// empty — a timeout here just means "no new entry yet, try
    /// again", not an error, same contract MQTT's `idle_timeout_ms`
    /// documents.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Max entries read per `XREAD` call (`COUNT`).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Timeout in seconds for connecting.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RedisStartingPosition {
    #[default]
    Latest,
    Earliest,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RedisFieldSpec {
    pub name: String,
    /// One of `int64`, `float64`, `boolean`, `utf8` — the same four
    /// primitives `RecordBatchBuilder::from_json_rows` (`nexus-core`)
    /// supports.
    pub r#type: String,
}

impl RedisConnectorConfig {
    pub fn validate(&self) -> Result<(), nexus_core::NexusError> {
        if self.url.trim().is_empty() {
            return Err(nexus_core::NexusError::Connector(
                "redis: url is required".to_string(),
            ));
        }
        if self.stream_key.trim().is_empty() {
            return Err(nexus_core::NexusError::Connector(
                "redis: stream_key is required".to_string(),
            ));
        }
        Ok(())
    }
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
