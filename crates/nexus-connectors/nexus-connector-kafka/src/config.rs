use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// `envelope: Raw` is the basic consumer: each message's payload is decoded
/// as JSON and projected onto `fields`. `envelope: Debezium` is the Marco 4
/// CDC mode — see ARCHITECTURE.md §7 and `docs/cdc-reference/README.md`.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConnectorConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    /// Explicit target schema — a JSON message payload carries no fixed
    /// schema of its own, so the node config must say what to project each
    /// field to. For `envelope: Debezium`, these describe `before`/`after`
    /// row fields; an extra `__opcode` column is appended automatically.
    pub fields: Vec<KafkaFieldSpec>,
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
    /// Message decoding mode.
    #[serde(default)]
    pub envelope: KafkaEnvelope,
    /// Explicit per-partition start offsets for resume (checkpoint replay) —
    /// see `checkpoint_store` (already generic per `(pipeline_id,
    /// partition_id)` since Marco 1). Absent partitions fall back to
    /// `auto.offset.reset = earliest`.
    #[serde(default)]
    pub start_offsets: std::collections::HashMap<i32, i64>,
}

/// How to decode each Kafka message payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaEnvelope {
    /// Payload is the row itself, JSON-encoded.
    #[default]
    Raw,
    /// Payload is a Debezium change event (`{"payload": {"before", "after",
    /// "op", ...}}`, optionally wrapped in a `"schema"`/`"payload"` envelope
    /// when the Connect `JsonConverter` has `schemas.enable=true`).
    Debezium,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaFieldSpec {
    pub name: String,
    pub data_type: KafkaDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaDataType {
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
