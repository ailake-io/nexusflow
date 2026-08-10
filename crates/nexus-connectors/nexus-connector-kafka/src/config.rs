use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// Generic Kafka consumer: each message's payload is decoded as JSON and
/// projected onto `fields`. CDC is covered natively per-database instead
/// (`postgres-cdc`/`mongodb-cdc`/`mysql-cdc`) — see ARCHITECTURE.md §7.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct KafkaConnectorConfig {
    /// Comma-separated `host:port` list of Kafka brokers to bootstrap from,
    /// e.g. `"broker1:9092,broker2:9092"`.
    pub bootstrap_servers: String,
    /// Topic to consume from.
    pub topic: String,
    /// Consumer group id — controls offset tracking on the broker side;
    /// reuse the same id across runs of the same pipeline to resume from
    /// where the group last committed.
    pub group_id: String,
    /// Explicit target schema — a JSON message payload carries no fixed
    /// schema of its own, so the node config must say what to project each
    /// field to.
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
    /// Explicit per-partition start offsets for resume (checkpoint replay) —
    /// see `checkpoint_store` (already generic per `(pipeline_id,
    /// partition_id)` since Marco 1). Absent partitions fall back to
    /// `auto.offset.reset = earliest`.
    #[serde(default)]
    pub start_offsets: std::collections::HashMap<i32, i64>,
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

fn default_batch_size() -> usize {
    500
}

fn default_poll_timeout_ms() -> u64 {
    2000
}

fn default_max_messages() -> usize {
    100_000
}
