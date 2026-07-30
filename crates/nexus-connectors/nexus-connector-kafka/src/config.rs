use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// This is a basic consumer: each message's payload is decoded as JSON and
/// projected onto `fields`. It's the foundation Marco 4 builds the Debezium
/// envelope mode on top of (see IMPLEMENTATION_PLAN.md Marco 4) — no opcode
/// handling here yet.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConnectorConfig {
    pub bootstrap_servers: String,
    pub topic: String,
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
