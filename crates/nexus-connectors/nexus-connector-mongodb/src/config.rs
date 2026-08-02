use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MongoConnectorConfig {
    pub uri: String,
    pub database: String,
    pub collection: String,
    /// Document field used as the upsert key on the sink side — see
    /// ARCHITECTURE.md §5 (idempotency is a `Sink` contract, not optional).
    pub primary_key: String,
    /// Explicit target schema — a MongoDB collection carries no fixed schema
    /// of its own, so the node config must say what to project each field to.
    pub fields: Vec<MongoFieldSpec>,
    /// How many documents to fold into a single `RecordBatch` while scanning.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MongoFieldSpec {
    pub name: String,
    pub data_type: MongoDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MongoDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_batch_size() -> usize {
    1000
}
