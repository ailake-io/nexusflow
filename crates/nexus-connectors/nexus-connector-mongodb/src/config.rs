use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MongoConnectorConfig {
    /// MongoDB connection string, e.g. `mongodb://user:pass@host:27017` or
    /// a `mongodb+srv://` Atlas URI. Needs read access for a source, write
    /// access for a sink.
    pub uri: String,
    /// Database name within the cluster this connector reads from/writes to.
    pub database: String,
    /// Collection name within `database`.
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
    /// Document field name to project — supports dot notation for nested
    /// fields (e.g. `"address.city"`).
    pub name: String,
    /// Arrow type this field's value gets converted to.
    pub data_type: MongoDataType,
    /// Whether a missing/null value for this field is allowed — if false
    /// and the document lacks it, that row fails instead of being written
    /// with a null.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a document field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
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
