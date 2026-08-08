use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct OdbcConnectorConfig {
    /// Full ODBC connection string (`Driver={...};Server=...;...`) — the
    /// driver name must match a driver already registered with unixODBC/
    /// the platform's ODBC driver manager on the machine running this.
    pub connection_string: String,
    /// Table name to read from (source) or write to (sink).
    pub table: String,
    /// Column used to partition reads and upsert on write — should be
    /// indexed on the source database.
    pub primary_key: String,
    /// Explicit target schema — generic ODBC introspection varies too much
    /// across legacy drivers to infer types reliably, so the node config
    /// must say what to project each column to.
    pub fields: Vec<OdbcFieldSpec>,
    /// How many rows to fold into a single `RecordBatch` while scanning.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Timeout in seconds for each batch write to fail if the ODBC worker
    /// thread doesn't respond in time (a stalled driver call would
    /// otherwise block the pipeline indefinitely — C15). Only unblocks the
    /// async side: the blocking ODBC call itself, and the OS thread running
    /// it, keeps running regardless (no cross-thread cancellation for raw
    /// ODBC handles).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct OdbcFieldSpec {
    /// Column name as it appears in the source/sink table.
    pub name: String,
    /// Arrow type this column's value gets converted to.
    pub data_type: OdbcDataType,
    /// Whether a NULL value for this column is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a column is projected onto — one of these four primitives,
/// matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OdbcDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_batch_size() -> usize {
    1000
}
