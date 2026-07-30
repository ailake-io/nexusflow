use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize)]
pub struct OdbcConnectorConfig {
    /// Full ODBC connection string (`Driver={...};Server=...;...`).
    pub connection_string: String,
    pub table: String,
    pub primary_key: String,
    /// Explicit target schema — generic ODBC introspection varies too much
    /// across legacy drivers to infer types reliably, so the node config
    /// must say what to project each column to.
    pub fields: Vec<OdbcFieldSpec>,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OdbcFieldSpec {
    pub name: String,
    pub data_type: OdbcDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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
