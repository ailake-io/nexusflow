use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresConnectorConfig {
    /// Full `postgresql://user:pass@host:port/db` URI. The role needs
    /// SELECT on the source table, or INSERT/UPDATE on the sink table.
    pub uri: String,
    /// Table name to read from (source) or write to (sink) — no schema
    /// prefix needed unless the table isn't in the connection's default
    /// `search_path`.
    pub table: String,
    /// Column used to partition reads by range and to upsert on write —
    /// must be an indexed, orderable column (integer/UUID/timestamp).
    pub primary_key: String,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — the
    /// driver is a blocking FFI call run via `spawn_blocking`, so a stalled
    /// connection would otherwise block that call forever (though the
    /// underlying blocking thread keeps running regardless — no
    /// cancellation for in-flight libpq/ADBC calls) (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Config for the native logical-replication CDC source (`"postgres-cdc"`) —
/// a separate connector name from `"postgres"` rather than a mode flag, so
/// the batch connector's config/behavior never changes. See
/// `ARCHITECTURE.md §7`.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresCdcConfig {
    /// Same `postgres://user:pass@host:port/db` shape as the batch
    /// connector — `?replication=database` is appended automatically, no
    /// need to include it here.
    pub uri: String,
    /// Table this connector reads changes for. The publication (see
    /// `publication_name`) must already cover this table — run
    /// `CREATE PUBLICATION <publication_name> FOR TABLE <table>` by hand
    /// once before starting this connector; it isn't created automatically.
    pub table: String,
    pub publication_name: String,
    /// Replication slot name — created automatically on first connect if it
    /// doesn't exist yet. Reconnecting later with the same name resumes
    /// from where this connector last left off: Postgres tracks the
    /// confirmed position server-side, so there's no separate LSN/offset to
    /// persist on the nexus-server side for this to work.
    pub slot_name: String,
    /// Target schema for each change event's row — same 4-primitive-type
    /// ceiling as every other bridging connector (Kafka/MongoDB); Postgres
    /// column types beyond these aren't supported yet.
    pub fields: Vec<PostgresCdcFieldSpec>,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresCdcFieldSpec {
    pub name: String,
    pub data_type: PostgresCdcDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PostgresCdcDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}
