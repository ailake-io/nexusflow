use serde::Deserialize;

/// Delta Lake sink/source (Marco 6 — `deltalake` crate). `table_uri` is a
/// local path or `file://` URI; ADLS/S3/GCS work the same way via
/// deltalake's own storage backends but aren't exercised here.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DeltaConnectorConfig {
    /// Local directory path or `file://` URI where the Delta table lives —
    /// created automatically on first write if it doesn't exist yet
    /// (ADLS/S3/GCS URIs work too via deltalake's own storage backends,
    /// not exercised here).
    pub table_uri: String,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Timeout in seconds for each call to the table's storage backend
    /// (open, create, write, delete) — matters most for ADLS/S3/GCS URIs,
    /// the only case where a call can actually stall on the network (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Native CDC source for Delta Lake's built-in Change Data Feed — a separate
/// connector name (`"deltalake-cdc"`) from `"deltalake"` rather than a mode
/// flag, same convention as `postgres-cdc`. See `ARCHITECTURE.md §7`.
///
/// Unlike Postgres/MongoDB/MySQL CDC, no `fields` list is needed here: a
/// Delta table is self-describing (declared schema in its own metadata,
/// same reason `DeltaConnectorConfig`'s batch source never needed one
/// either) — the change feed's data columns already carry the table's real
/// Arrow types, nothing to coerce from a wire format.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DeltaCdcConfig {
    /// Same shape as the batch connector's `table_uri`.
    pub table_uri: String,
    /// Delta commit version to read changes from (inclusive) — omit to read
    /// from version 0, i.e. every change since `delta.enableChangeDataFeed`
    /// was turned on. Static field, not auto-advanced between runs (same
    /// precedent as Kafka's `start_offsets`) — the destination sink's
    /// idempotent upsert makes re-reading old versions safe, just wasteful
    /// on a large table.
    #[serde(default)]
    pub starting_version: Option<u64>,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}
