use serde::Deserialize;

/// Pure Parquet sink/source (Marco 6 — no Delta/Iceberg metadata layer, just
/// the `parquet` crate directly). `path` is a single local `.parquet` file.
/// CDC-aware upserts/deletes are implemented as read-filter-rewrite since
/// plain Parquet has no update/delete of its own — see `sink.rs`.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ParquetConnectorConfig {
    /// Local path to a single `.parquet` file — created if it doesn't
    /// exist yet; on write, upserts/deletes are done by reading the whole
    /// file, filtering, and rewriting it (no update/delete of its own).
    pub path: String,
    /// Column used to identify a row for upsert/delete on write.
    pub primary_key: String,
}
