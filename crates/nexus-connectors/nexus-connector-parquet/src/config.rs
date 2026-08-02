use serde::Deserialize;

/// Pure Parquet sink/source (Marco 6 — no Delta/Iceberg metadata layer, just
/// the `parquet` crate directly). `path` is a single local `.parquet` file.
/// CDC-aware upserts/deletes are implemented as read-filter-rewrite since
/// plain Parquet has no update/delete of its own — see `sink.rs`.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ParquetConnectorConfig {
    pub path: String,
    pub primary_key: String,
}
