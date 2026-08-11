use serde::Deserialize;

/// Iceberg table format version to create new tables with. Only applies at
/// table creation time — an already-existing table keeps whatever version
/// it was created with (`ensure_table`'s `load_table` branch never touches
/// it). Defaults to V2, the still-most-widely-supported spec version;
/// pick V3 explicitly to get V3-only features as they land upstream (row
/// lineage, deletion vectors, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum IcebergFormatVersion {
    #[default]
    V2,
    V3,
}

/// Iceberg sink/source (Marco 6 — `iceberg`/`iceberg-catalog-sql` crates).
/// `catalog_uri` is a SQLite URI (e.g. `sqlite:///abs/path/catalog.db?mode=rwc`)
/// backing the catalog metadata; `warehouse_location` is a local `file://`
/// path where data files are written. Both embedded — no external metastore
/// or object store server required, same shape as the other Marco 6/AI-Lake
/// connectors.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct IcebergConnectorConfig {
    /// SQLite URI backing the catalog metadata, e.g.
    /// `"sqlite:///abs/path/catalog.db?mode=rwc"` — created automatically
    /// if it doesn't exist yet (`mode=rwc`).
    pub catalog_uri: String,
    /// Local `file://` path where Iceberg data files are written —
    /// created automatically if it doesn't exist yet.
    pub warehouse_location: String,
    /// Iceberg namespace (like a database/schema) — created automatically
    /// if it doesn't exist yet.
    pub namespace: String,
    /// Table name within `namespace` — created automatically on first
    /// write if it doesn't exist yet, using `format_version`.
    pub table: String,
    /// Format version for a newly-created table — ignored for a table
    /// that already exists.
    #[serde(default)]
    pub format_version: IcebergFormatVersion,
    /// Timeout in seconds for each catalog/table call — both the SQLite
    /// catalog and local warehouse are embedded today, but this still
    /// guards against a locked catalog file or a future remote storage
    /// backend stalling the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Native CDC source for Iceberg, via manual manifest diffing — separate
/// connector name (`"iceberg-cdc"`) from `"iceberg"`, same convention as
/// `postgres-cdc`/`deltalake-cdc`. See `ARCHITECTURE.md §7`.
///
/// **Insert-only.** `IcebergSink` (this same crate) only ever commits
/// `fast_append` snapshots — `iceberg` 0.10.0's `Transaction` API has no
/// committable row-delta/equality-delete action yet, so CDC delete batches
/// are rejected at the sink (see `sink.rs`). Since our own writer never
/// produces an `Overwrite`/`Delete` snapshot, this source only ever emits
/// `Insert` — there is no `Update`/`Delete` to detect from data this system
/// wrote itself. (Contrast `ailake-cdc`: `AilakeSink` *does* commit real
/// equality-deletes today, so that one supports the full `I`/`U`/`D` set.)
/// No `fields` list needed — Iceberg tables are self-describing, same
/// reason the batch `IcebergConnectorConfig` never needed one.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct IcebergCdcConfig {
    pub catalog_uri: String,
    pub warehouse_location: String,
    pub namespace: String,
    pub table: String,
    /// Snapshot id to read changes after (exclusive) — omit to read every
    /// snapshot in the table's history. Static field, not auto-advanced
    /// between runs (same precedent as Kafka's `start_offsets`).
    #[serde(default)]
    pub starting_snapshot_id: Option<i64>,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}
