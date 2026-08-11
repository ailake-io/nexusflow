use serde::Deserialize;

/// AI-Lake sink/source config. AI-Lake (github.com/ailake-io/ai-lakehouse) is
/// a self-contained Parquet+HNSW vector-native Lakehouse format: tabular
/// data, embeddings, and the vector index all live in one Iceberg-compatible
/// `.parquet` file. `warehouse` is a local filesystem root — AI-Lake's
/// `HadoopCatalog`+`LocalStore` backend, no server/container required (same
/// embedded shape as LanceDB in Marco 5). `namespace`/`table` address one
/// table within it.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct AilakeConnectorConfig {
    /// Local filesystem root for the AI-Lake warehouse — created if it
    /// doesn't exist yet, no server/container required.
    pub warehouse: String,
    /// Namespace (like a database/schema) within the warehouse.
    pub namespace: String,
    /// Table name within `namespace` — created automatically on first
    /// write if it doesn't exist yet.
    pub table: String,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to — indexed with HNSW automatically.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: u32,
    /// Timeout in seconds for each catalog/store call — the warehouse is a
    /// local filesystem today, but this still guards against a locked
    /// catalog file or a slow disk stalling the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Native CDC source for AI-Lake — separate connector name
/// (`"ailake-cdc"`) from `"ailake"`, same convention as `postgres-cdc`/
/// `deltalake-cdc`/`iceberg-cdc`. See `ARCHITECTURE.md §7`.
///
/// Unlike `iceberg-cdc` (Insert-only — the plain `iceberg` crate has no
/// committable delete action yet), `AilakeSink::delete` already commits
/// real Iceberg-compatible equality-deletes, so this one emits `D` for a
/// real delete, `U` when a key is both newly-inserted and newly-deleted in
/// the same read window (an explicit delete followed by a fresh insert of
/// the same key, across two separate writes), and `I` otherwise.
/// `CatalogProvider::list_files`/`list_equality_deletes` both take an
/// `Option<SnapshotId>` "as of" parameter — diffing the "as of
/// `starting_snapshot_id`" list against the "as of current" list gives
/// exactly the files/deletes added in between, without walking Avro
/// manifests by hand (unlike `iceberg-cdc`, which had to, since the plain
/// `iceberg` crate only exposes the low-level manifest/manifest-list
/// types, not this "as-of" convenience).
///
/// Note `AilakeSink::upsert` (a plain batch with no `__opcode`) is a blind
/// append today — it does not delete the row it's replacing first, so two
/// writes of the same key currently produce two physical rows (both `I`
/// here), not the `U` pattern above. That's an `AilakeSink` limitation,
/// not something this source can paper over.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct AilakeCdcConfig {
    pub warehouse: String,
    pub namespace: String,
    pub table: String,
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: u32,
    /// Snapshot id to read changes after (exclusive) — omit to read the
    /// table's entire history. Static field, not auto-advanced between
    /// runs (same precedent as Kafka's `start_offsets`).
    #[serde(default)]
    pub starting_snapshot_id: Option<i64>,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}
