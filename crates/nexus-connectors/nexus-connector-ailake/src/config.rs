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
/// real delete and `I` for a real insert/update — see `ailake-cdc`'s
/// `deleted_key_sequences` doc comment for how it tells the two apart using
/// `ailake-catalog`/`ailake-query` >=0.1.11's sequence-scoped equality
/// deletes (a delete only masks a data file with a strictly lower sequence
/// number than its own). `CatalogProvider::list_files`/`list_equality_deletes`
/// both take an `Option<SnapshotId>` "as of" parameter — diffing the "as of
/// `starting_snapshot_id`" list against the "as of current" list gives
/// exactly the files/deletes added in between, without walking Avro
/// manifests by hand (unlike `iceberg-cdc`, which had to, since the plain
/// `iceberg` crate only exposes the low-level manifest/manifest-list
/// types, not this "as-of" convenience).
///
/// `AilakeSink::upsert` (a plain batch with no `__opcode`) now emits a real
/// delete-then-insert per batch (two sequential commits — see
/// `AilakeSink::upsert`'s own doc comment), giving true upsert semantics:
/// a second write of the same key replaces the row instead of producing a
/// duplicate. This source doesn't yet distinguish that replacement (an
/// update of a previously-live key) from a genuine first-time insert —
/// telling them apart needs checking whether the key was already live as
/// of the window's *starting* snapshot, which this diff-based approach
/// doesn't do — so both are tagged `I` for now.
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
