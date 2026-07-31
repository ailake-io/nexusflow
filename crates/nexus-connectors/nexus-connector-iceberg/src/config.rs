use serde::Deserialize;

/// Iceberg table format version to create new tables with. Only applies at
/// table creation time — an already-existing table keeps whatever version
/// it was created with (`ensure_table`'s `load_table` branch never touches
/// it). Defaults to V2, the still-most-widely-supported spec version;
/// pick V3 explicitly to get V3-only features as they land upstream (row
/// lineage, deletion vectors, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
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
#[derive(Debug, Clone, Deserialize)]
pub struct IcebergConnectorConfig {
    pub catalog_uri: String,
    pub warehouse_location: String,
    pub namespace: String,
    pub table: String,
    #[serde(default)]
    pub format_version: IcebergFormatVersion,
}
