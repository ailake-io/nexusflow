use serde::Deserialize;
use std::collections::HashMap;

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

/// S3-compatible object-storage options for lakehouse connectors.
///
/// These fields map to the standard `s3.*` properties used by Iceberg's
/// object-store integrations (e.g. `s3.bucket`, `s3.region`,
/// `s3.access-key-id`, `s3.secret-access-key`, `s3.endpoint`). They are only
/// required when the warehouse or catalog lives on S3, MinIO, R2, Wasabi, or
/// another S3-compatible store; for the default local-warehouse/SQLite-catalog
/// setup they can all be left empty.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct StorageOptions {
    /// S3 bucket that contains the Iceberg warehouse or table data.
    /// Example: `my-iceberg-warehouse`.
    #[serde(default)]
    pub s3_bucket: Option<String>,
    /// AWS/S3 region where the bucket lives.
    /// Example: `us-east-1`.
    #[serde(default)]
    pub s3_region: Option<String>,
    /// Access key ID for the S3-compatible store.
    /// Keep secret in production — this value is serialized in pipeline
    /// configurations just like any other connector field.
    #[serde(default)]
    pub s3_access_key: Option<String>,
    /// Secret access key for the S3-compatible store.
    /// Keep secret in production — this value is serialized in pipeline
    /// configurations just like any other connector field.
    #[serde(default)]
    pub s3_secret_key: Option<String>,
    /// Custom endpoint URL for S3-compatible stores such as MinIO.
    /// Example: `http://localhost:9000`.
    #[serde(default)]
    pub s3_endpoint: Option<String>,
}

impl StorageOptions {
    /// Returns only the populated S3 properties as a key/value map, using the
    /// conventional `s3.*` names consumed by Iceberg object-store backends.
    /// Empty values are skipped so the map can be passed straight to a
    /// storage builder.
    pub fn as_hash_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(v) = self.s3_bucket.as_ref().filter(|s| !s.is_empty()) {
            map.insert("s3.bucket".to_string(), v.clone());
        }
        if let Some(v) = self.s3_region.as_ref().filter(|s| !s.is_empty()) {
            map.insert("s3.region".to_string(), v.clone());
        }
        if let Some(v) = self.s3_access_key.as_ref().filter(|s| !s.is_empty()) {
            map.insert("s3.access-key-id".to_string(), v.clone());
        }
        if let Some(v) = self.s3_secret_key.as_ref().filter(|s| !s.is_empty()) {
            map.insert("s3.secret-access-key".to_string(), v.clone());
        }
        if let Some(v) = self.s3_endpoint.as_ref().filter(|s| !s.is_empty()) {
            map.insert("s3.endpoint".to_string(), v.clone());
        }
        map
    }
}

/// Iceberg sink/source (Marco 6 — `iceberg`/`iceberg-catalog-sql` crates).
///
/// The connector can be configured either with the legacy combined fields
/// (`catalog_uri`, `warehouse_location`, `namespace`, `table`) or with the
/// newer split fields (`catalog_path`, `warehouse_path`, `namespace_name`,
/// `table_name`). When both are present, the legacy field wins so existing
/// pipelines keep working unchanged. The SQLite catalog and local warehouse are
/// embedded — no external metastore or object-store server is required for the
/// default local setup.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct IcebergConnectorConfig {
    /// Legacy combined catalog URI. Keep using this if you already have a full
    /// URI such as `sqlite:///abs/path/catalog.db?mode=rwc` — it takes
    /// priority over `catalog_path` when both are present.
    ///
    /// For new pipelines prefer `catalog_path` and let the connector build the
    /// SQLite URI automatically.
    #[serde(default)]
    pub catalog_uri: Option<String>,
    /// Filesystem path to the SQLite catalog backing metadata.
    /// Example: `/var/lib/nexus/iceberg/catalog.db`.
    ///
    /// Used only when `catalog_uri` is empty or omitted. The connector
    /// normalizes this into a `sqlite://...?mode=rwc` URI internally.
    #[serde(default)]
    pub catalog_path: Option<String>,
    /// Legacy combined warehouse location. Keep using this if you already have
    /// a full URI such as `file:///abs/path/warehouse` — it takes priority
    /// over `warehouse_path` when both are present.
    #[serde(default)]
    pub warehouse_location: Option<String>,
    /// Filesystem path to the Iceberg warehouse root.
    /// Example: `/var/lib/nexus/iceberg/warehouse`.
    ///
    /// Used only when `warehouse_location` is empty or omitted. The connector
    /// normalizes this into a `file://` URI via `warehouse_location()`.
    #[serde(default)]
    pub warehouse_path: Option<String>,
    /// Legacy namespace (database/schema) identifier. Keep using this if you
    /// already have a value here — it takes priority over `namespace_name`
    /// when both are present.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Namespace (like a database/schema) within the Iceberg catalog.
    /// Created automatically on first write if it does not exist yet.
    ///
    /// Used only when `namespace` is empty or omitted.
    #[serde(default)]
    pub namespace_name: Option<String>,
    /// Legacy table name. Keep using this if you already have a value here —
    /// it takes priority over `table_name` when both are present.
    #[serde(default)]
    pub table: Option<String>,
    /// Table name within `namespace`.
    /// Created automatically on first write if it does not exist yet, using
    /// `format_version`.
    ///
    /// Used only when `table` is empty or omitted.
    #[serde(default)]
    pub table_name: Option<String>,
    /// S3-compatible object-storage options. Only required when the catalog
    /// or warehouse is backed by S3/MinIO/R2/Wasabi; leave empty for the
    /// default local SQLite + local filesystem setup.
    #[serde(default)]
    pub storage_options: StorageOptions,
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

impl IcebergConnectorConfig {
    /// Returns the effective catalog URI, preferring the legacy `catalog_uri`
    /// field. If that is empty or missing, falls back to `catalog_path`.
    pub fn catalog_uri(&self) -> String {
        self.catalog_uri
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| {
                self.catalog_path
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
            })
            .unwrap_or_default()
    }

    /// Returns the effective warehouse location as a URI, preferring the
    /// legacy `warehouse_location` field. If that is empty or missing, falls
    /// back to `warehouse_path` and normalizes it to a `file://` URI when it
    /// does not already have a scheme.
    pub fn warehouse_location(&self) -> String {
        let raw = self
            .warehouse_location
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| {
                self.warehouse_path
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
            })
            .unwrap_or_default();
        if raw.is_empty() || raw.contains("://") {
            raw
        } else {
            format!("file://{raw}")
        }
    }

    /// Returns the effective namespace identifier, preferring the legacy
    /// `namespace` field. If that is empty or missing, falls back to
    /// `namespace_name`.
    pub fn namespace(&self) -> String {
        self.namespace
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| {
                self.namespace_name
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
            })
            .unwrap_or_default()
    }

    /// Returns the effective table name, preferring the legacy `table` field.
    /// If that is empty or missing, falls back to `table_name`.
    pub fn table_name(&self) -> String {
        self.table
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| self.table_name.as_ref().filter(|s| !s.is_empty()).cloned())
            .unwrap_or_default()
    }

    /// Returns the populated S3 storage options as a `s3.*` property map.
    /// Empty values are omitted.
    pub fn storage_options(&self) -> HashMap<String, String> {
        self.storage_options.as_hash_map()
    }
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
    /// Legacy combined catalog URI. Keep using this if you already have a full
    /// URI such as `sqlite:///abs/path/catalog.db?mode=rwc` — it takes
    /// priority over `catalog_path` when both are present.
    #[serde(default)]
    pub catalog_uri: Option<String>,
    /// Filesystem path to the SQLite catalog backing metadata.
    /// Used only when `catalog_uri` is empty or omitted.
    #[serde(default)]
    pub catalog_path: Option<String>,
    /// Legacy combined warehouse location. Keep using this if you already have
    /// a full URI such as `file:///abs/path/warehouse` — it takes priority
    /// over `warehouse_path` when both are present.
    #[serde(default)]
    pub warehouse_location: Option<String>,
    /// Filesystem path to the Iceberg warehouse root.
    /// Used only when `warehouse_location` is empty or omitted.
    #[serde(default)]
    pub warehouse_path: Option<String>,
    /// Legacy namespace identifier. Takes priority over `namespace_name` when
    /// both are present.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Namespace (database/schema) within the Iceberg catalog.
    /// Used only when `namespace` is empty or omitted.
    #[serde(default)]
    pub namespace_name: Option<String>,
    /// Legacy table name. Takes priority over `table_name` when both are
    /// present.
    #[serde(default)]
    pub table: Option<String>,
    /// Table name within `namespace`.
    /// Used only when `table` is empty or omitted.
    #[serde(default)]
    pub table_name: Option<String>,
    /// S3-compatible object-storage options. Only required for S3-backed
    /// catalogs or warehouses; leave empty for the local SQLite + local
    /// filesystem setup.
    #[serde(default)]
    pub storage_options: StorageOptions,
    /// Snapshot id to read changes after (exclusive) — omit to read every
    /// snapshot in the table's history. Static field, not auto-advanced
    /// between runs (same precedent as Kafka's `start_offsets`).
    #[serde(default)]
    pub starting_snapshot_id: Option<i64>,
    /// Timeout in seconds for each Iceberg CDC call (connect to catalog,
    /// read snapshots, scan changed files) — a stalled catalog or filesystem
    /// call would otherwise block the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl IcebergCdcConfig {
    /// Returns the effective catalog URI, preferring the legacy `catalog_uri`
    /// field, then `catalog_path`.
    pub fn catalog_uri(&self) -> String {
        self.catalog_uri
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| {
                self.catalog_path
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
            })
            .unwrap_or_default()
    }

    /// Returns the effective warehouse location as a URI, preferring the
    /// legacy `warehouse_location` field, then `warehouse_path`.
    pub fn warehouse_location(&self) -> String {
        let raw = self
            .warehouse_location
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| {
                self.warehouse_path
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
            })
            .unwrap_or_default();
        if raw.is_empty() || raw.contains("://") {
            raw
        } else {
            format!("file://{raw}")
        }
    }

    /// Returns the effective namespace identifier, preferring the legacy
    /// `namespace` field, then `namespace_name`.
    pub fn namespace(&self) -> String {
        self.namespace
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| {
                self.namespace_name
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
            })
            .unwrap_or_default()
    }

    /// Returns the effective table name, preferring the legacy `table` field,
    /// then `table_name`.
    pub fn table_name(&self) -> String {
        self.table
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| self.table_name.as_ref().filter(|s| !s.is_empty()).cloned())
            .unwrap_or_default()
    }

    /// Returns the populated S3 storage options as a `s3.*` property map.
    pub fn storage_options(&self) -> HashMap<String, String> {
        self.storage_options.as_hash_map()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_cfg() -> IcebergConnectorConfig {
        IcebergConnectorConfig {
            catalog_uri: Some("sqlite:///legacy/catalog.db?mode=rwc".to_string()),
            catalog_path: Some("/new/catalog.db".to_string()),
            warehouse_location: Some("file:///legacy/warehouse".to_string()),
            warehouse_path: Some("/new/warehouse".to_string()),
            namespace: Some("legacy_ns".to_string()),
            namespace_name: Some("new_ns".to_string()),
            table: Some("legacy_table".to_string()),
            table_name: Some("new_table".to_string()),
            storage_options: StorageOptions::default(),
            format_version: IcebergFormatVersion::V2,
            timeout_seconds: 30,
        }
    }

    #[test]
    fn legacy_fields_take_priority() {
        let cfg = batch_cfg();
        assert_eq!(cfg.catalog_uri(), "sqlite:///legacy/catalog.db?mode=rwc");
        assert_eq!(cfg.warehouse_location(), "file:///legacy/warehouse");
        assert_eq!(cfg.namespace(), "legacy_ns");
        assert_eq!(cfg.table_name(), "legacy_table");
    }

    #[test]
    fn separated_fields_are_used_when_legacy_is_empty() {
        let cfg = IcebergConnectorConfig {
            catalog_uri: Some(String::new()),
            catalog_path: Some("/new/catalog.db".to_string()),
            warehouse_location: Some(String::new()),
            warehouse_path: Some("/new/warehouse".to_string()),
            namespace: Some(String::new()),
            namespace_name: Some("new_ns".to_string()),
            table: Some(String::new()),
            table_name: Some("new_table".to_string()),
            storage_options: StorageOptions::default(),
            format_version: IcebergFormatVersion::V2,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.catalog_uri(), "/new/catalog.db");
        assert_eq!(cfg.warehouse_location(), "file:///new/warehouse");
        assert_eq!(cfg.namespace(), "new_ns");
        assert_eq!(cfg.table_name(), "new_table");
    }

    #[test]
    fn warehouse_path_is_normalized_to_file_url() {
        let cfg = IcebergConnectorConfig {
            catalog_uri: None,
            catalog_path: None,
            warehouse_location: None,
            warehouse_path: Some("/data/warehouse".to_string()),
            namespace: None,
            namespace_name: None,
            table: None,
            table_name: None,
            storage_options: StorageOptions::default(),
            format_version: IcebergFormatVersion::V2,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.warehouse_location(), "file:///data/warehouse");
    }

    #[test]
    fn helpers_return_empty_when_unconfigured() {
        let cfg = IcebergConnectorConfig {
            catalog_uri: None,
            catalog_path: None,
            warehouse_location: None,
            warehouse_path: None,
            namespace: None,
            namespace_name: None,
            table: None,
            table_name: None,
            storage_options: StorageOptions::default(),
            format_version: IcebergFormatVersion::V2,
            timeout_seconds: 30,
        };
        assert!(cfg.catalog_uri().is_empty());
        assert!(cfg.warehouse_location().is_empty());
        assert!(cfg.namespace().is_empty());
        assert!(cfg.table_name().is_empty());
    }

    #[test]
    fn storage_options_omit_empty_values() {
        let cfg = IcebergConnectorConfig {
            catalog_uri: None,
            catalog_path: None,
            warehouse_location: None,
            warehouse_path: None,
            namespace: None,
            namespace_name: None,
            table: None,
            table_name: None,
            storage_options: StorageOptions {
                s3_bucket: Some("bucket".to_string()),
                s3_region: Some(String::new()),
                s3_access_key: Some("key".to_string()),
                s3_secret_key: Some(String::new()),
                s3_endpoint: Some("http://minio:9000".to_string()),
            },
            format_version: IcebergFormatVersion::V2,
            timeout_seconds: 30,
        };
        let map = cfg.storage_options();
        assert_eq!(map.get("s3.bucket"), Some(&"bucket".to_string()));
        assert_eq!(map.get("s3.access-key-id"), Some(&"key".to_string()));
        assert_eq!(
            map.get("s3.endpoint"),
            Some(&"http://minio:9000".to_string())
        );
        assert!(!map.contains_key("s3.region"));
        assert!(!map.contains_key("s3.secret-access-key"));
    }

    #[test]
    fn cdc_legacy_fields_take_priority() {
        let cfg = IcebergCdcConfig {
            catalog_uri: Some("sqlite:///legacy/catalog.db?mode=rwc".to_string()),
            catalog_path: Some("/new/catalog.db".to_string()),
            warehouse_location: Some("file:///legacy/warehouse".to_string()),
            warehouse_path: Some("/new/warehouse".to_string()),
            namespace: Some("legacy_ns".to_string()),
            namespace_name: Some("new_ns".to_string()),
            table: Some("legacy_table".to_string()),
            table_name: Some("new_table".to_string()),
            storage_options: StorageOptions::default(),
            starting_snapshot_id: None,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.catalog_uri(), "sqlite:///legacy/catalog.db?mode=rwc");
        assert_eq!(cfg.warehouse_location(), "file:///legacy/warehouse");
        assert_eq!(cfg.namespace(), "legacy_ns");
        assert_eq!(cfg.table_name(), "legacy_table");
    }
}
