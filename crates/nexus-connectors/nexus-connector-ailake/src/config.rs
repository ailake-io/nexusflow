use serde::Deserialize;
use std::collections::HashMap;

/// Storage options for AI-Lake backends that are backed by object storage
/// rather than the default embedded local filesystem. Currently the connector
/// uses `LocalStore`, so these values are collected and exposed for future
/// S3-compatible backends and are ignored by the local implementation.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct AilakeStorageOptions {
    /// S3 bucket that contains the AI-Lake warehouse. Ignored for local
    /// warehouses; used when the warehouse is resolved to an S3-compatible
    /// path.
    #[serde(default)]
    pub s3_bucket: Option<String>,
    /// AWS region (or S3-compatible region) where the bucket lives, e.g.
    /// `"us-east-1"`.
    #[serde(default)]
    pub s3_region: Option<String>,
    /// AWS access key ID (or S3-compatible access key) for authenticating
    /// warehouse reads and writes.
    #[serde(default)]
    pub s3_access_key: Option<String>,
    /// AWS secret access key (or S3-compatible secret key) for authenticating
    /// warehouse reads and writes.
    #[serde(default)]
    pub s3_secret_key: Option<String>,
    /// Custom S3-compatible endpoint, e.g. `"http://localhost:9000"` for
    /// MinIO. Leave unset to use real AWS S3.
    #[serde(default)]
    pub s3_endpoint: Option<String>,
}

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
    /// doesn't exist yet, no server/container required. This is the legacy
    /// field; `warehouse_path` is preferred for new canvas nodes.
    pub warehouse: String,
    /// Optional override for `warehouse`. If set and `warehouse` is empty,
    /// this path is used as the warehouse root instead. Kept as the primary
    /// identifier for SchemaForm/FieldHint tooltip discovery.
    #[serde(default)]
    pub warehouse_path: Option<String>,
    /// Namespace (like a database/schema) within the warehouse. This is the
    /// legacy field; `namespace_name` is preferred for new canvas nodes.
    pub namespace: String,
    /// Optional override for `namespace`. If set and `namespace` is empty,
    /// this name is used as the namespace instead.
    #[serde(default)]
    pub namespace_name: Option<String>,
    /// Table name within `namespace` — created automatically on first
    /// write if it doesn't exist yet. This is the legacy field;
    /// `table_name` is preferred for new canvas nodes.
    pub table: String,
    /// Optional override for `table`. If set and `table` is empty, this name
    /// is used as the table name instead.
    #[serde(default)]
    pub table_name: Option<String>,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to — indexed with HNSW automatically.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: u32,
    /// Object-storage settings for non-local warehouses. Ignored by the
    /// current `LocalStore` backend; reserved for future S3-backed
    /// deployments.
    #[serde(default)]
    pub storage_options: AilakeStorageOptions,
    /// Timeout in seconds for each catalog/store call — the warehouse is a
    /// local filesystem today, but this still guards against a locked
    /// catalog file or a slow disk stalling the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl AilakeConnectorConfig {
    /// Resolves the effective warehouse root. The legacy `warehouse` field
    /// takes priority for backwards compatibility; only if it is empty does
    /// `warehouse_path` contribute.
    pub fn warehouse(&self) -> &str {
        if self.warehouse.is_empty() {
            self.warehouse_path.as_deref().unwrap_or("")
        } else {
            &self.warehouse
        }
    }

    /// Resolves the effective namespace. The legacy `namespace` field takes
    /// priority for backwards compatibility; only if it is empty does
    /// `namespace_name` contribute.
    pub fn namespace(&self) -> &str {
        if self.namespace.is_empty() {
            self.namespace_name.as_deref().unwrap_or("")
        } else {
            &self.namespace
        }
    }

    /// Resolves the effective table name. The legacy `table` field takes
    /// priority for backwards compatibility; only if it is empty does
    /// `table_name` contribute.
    pub fn table_name(&self) -> &str {
        if self.table.is_empty() {
            self.table_name.as_deref().unwrap_or("")
        } else {
            &self.table
        }
    }

    /// Returns the storage options as a key-value map suitable for object-
    /// store SDKs. Empty for local warehouses. Keys follow the de-facto
    /// object-store convention (`bucket`, `region`, `access_key_id`,
    /// `secret_access_key`, `endpoint`).
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        if let Some(bucket) = &self.storage_options.s3_bucket {
            if !bucket.is_empty() {
                opts.insert("bucket".to_string(), bucket.clone());
            }
        }
        if let Some(region) = &self.storage_options.s3_region {
            if !region.is_empty() {
                opts.insert("region".to_string(), region.clone());
            }
        }
        if let Some(access_key) = &self.storage_options.s3_access_key {
            if !access_key.is_empty() {
                opts.insert("access_key_id".to_string(), access_key.clone());
            }
        }
        if let Some(secret_key) = &self.storage_options.s3_secret_key {
            if !secret_key.is_empty() {
                opts.insert("secret_access_key".to_string(), secret_key.clone());
            }
        }
        if let Some(endpoint) = &self.storage_options.s3_endpoint {
            if !endpoint.is_empty() {
                opts.insert("endpoint".to_string(), endpoint.clone());
            }
        }
        opts
    }
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
    /// Local filesystem root for the AI-Lake warehouse. This is the legacy
    /// field; `warehouse_path` is preferred for new canvas nodes.
    pub warehouse: String,
    /// Optional override for `warehouse`. Used only when `warehouse` is
    /// empty.
    #[serde(default)]
    pub warehouse_path: Option<String>,
    /// Namespace (like a database/schema) within the warehouse. This is the
    /// legacy field; `namespace_name` is preferred for new canvas nodes.
    pub namespace: String,
    /// Optional override for `namespace`. Used only when `namespace` is
    /// empty.
    #[serde(default)]
    pub namespace_name: Option<String>,
    /// Table name within `namespace`. This is the legacy field;
    /// `table_name` is preferred for new canvas nodes.
    pub table: String,
    /// Optional override for `table`. Used only when `table` is empty.
    #[serde(default)]
    pub table_name: Option<String>,
    /// Column used to identify rows for CDC deletes and synthetic delete
    /// rows.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` embedding column — used when
    /// reading committed data files back.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: u32,
    /// Object-storage settings for non-local warehouses. Ignored by the
    /// current `LocalStore` backend; reserved for future S3-backed
    /// deployments.
    #[serde(default)]
    pub storage_options: AilakeStorageOptions,
    /// Snapshot id to read changes after (exclusive) — omit to read the
    /// table's entire history. Static field, not auto-advanced between
    /// runs (same precedent as Kafka's `start_offsets`).
    #[serde(default)]
    pub starting_snapshot_id: Option<i64>,
    /// Timeout in seconds for each AI-Lake CDC call (connect to catalog,
    /// read snapshots, scan changed files) — a stalled catalog or filesystem
    /// call would otherwise block the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl AilakeCdcConfig {
    /// Resolves the effective warehouse root. The legacy `warehouse` field
    /// takes priority for backwards compatibility.
    pub fn warehouse(&self) -> &str {
        if self.warehouse.is_empty() {
            self.warehouse_path.as_deref().unwrap_or("")
        } else {
            &self.warehouse
        }
    }

    /// Resolves the effective namespace. The legacy `namespace` field takes
    /// priority for backwards compatibility.
    pub fn namespace(&self) -> &str {
        if self.namespace.is_empty() {
            self.namespace_name.as_deref().unwrap_or("")
        } else {
            &self.namespace
        }
    }

    /// Resolves the effective table name. The legacy `table` field takes
    /// priority for backwards compatibility.
    pub fn table_name(&self) -> &str {
        if self.table.is_empty() {
            self.table_name.as_deref().unwrap_or("")
        } else {
            &self.table
        }
    }

    /// Returns the storage options as a key-value map. Empty for local
    /// warehouses.
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        if let Some(bucket) = &self.storage_options.s3_bucket {
            if !bucket.is_empty() {
                opts.insert("bucket".to_string(), bucket.clone());
            }
        }
        if let Some(region) = &self.storage_options.s3_region {
            if !region.is_empty() {
                opts.insert("region".to_string(), region.clone());
            }
        }
        if let Some(access_key) = &self.storage_options.s3_access_key {
            if !access_key.is_empty() {
                opts.insert("access_key_id".to_string(), access_key.clone());
            }
        }
        if let Some(secret_key) = &self.storage_options.s3_secret_key {
            if !secret_key.is_empty() {
                opts.insert("secret_access_key".to_string(), secret_key.clone());
            }
        }
        if let Some(endpoint) = &self.storage_options.s3_endpoint {
            if !endpoint.is_empty() {
                opts.insert("endpoint".to_string(), endpoint.clone());
            }
        }
        opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_connector_config() -> AilakeConnectorConfig {
        AilakeConnectorConfig {
            warehouse: "/tmp/wh".to_string(),
            warehouse_path: None,
            namespace: "ns".to_string(),
            namespace_name: None,
            table: "docs".to_string(),
            table_name: None,
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            storage_options: AilakeStorageOptions::default(),
            timeout_seconds: 30,
        }
    }

    fn sample_cdc_config() -> AilakeCdcConfig {
        AilakeCdcConfig {
            warehouse: "/tmp/wh".to_string(),
            warehouse_path: None,
            namespace: "ns".to_string(),
            namespace_name: None,
            table: "docs".to_string(),
            table_name: None,
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            storage_options: AilakeStorageOptions::default(),
            starting_snapshot_id: None,
            timeout_seconds: 30,
        }
    }

    #[test]
    fn connector_legacy_fields_take_priority() {
        let cfg = sample_connector_config();
        assert_eq!(cfg.warehouse(), "/tmp/wh");
        assert_eq!(cfg.namespace(), "ns");
        assert_eq!(cfg.table_name(), "docs");
    }

    #[test]
    fn connector_new_fields_used_when_legacy_empty() {
        let cfg = AilakeConnectorConfig {
            warehouse: "".to_string(),
            warehouse_path: Some("/new/wh".to_string()),
            namespace: "".to_string(),
            namespace_name: Some("new_ns".to_string()),
            table: "".to_string(),
            table_name: Some("new_docs".to_string()),
            ..sample_connector_config()
        };
        assert_eq!(cfg.warehouse(), "/new/wh");
        assert_eq!(cfg.namespace(), "new_ns");
        assert_eq!(cfg.table_name(), "new_docs");
    }

    #[test]
    fn connector_legacy_overrides_new_fields() {
        let cfg = AilakeConnectorConfig {
            warehouse: "/old/wh".to_string(),
            warehouse_path: Some("/new/wh".to_string()),
            namespace: "old_ns".to_string(),
            namespace_name: Some("new_ns".to_string()),
            table: "old_docs".to_string(),
            table_name: Some("new_docs".to_string()),
            ..sample_connector_config()
        };
        assert_eq!(cfg.warehouse(), "/old/wh");
        assert_eq!(cfg.namespace(), "old_ns");
        assert_eq!(cfg.table_name(), "old_docs");
    }

    #[test]
    fn connector_storage_options_empty_by_default() {
        let cfg = sample_connector_config();
        assert!(cfg.storage_options().is_empty());
    }

    #[test]
    fn connector_storage_options_collects_s3_settings() {
        let cfg = AilakeConnectorConfig {
            storage_options: AilakeStorageOptions {
                s3_bucket: Some("my-bucket".to_string()),
                s3_region: Some("us-west-2".to_string()),
                s3_access_key: Some("AKIA".to_string()),
                s3_secret_key: Some("secret".to_string()),
                s3_endpoint: Some("http://localhost:9000".to_string()),
            },
            ..sample_connector_config()
        };
        let opts = cfg.storage_options();
        assert_eq!(opts.get("bucket"), Some(&"my-bucket".to_string()));
        assert_eq!(opts.get("region"), Some(&"us-west-2".to_string()));
        assert_eq!(opts.get("access_key_id"), Some(&"AKIA".to_string()));
        assert_eq!(opts.get("secret_access_key"), Some(&"secret".to_string()));
        assert_eq!(
            opts.get("endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
    }

    #[test]
    fn cdc_legacy_fields_take_priority() {
        let cfg = sample_cdc_config();
        assert_eq!(cfg.warehouse(), "/tmp/wh");
        assert_eq!(cfg.namespace(), "ns");
        assert_eq!(cfg.table_name(), "docs");
    }

    #[test]
    fn cdc_new_fields_used_when_legacy_empty() {
        let cfg = AilakeCdcConfig {
            warehouse: "".to_string(),
            warehouse_path: Some("/new/wh".to_string()),
            namespace: "".to_string(),
            namespace_name: Some("new_ns".to_string()),
            table: "".to_string(),
            table_name: Some("new_docs".to_string()),
            ..sample_cdc_config()
        };
        assert_eq!(cfg.warehouse(), "/new/wh");
        assert_eq!(cfg.namespace(), "new_ns");
        assert_eq!(cfg.table_name(), "new_docs");
    }

    #[test]
    fn cdc_storage_options_collects_s3_settings() {
        let cfg = AilakeCdcConfig {
            storage_options: AilakeStorageOptions {
                s3_bucket: Some("cdc-bucket".to_string()),
                s3_region: None,
                s3_access_key: None,
                s3_secret_key: None,
                s3_endpoint: None,
            },
            ..sample_cdc_config()
        };
        let opts = cfg.storage_options();
        assert_eq!(opts.len(), 1);
        assert_eq!(opts.get("bucket"), Some(&"cdc-bucket".to_string()));
    }
}
