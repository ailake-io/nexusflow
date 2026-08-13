use serde::Deserialize;
use std::collections::HashMap;

/// Object-store credentials and settings used when the Delta table lives on
/// S3, GCS, MinIO or another S3-compatible store. These values are translated
/// into the key/value format expected by `deltalake`/`object_store`.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct StorageOptions {
    /// S3 bucket name. Only needed for object-store paths; local filesystem
    /// tables ignore this field.
    #[serde(default)]
    pub s3_bucket: Option<String>,
    /// AWS region (e.g. `us-east-1`) or the region of an S3-compatible store.
    #[serde(default)]
    pub s3_region: Option<String>,
    /// AWS access key id or the access key for an S3-compatible store.
    #[serde(default)]
    pub s3_access_key_id: Option<String>,
    /// AWS secret access key or the secret key for an S3-compatible store.
    #[serde(default)]
    pub s3_secret_access_key: Option<String>,
    /// Custom S3-compatible endpoint, e.g. `http://localhost:9000` for MinIO.
    #[serde(default)]
    pub s3_endpoint: Option<String>,
}

/// Delta Lake sink/source (Marco 6 — `deltalake` crate). The table can be
/// addressed either through the legacy `table_uri` field or through the
/// separated `path`/`table_name`/`storage_options` fields; both forms are
/// fully supported for backward compatibility.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DeltaConnectorConfig {
    /// Full table URI — a local directory, a `file://` URI, or an object-store
    /// URI such as `s3://bucket/namespace/table`. Kept for backward
    /// compatibility: when this field is non-empty it takes precedence over
    /// `path`/`table_name`/`storage_options`.
    #[serde(default)]
    pub table_uri: String,
    /// Base directory path or object-store prefix where the Delta table lives.
    /// Only used when `table_uri` is empty. The effective table URI is built
    /// by appending `table_name` to this path.
    #[serde(default)]
    pub path: Option<String>,
    /// Table name within `path`. Only used when `table_uri` is empty.
    #[serde(default)]
    pub table_name: Option<String>,
    /// Object-store credentials and settings. Only used when `table_uri` is
    /// empty and the resolved path points at an object store.
    #[serde(default)]
    pub storage_options: StorageOptions,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Timeout in seconds for each call to the table's storage backend
    /// (open, create, write, delete) — matters most for ADLS/S3/GCS URIs,
    /// the only case where a call can actually stall on the network (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl DeltaConnectorConfig {
    /// Resolves the effective Delta table URI.
    ///
    /// If the legacy `table_uri` field is non-empty, it is returned unchanged.
    /// Otherwise the URI is built from `path` and `table_name`.
    pub fn table_uri(&self) -> String {
        if !self.table_uri.is_empty() {
            return self.table_uri.clone();
        }
        let path = self.path.as_deref().unwrap_or("").trim_end_matches('/');
        let name = self.table_name.as_deref().unwrap_or("");
        if path.is_empty() && name.is_empty() {
            return String::new();
        }
        if name.is_empty() {
            return path.to_string();
        }
        if path.is_empty() {
            return name.to_string();
        }
        format!("{}/{}", path, name)
    }

    /// Resolves the table name.
    ///
    /// If `table_name` is set and non-empty, it wins. Otherwise the name is
    /// parsed from the trailing segment of the effective `table_uri`.
    pub fn table_name(&self) -> String {
        if let Some(name) = &self.table_name {
            if !name.is_empty() {
                return name.clone();
            }
        }
        self.table_uri()
            .trim_end_matches('/')
            .split('/')
            .next_back()
            .unwrap_or("")
            .to_string()
    }

    /// Returns `deltalake`/`object_store`-compatible storage options built
    /// from the nested `storage_options` fields.
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(bucket) = &self.storage_options.s3_bucket {
            map.insert("bucket".to_string(), bucket.clone());
        }
        if let Some(region) = &self.storage_options.s3_region {
            map.insert("aws_region".to_string(), region.clone());
        }
        if let Some(key) = &self.storage_options.s3_access_key_id {
            map.insert("aws_access_key_id".to_string(), key.clone());
        }
        if let Some(secret) = &self.storage_options.s3_secret_access_key {
            map.insert("aws_secret_access_key".to_string(), secret.clone());
        }
        if let Some(endpoint) = &self.storage_options.s3_endpoint {
            map.insert("endpoint".to_string(), endpoint.clone());
        }
        map
    }
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
#[cfg(feature = "cdc")]
pub struct DeltaCdcConfig {
    /// Full table URI for the CDC source — a local directory, a `file://` URI,
    /// or an object-store URI. Kept for backward compatibility: when this
    /// field is non-empty it takes precedence over `path`/
    /// `table_name`/`storage_options`.
    #[serde(default)]
    pub table_uri: String,
    /// Base directory path or object-store prefix where the Delta table lives.
    /// Only used when `table_uri` is empty.
    #[serde(default)]
    pub path: Option<String>,
    /// Table name within `path`. Only used when `table_uri` is empty.
    #[serde(default)]
    pub table_name: Option<String>,
    /// Object-store credentials and settings. Only used when `table_uri` is
    /// empty and the resolved path points at an object store.
    #[serde(default)]
    pub storage_options: StorageOptions,
    /// Delta commit version to read changes from (inclusive) — omit to read
    /// from version 0, i.e. every change since `delta.enableChangeDataFeed`
    /// was turned on. Static field, not auto-advanced between runs (same
    /// precedent as Kafka's `start_offsets`) — the destination sink's
    /// idempotent upsert makes re-reading old versions safe, just wasteful
    /// on a large table.
    #[serde(default)]
    pub starting_version: Option<u64>,
    /// Timeout in seconds for each Delta Lake CDC call (connect, read commit
    /// history, scan changed files) — a stalled object-store or filesystem
    /// call would otherwise block the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

#[cfg(feature = "cdc")]
impl DeltaCdcConfig {
    /// Resolves the effective Delta table URI.
    ///
    /// If the legacy `table_uri` field is non-empty, it is returned unchanged.
    /// Otherwise the URI is built from `path` and `table_name`.
    pub fn table_uri(&self) -> String {
        if !self.table_uri.is_empty() {
            return self.table_uri.clone();
        }
        let path = self.path.as_deref().unwrap_or("").trim_end_matches('/');
        let name = self.table_name.as_deref().unwrap_or("");
        if path.is_empty() && name.is_empty() {
            return String::new();
        }
        if name.is_empty() {
            return path.to_string();
        }
        if path.is_empty() {
            return name.to_string();
        }
        format!("{}/{}", path, name)
    }

    /// Resolves the table name.
    ///
    /// If `table_name` is set and non-empty, it wins. Otherwise the name is
    /// parsed from the trailing segment of the effective `table_uri`.
    pub fn table_name(&self) -> String {
        if let Some(name) = &self.table_name {
            if !name.is_empty() {
                return name.clone();
            }
        }
        self.table_uri()
            .trim_end_matches('/')
            .split('/')
            .next_back()
            .unwrap_or("")
            .to_string()
    }

    /// Returns `deltalake`/`object_store`-compatible storage options built
    /// from the nested `storage_options` fields.
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(bucket) = &self.storage_options.s3_bucket {
            map.insert("bucket".to_string(), bucket.clone());
        }
        if let Some(region) = &self.storage_options.s3_region {
            map.insert("aws_region".to_string(), region.clone());
        }
        if let Some(key) = &self.storage_options.s3_access_key_id {
            map.insert("aws_access_key_id".to_string(), key.clone());
        }
        if let Some(secret) = &self.storage_options.s3_secret_access_key {
            map.insert("aws_secret_access_key".to_string(), secret.clone());
        }
        if let Some(endpoint) = &self.storage_options.s3_endpoint {
            map.insert("endpoint".to_string(), endpoint.clone());
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_cfg(table_uri: &str) -> DeltaConnectorConfig {
        DeltaConnectorConfig {
            table_uri: table_uri.to_string(),
            path: None,
            table_name: None,
            storage_options: StorageOptions::default(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        }
    }

    #[test]
    fn legacy_table_uri_takes_precedence() {
        let cfg = DeltaConnectorConfig {
            table_uri: "s3://bucket/legacy".to_string(),
            path: Some("/tmp/data".to_string()),
            table_name: Some("orders".to_string()),
            storage_options: StorageOptions::default(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.table_uri(), "s3://bucket/legacy");
        assert_eq!(cfg.table_name(), "orders");
    }

    #[test]
    fn builds_table_uri_from_path_and_name() {
        let cfg = DeltaConnectorConfig {
            table_uri: String::new(),
            path: Some("/tmp/data".to_string()),
            table_name: Some("orders".to_string()),
            storage_options: StorageOptions::default(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.table_uri(), "/tmp/data/orders");
        assert_eq!(cfg.table_name(), "orders");
    }

    #[test]
    fn parses_table_name_from_legacy_uri() {
        let cfg = batch_cfg("/tmp/data/orders");
        assert_eq!(cfg.table_name(), "orders");
    }

    #[test]
    fn storage_options_maps_s3_fields() {
        let cfg = DeltaConnectorConfig {
            table_uri: String::new(),
            path: Some("s3://bucket".to_string()),
            table_name: Some("orders".to_string()),
            storage_options: StorageOptions {
                s3_bucket: Some("bucket".to_string()),
                s3_region: Some("us-east-1".to_string()),
                s3_access_key_id: Some("key".to_string()),
                s3_secret_access_key: Some("secret".to_string()),
                s3_endpoint: Some("http://localhost:9000".to_string()),
            },
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        let opts = cfg.storage_options();
        assert_eq!(opts.get("bucket"), Some(&"bucket".to_string()));
        assert_eq!(opts.get("aws_region"), Some(&"us-east-1".to_string()));
        assert_eq!(opts.get("aws_access_key_id"), Some(&"key".to_string()));
        assert_eq!(
            opts.get("aws_secret_access_key"),
            Some(&"secret".to_string())
        );
        assert_eq!(
            opts.get("endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
    }

    #[test]
    #[cfg(feature = "cdc")]
    fn cdc_config_table_uri_helper() {
        let cfg = DeltaCdcConfig {
            table_uri: String::new(),
            path: Some("s3://warehouse".to_string()),
            table_name: Some("events".to_string()),
            storage_options: StorageOptions::default(),
            starting_version: None,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.table_uri(), "s3://warehouse/events");
        assert_eq!(cfg.table_name(), "events");
    }

    #[test]
    #[cfg(feature = "cdc")]
    fn cdc_legacy_table_uri_takes_precedence() {
        let cfg = DeltaCdcConfig {
            table_uri: "/overridden/cdc".to_string(),
            path: Some("s3://warehouse".to_string()),
            table_name: Some("events".to_string()),
            storage_options: StorageOptions::default(),
            starting_version: None,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.table_uri(), "/overridden/cdc");
        assert_eq!(cfg.table_name(), "events");
    }
}
