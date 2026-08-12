use serde::Deserialize;

/// Object-store storage options for LanceDB.
///
/// Only required when the database lives on S3, GCS or Azure rather than on
/// local disk. For local deployments leave every field empty and set `path`
/// instead.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct LanceDbStorageOptions {
    /// S3 bucket name. When filled, `connection_uri()` builds an `s3://`
    /// URI by combining this bucket with `path`.
    #[serde(default)]
    pub s3_bucket: Option<String>,
    /// AWS region, e.g. `"us-east-1"`. Falls back to the standard AWS
    /// provider chain when omitted.
    #[serde(default)]
    pub s3_region: Option<String>,
    /// AWS access key ID. Prefer IAM roles or environment variables in
    /// production; this field is intended for local development and tests.
    #[serde(default)]
    pub s3_access_key: Option<String>,
    /// AWS secret access key. Prefer IAM roles or environment variables in
    /// production; this field is intended for local development and tests.
    #[serde(default)]
    pub s3_secret_key: Option<String>,
    /// Custom S3-compatible endpoint, e.g. `"http://localhost:9000"` for
    /// MinIO or LocalStack.
    #[serde(default)]
    pub s3_endpoint: Option<String>,
}

/// AI Lakehouse sink #3 (ROADMAP.md Fase 5 order). LanceDB is embedded —
/// the connection target is a local path (or object-store URI); no server to
/// run. See ARCHITECTURE.md §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct LanceDbConnectorConfig {
    /// Deprecated legacy field. Local directory path or object-store URI where
    /// LanceDB stores its data — created if it does not exist yet. Kept for
    /// backward compatibility; if filled it takes precedence over `path` and
    /// `storage_options`. Prefer `path` + `storage_options` for new pipelines.
    #[serde(default)]
    pub uri: Option<String>,
    /// Local directory path where LanceDB stores its data. Created if it does
    /// not exist yet. Use this for local deployments, or combine it with
    /// `storage_options.s3_bucket` to build an S3-backed URI.
    #[serde(default)]
    pub path: Option<String>,
    /// Object-store credentials and location hints. Required when the database
    /// lives on S3 / GCS / Azure rather than on local disk.
    #[serde(default)]
    pub storage_options: LanceDbStorageOptions,
    /// Deprecated legacy field. Table name within the database. Kept for
    /// backward compatibility; if filled it takes precedence over
    /// `table_name`. Prefer `table_name` for new pipelines.
    #[serde(default)]
    pub table: Option<String>,
    /// Table name within the database — created automatically on first write
    /// if it does not exist yet.
    #[serde(default)]
    pub table_name: Option<String>,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to.
    pub embedding_column: String,
    /// Vector size — must match the embedding column's actual length.
    pub dimension: usize,
    /// Timeout in seconds for each call to LanceDB — matters most when the
    /// connection URI points at an object store rather than local disk, the
    /// only case where a call can actually stall on the network (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_timeout_seconds() -> u64 {
    30
}

impl LanceDbConnectorConfig {
    /// Returns the connection URI to hand to `lancedb::connect`.
    ///
    /// Priority:
    /// 1. The legacy `uri` field, if filled.
    /// 2. An object-store URI built from `storage_options.s3_bucket` + `path`.
    /// 3. The local `path` field.
    pub fn connection_uri(&self) -> String {
        if let Some(uri) = &self.uri {
            return uri.clone();
        }

        if let Some(bucket) = &self.storage_options.s3_bucket {
            let path = self
                .path
                .as_deref()
                .unwrap_or_default()
                .trim_start_matches('/');
            if path.is_empty() {
                return format!("s3://{bucket}");
            }
            return format!("s3://{bucket}/{path}");
        }

        self.path.clone().unwrap_or_default()
    }

    /// Returns the table name to open or create.
    ///
    /// Priority:
    /// 1. The legacy `table` field, if filled.
    /// 2. The `table_name` field.
    pub fn table_name(&self) -> String {
        self.table
            .clone()
            .or_else(|| self.table_name.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_uri(uri: &str) -> LanceDbConnectorConfig {
        LanceDbConnectorConfig {
            uri: Some(uri.to_string()),
            path: None,
            storage_options: LanceDbStorageOptions::default(),
            table: Some("legacy_table".to_string()),
            table_name: Some("new_table".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        }
    }

    #[test]
    fn connection_uri_prefers_legacy_uri() {
        let cfg = cfg_with_uri("s3://bucket/db");
        assert_eq!(cfg.connection_uri(), "s3://bucket/db");
    }

    #[test]
    fn connection_uri_builds_s3_uri_from_bucket_and_path() {
        let cfg = LanceDbConnectorConfig {
            uri: None,
            path: Some("data/lancedb".to_string()),
            storage_options: LanceDbStorageOptions {
                s3_bucket: Some("my-bucket".to_string()),
                ..Default::default()
            },
            table: None,
            table_name: Some("docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_uri(), "s3://my-bucket/data/lancedb");
    }

    #[test]
    fn connection_uri_builds_s3_uri_without_path() {
        let cfg = LanceDbConnectorConfig {
            uri: None,
            path: None,
            storage_options: LanceDbStorageOptions {
                s3_bucket: Some("my-bucket".to_string()),
                ..Default::default()
            },
            table: None,
            table_name: Some("docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_uri(), "s3://my-bucket");
    }

    #[test]
    fn connection_uri_falls_back_to_local_path() {
        let cfg = LanceDbConnectorConfig {
            uri: None,
            path: Some("/tmp/lancedb".to_string()),
            storage_options: LanceDbStorageOptions::default(),
            table: None,
            table_name: Some("docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_uri(), "/tmp/lancedb");
    }

    #[test]
    fn table_name_prefers_legacy_table() {
        let cfg = cfg_with_uri("ignored");
        assert_eq!(cfg.table_name(), "legacy_table");
    }

    #[test]
    fn table_name_falls_back_to_table_name_field() {
        let cfg = LanceDbConnectorConfig {
            uri: None,
            path: Some("/tmp/lancedb".to_string()),
            storage_options: LanceDbStorageOptions::default(),
            table: None,
            table_name: Some("docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.table_name(), "docs");
    }

    #[test]
    fn legacy_json_without_new_fields_still_deserializes() {
        let json = serde_json::json!({
            "uri": "/tmp/lancedb",
            "table": "docs",
            "primary_key": "id",
            "embedding_column": "embedding",
            "dimension": 384
        });
        let cfg: LanceDbConnectorConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.connection_uri(), "/tmp/lancedb");
        assert_eq!(cfg.table_name(), "docs");
    }
}
