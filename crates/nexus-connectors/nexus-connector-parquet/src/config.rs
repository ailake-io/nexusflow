use nexus_core::NexusError;
use serde::Deserialize;
use std::collections::HashMap;

/// Where the target Parquet file lives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageType {
    /// Local filesystem path (the default).
    #[default]
    Local,
    /// Amazon S3 (`s3://bucket/key`).
    S3,
    /// Google Cloud Storage (`gs://bucket/key`).
    Gcs,
    /// Azure Blob Storage (`az://container/key`).
    Azure,
}

/// Compression codec used when writing Parquet files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    /// No compression.
    None,
    /// Snappy (default, fast, widely supported).
    #[default]
    Snappy,
    /// Gzip.
    Gzip,
    /// LZ4 (legacy frame format).
    Lz4,
    /// LZ4 raw block format.
    Lz4Raw,
    /// Zstandard.
    Zstd,
    /// Brotli.
    Brotli,
}

impl From<ParquetCompression> for parquet::basic::Compression {
    fn from(c: ParquetCompression) -> Self {
        match c {
            ParquetCompression::None => parquet::basic::Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => parquet::basic::Compression::SNAPPY,
            ParquetCompression::Gzip => {
                parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
            }
            ParquetCompression::Lz4 => parquet::basic::Compression::LZ4,
            ParquetCompression::Lz4Raw => parquet::basic::Compression::LZ4_RAW,
            ParquetCompression::Zstd => {
                parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
            }
            ParquetCompression::Brotli => {
                parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default())
            }
        }
    }
}

/// Pure Parquet sink/source (Marco 6 — no Delta/Iceberg metadata layer, just
/// the `parquet` crate directly). `path` is a single Parquet file. Cloud URLs
/// are built from `storage` + `bucket` + `path`; the legacy `uri` field is
/// still accepted for backward compatibility.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct ParquetConnectorConfig {
    /// Legacy location: a local path or a cloud URL (`s3://bucket/key`,
    /// `gs://bucket/key`, `az://container/key`). Takes precedence over
    /// `storage` + `bucket` + `path` when present, for backward compatibility.
    #[serde(default)]
    pub uri: Option<String>,
    /// Path to the Parquet file. For local storage this is a filesystem path;
    /// for cloud storage it is the key inside the bucket/container.
    #[serde(default)]
    pub path: String,
    /// Storage backend. Defaults to local disk; set to `s3`, `gcs` or `azure`
    /// to build a cloud URI from `bucket` + `path`.
    #[serde(default)]
    pub storage: StorageType,
    /// Bucket (S3/GCS) or container (Azure) name. Required for cloud storage
    /// unless a full `uri` is provided.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Cloud region — used for S3 as `aws_region` when `storage_options` is
    /// built.
    #[serde(default)]
    pub region: Option<String>,
    /// Cloud access key / account name — mapped to `aws_access_key_id` for S3,
    /// `azure_storage_account_name` for Azure, and ignored for GCS (use a
    /// service account key through other means or extend `storage_options`
    /// manually in the future).
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Cloud secret / account key — mapped to `aws_secret_access_key` for S3,
    /// `azure_storage_account_key` for Azure, and to `google_service_account`
    /// for GCS.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Custom endpoint — mapped to `aws_endpoint` for S3 and
    /// `azure_storage_endpoint` for Azure.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Compression codec applied when writing the Parquet file.
    #[serde(default)]
    pub compression: ParquetCompression,
    /// Maximum number of rows per row group. `None` leaves the writer default
    /// unchanged.
    #[serde(default)]
    pub row_group_size: Option<usize>,
    /// Column used to identify a row for upsert/delete on write.
    pub primary_key: String,
}

impl ParquetConnectorConfig {
    /// Resolves the effective target URI: legacy `uri` wins, otherwise builds
    /// a cloud URI from `storage` + `bucket` + `path`, or returns `path` for
    /// local storage.
    pub fn uri(&self) -> Result<String, NexusError> {
        if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            return Ok(uri.to_string());
        }

        match self.storage {
            StorageType::Local => Ok(self.path.clone()),
            StorageType::S3 => {
                let bucket = self
                    .bucket
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        NexusError::Connector("S3 storage requires a bucket name".to_string())
                    })?;
                Ok(format!(
                    "s3://{}/{}",
                    bucket,
                    self.path.trim_start_matches('/')
                ))
            }
            StorageType::Gcs => {
                let bucket = self
                    .bucket
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        NexusError::Connector("GCS storage requires a bucket name".to_string())
                    })?;
                Ok(format!(
                    "gs://{}/{}",
                    bucket,
                    self.path.trim_start_matches('/')
                ))
            }
            StorageType::Azure => {
                let container = self
                    .bucket
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        NexusError::Connector("Azure storage requires a container name".to_string())
                    })?;
                Ok(format!(
                    "az://{}/{}",
                    container,
                    self.path.trim_start_matches('/')
                ))
            }
        }
    }

    /// Builds the `storage_options` hash expected by `object_store` for cloud
    /// backends. Local storage ignores these values.
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::new();

        match self.storage {
            StorageType::S3 => {
                if let Some(v) = &self.access_key_id {
                    opts.insert("aws_access_key_id".to_string(), v.clone());
                }
                if let Some(v) = &self.secret_access_key {
                    opts.insert("aws_secret_access_key".to_string(), v.clone());
                }
                if let Some(v) = &self.region {
                    opts.insert("aws_region".to_string(), v.clone());
                }
                if let Some(v) = &self.endpoint {
                    opts.insert("aws_endpoint".to_string(), v.clone());
                }
            }
            StorageType::Gcs => {
                if let Some(v) = &self.secret_access_key {
                    opts.insert("google_service_account".to_string(), v.clone());
                }
            }
            StorageType::Azure => {
                if let Some(v) = &self.access_key_id {
                    opts.insert("azure_storage_account_name".to_string(), v.clone());
                }
                if let Some(v) = &self.secret_access_key {
                    opts.insert("azure_storage_account_key".to_string(), v.clone());
                }
                if let Some(v) = &self.endpoint {
                    opts.insert("azure_storage_endpoint".to_string(), v.clone());
                }
                if let Some(v) = &self.bucket {
                    opts.insert("azure_storage_container_name".to_string(), v.clone());
                }
            }
            StorageType::Local => {}
        }

        opts
    }

    /// Returns the Parquet compression codec selected by this config.
    pub fn compression(&self) -> parquet::basic::Compression {
        self.compression.into()
    }
}
