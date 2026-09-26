use nexus_core::NexusError;
use serde::Deserialize;
use std::collections::HashMap;

/// Storage backend used to reach the `.xlsx` file. Same shape as
/// `nexus-connector-csv`'s `StorageType` — duplicated on purpose, this
/// crate can't depend on an OSS crate's internal types across the
/// public/private repo boundary (`LICENSING.md §3`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageType {
    /// Local filesystem path (default).
    #[default]
    Local,
    /// Amazon S3 (or S3-compatible) object storage.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
}

/// Excel (`.xlsx`) connector — source and sink. Split config fields (not a
/// single `uri`) for local-or-cloud, same UX as `csv`'s `storage`/`path`/
/// `bucket` toggle.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ExcelConnectorConfig {
    /// Storage backend. Defaults to `local`.
    #[serde(default)]
    pub storage: StorageType,
    /// Local file path, or cloud object key inside `bucket` for S3/GCS/
    /// Azure.
    #[serde(default)]
    pub path: String,
    /// Bucket (S3/GCS) or container (Azure) name. Required for cloud
    /// backends.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Cloud region, mainly for S3 (e.g. `us-east-1`).
    #[serde(default)]
    pub region: Option<String>,
    /// Access key / service account / storage account name, depending on
    /// the backend.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret key / service account key / storage account key, depending
    /// on the backend.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Custom endpoint URL (e.g. for MinIO or localstack).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Sheet to read/write by name. Takes precedence over `sheet_index`
    /// when set.
    #[serde(default)]
    pub sheet_name: Option<String>,
    /// Sheet to read/write by 0-based index. Ignored when `sheet_name` is
    /// set. Defaults to `0` (first sheet).
    #[serde(default)]
    pub sheet_index: usize,
    /// Whether the first row of the sheet names each column (by `fields`'
    /// order, or by literal header text when `fields` is empty) rather
    /// than data. Defaults to `true`.
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    /// Explicit target schema, in column order. Optional — unlike `csv`,
    /// calamine cells already carry a type (int/float/string/bool/
    /// datetime), so when this is empty the source infers types by
    /// sampling `schema_sample_rows` data rows instead of requiring the
    /// caller to spell every column out. Ignored by the sink, which
    /// always derives its header row and types from the incoming
    /// `RecordBatch` schema.
    #[serde(default)]
    pub fields: Vec<ExcelFieldSpec>,
    /// How many data rows the source samples to infer a column's type
    /// when `fields` is empty. Defaults to `100`.
    #[serde(default = "default_sample_rows")]
    pub schema_sample_rows: usize,
    /// Column used to upsert/delete on write — required for the sink
    /// side; ignored by the source.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Extra key/value options forwarded to `object_store`'s cloud
    /// builders. The connector also injects backend-specific credentials
    /// from `access_key_id`/`secret_access_key`/`region`/`endpoint` when
    /// available, so this map can usually stay empty. Ignored for local
    /// paths.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Timeout in seconds for each call to the object store. Defaults to
    /// `30`.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl ExcelConnectorConfig {
    /// Resolves the effective URI for this config from `storage`/
    /// `bucket`/`path` — see `nexus-connector-csv`'s `uri()` for the exact
    /// same scheme mapping (`s3://`/`gs://`/`az://`).
    pub fn uri(&self) -> Result<String, NexusError> {
        match self.storage {
            StorageType::Local => Ok(self.path.clone()),
            StorageType::S3 => {
                let bucket = self.bucket.as_ref().ok_or_else(|| {
                    NexusError::Connector("excel s3 storage requires bucket".into())
                })?;
                Ok(format!("s3://{bucket}/{}", self.path))
            }
            StorageType::Gcs => {
                let bucket = self.bucket.as_ref().ok_or_else(|| {
                    NexusError::Connector("excel gcs storage requires bucket".into())
                })?;
                Ok(format!("gs://{bucket}/{}", self.path))
            }
            StorageType::Azure => {
                let bucket = self.bucket.as_ref().ok_or_else(|| {
                    NexusError::Connector("excel azure storage requires bucket/container".into())
                })?;
                Ok(format!("az://{bucket}/{}", self.path))
            }
        }
    }

    /// Builds the map of options passed to `object_store` — same mapping
    /// `nexus-connector-csv` uses (`aws_*`/`google_*`/`azure_*` keys).
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut opts = self.storage_options.clone();

        match self.storage {
            StorageType::Local => {}
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
                if let Some(v) = &self.access_key_id {
                    opts.insert("google_service_account".to_string(), v.clone());
                }
                if let Some(v) = &self.secret_access_key {
                    opts.insert("google_service_account_key".to_string(), v.clone());
                }
                if let Some(v) = &self.endpoint {
                    opts.insert("google_storage_endpoint_url".to_string(), v.clone());
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
            }
        }

        opts
    }
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ExcelFieldSpec {
    /// Column name — must match the header row if `has_header` is true.
    pub name: String,
    /// Arrow type this column's value gets converted to.
    pub data_type: ExcelDataType,
    /// Whether an empty cell for this column is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a column is projected onto. `DateTime` cells are converted
/// to an ISO-ish string (`YYYY-MM-DDTHH:MM:SS`) and always land as `Utf8`
/// — no separate variant, keeps Arrow-side handling to four primitives
/// like `csv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExcelDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_has_header() -> bool {
    true
}

fn default_sample_rows() -> usize {
    100
}

fn default_timeout_seconds() -> u64 {
    30
}
