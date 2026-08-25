use nexus_core::NexusError;
use serde::Deserialize;
use std::collections::HashMap;

/// Storage backend used to reach the delimited text file.
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

/// Delimited text file connector (CSV, TSV, or any single-character
/// separator) — source and sink.
///
/// The connector accepts either the legacy `uri` field (local path or
/// `s3://`/`gs://`/`az://` URL) **or** the split `storage`/`path`/`bucket`
/// fields. `uri` takes precedence when present so existing canvas configs keep
/// working.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct CsvConnectorConfig {
    /// Legacy single-field URI. Local path (e.g. `/data/events.csv`) or cloud
    /// URL (`s3://bucket/key`, `gs://bucket/key`, `az://container/key`).
    /// When set, it overrides the split `storage`/`path`/`bucket` fields.
    #[serde(default)]
    pub uri: Option<String>,
    /// Storage backend. Defaults to `local`.
    #[serde(default)]
    pub storage: StorageType,
    /// Local file path or cloud object key. Required when `uri` is not used.
    /// For cloud backends this is the key inside the bucket/container.
    #[serde(default)]
    pub path: String,
    /// Bucket (S3/GCS) or container (Azure) name. Required for cloud backends
    /// when `uri` is not used.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Cloud region, mainly for S3 (e.g. `us-east-1`).
    #[serde(default)]
    pub region: Option<String>,
    /// Access key / service account / storage account name, depending on the
    /// backend. Mapped to the object_store option names by `storage_options()`.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret key / service account key / storage account key, depending on the
    /// backend. Mapped to the object_store option names by `storage_options()`.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Custom endpoint URL (e.g. for MinIO or localstack). Mapped to the
    /// backend-specific object_store option.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Field separator — `,` for CSV, `\t` for TSV, `;`/`|` or anything else
    /// for a custom-delimited TXT file. Defaults to `,`.
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Whether the file's first line is a header row naming each column (by
    /// `fields`' order) rather than data. Defaults to `true`.
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    /// Character used to quote fields. Defaults to `"`.
    #[serde(default = "default_quote")]
    pub quote: char,
    /// Optional escape character for quotes inside quoted fields. When unset,
    /// the reader/writer use their default behaviour (doubled quotes by default
    /// for the writer).
    #[serde(default)]
    pub escape: Option<char>,
    /// Explicit target schema, in file-column order. Delimited text has no
    /// type information of its own, so if this is left empty **on the
    /// source side**, the connector samples the first `schema_sample_rows`
    /// rows of the first resolved file and infers one (see
    /// `schema::infer_schema`) — same approach `arrow-csv`'s own
    /// `Format::infer_schema` uses elsewhere in the ecosystem, narrowed to
    /// this connector's 4 supported types (anything else, e.g. a date/time
    /// arrow infers, falls back to `utf8` so the value is never dropped).
    /// The sink side still requires this explicitly — there's no data yet
    /// to sample from when a sink connects.
    #[serde(default)]
    pub fields: Vec<CsvFieldSpec>,
    /// How many rows to sample when inferring `fields` (source side only,
    /// only used when `fields` is empty). Defaults to 1000 — enough to
    /// catch a column that's `int64` for many rows then turns out to need
    /// `utf8` (a stray non-numeric value), without reading a huge file
    /// twice just to guess its schema.
    #[serde(default = "default_schema_sample_rows")]
    pub schema_sample_rows: usize,
    /// Column used to upsert/delete on write — required for the sink side;
    /// ignored by the source.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// How many rows to fold into a single `RecordBatch` while scanning.
    /// Defaults to `1000`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Extra key/value options forwarded to `object_store`'s cloud builders.
    /// The connector also injects backend-specific credentials from
    /// `access_key_id`, `secret_access_key`, `region` and `endpoint` when
    /// available, so this map can be left empty for the common case.
    /// Ignored for local paths.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Timeout in seconds for each call to the object store — matters most
    /// for cloud URLs, the only case where a call can actually stall on the
    /// network (C15). Defaults to `30`.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl CsvConnectorConfig {
    /// Resolves the effective URI for this config.
    ///
    /// * If the legacy `uri` field is set, it is returned unchanged.
    /// * Otherwise a URI is built from `storage`/`bucket`/`path`:
    ///   - `Local` -> `path`
    ///   - `S3` -> `s3://{bucket}/{path}`
    ///   - `Gcs` -> `gs://{bucket}/{path}`
    ///   - `Azure` -> `az://{bucket}/{path}`
    ///
    /// Returns an error if a cloud backend is selected without a bucket.
    pub fn uri(&self) -> Result<String, NexusError> {
        if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            return Ok(uri.to_string());
        }

        match self.storage {
            StorageType::Local => Ok(self.path.clone()),
            StorageType::S3 => {
                let bucket = self.bucket.as_ref().ok_or_else(|| {
                    NexusError::Connector("csv s3 storage requires bucket".into())
                })?;
                Ok(format!("s3://{bucket}/{}", self.path))
            }
            StorageType::Gcs => {
                let bucket = self.bucket.as_ref().ok_or_else(|| {
                    NexusError::Connector("csv gcs storage requires bucket".into())
                })?;
                Ok(format!("gs://{bucket}/{}", self.path))
            }
            StorageType::Azure => {
                let bucket = self.bucket.as_ref().ok_or_else(|| {
                    NexusError::Connector("csv azure storage requires bucket/container".into())
                })?;
                Ok(format!("az://{bucket}/{}", self.path))
            }
        }
    }

    /// Builds the map of options passed to `object_store`.
    ///
    /// Starts from `storage_options` and injects backend-specific keys based on
    /// the split credential/endpoint fields:
    ///
    /// * `S3`: `aws_access_key_id`, `aws_secret_access_key`, `aws_region`,
    ///   `aws_endpoint`.
    /// * `Gcs`: `google_service_account`, `google_service_account_key`,
    ///   `google_storage_endpoint_url`.
    /// * `Azure`: `azure_storage_account_name`, `azure_storage_account_key`,
    ///   `azure_storage_endpoint`.
    /// * `Local`: no injection.
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
pub struct CsvFieldSpec {
    /// Column name — must match the header row if `has_header` is true.
    pub name: String,
    /// Arrow type this column's value gets converted to.
    pub data_type: CsvDataType,
    /// Whether an empty value for this column is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a column is projected onto — one of these four primitives,
/// matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CsvDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_delimiter() -> char {
    ','
}

fn default_has_header() -> bool {
    true
}

fn default_batch_size() -> usize {
    1000
}

fn default_timeout_seconds() -> u64 {
    30
}

fn default_quote() -> char {
    '"'
}

fn default_schema_sample_rows() -> usize {
    1000
}
