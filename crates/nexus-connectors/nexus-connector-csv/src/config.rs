use serde::Deserialize;
use std::collections::HashMap;

/// Delimited text file connector (CSV, TSV, or any single-character
/// separator) — source and sink. `uri` can be a local path or an
/// `s3://`/`gs://`/`az://` URL; see `crate::store::open_store` for how each
/// scheme is resolved. Unlike Parquet, delimited text carries no schema of
/// its own (a header row gives column *names*, never types), so `fields`
/// always has to say what each column is.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct CsvConnectorConfig {
    /// Local path (e.g. `/data/events.csv`) or cloud URL (`s3://bucket/key`,
    /// `gs://bucket/key`, `az://container/key`) of a single delimited text
    /// file — created on first write if it doesn't exist yet.
    pub uri: String,
    /// Field separator — `,` for CSV, `\t` for TSV, `;`/`|` or anything
    /// else for a custom-delimited TXT file.
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Whether the file's first line is a header row naming each column (by
    /// `fields`' order) rather than data.
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    /// Explicit target schema, in file-column order — delimited text has no
    /// type information of its own, so the node config must say what to
    /// project each column to.
    pub fields: Vec<CsvFieldSpec>,
    /// Column used to upsert/delete on write — required for the sink side;
    /// ignored by the source.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// How many rows to fold into a single `RecordBatch` while scanning.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Passed straight through to `object_store`'s cloud builders — e.g.
    /// `aws_access_key_id`/`aws_secret_access_key`/`aws_region` for `s3://`,
    /// `google_service_account`/`google_service_account_key` for `gs://`,
    /// `azure_storage_account_name`/`azure_storage_account_key` for `az://`.
    /// Ignored for local paths. See the `object_store` crate's per-backend
    /// `ConfigKey` docs for the full option list per scheme.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Timeout in seconds for each call to the object store — matters most
    /// for cloud URLs, the only case where a call can actually stall on the
    /// network (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
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
