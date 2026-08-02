use serde::Deserialize;

/// Delta Lake sink/source (Marco 6 — `deltalake` crate). `table_uri` is a
/// local path or `file://` URI; ADLS/S3/GCS work the same way via
/// deltalake's own storage backends but aren't exercised here.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DeltaConnectorConfig {
    pub table_uri: String,
    pub primary_key: String,
}
