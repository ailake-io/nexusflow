use serde::Deserialize;

/// Delta Lake sink/source (Marco 6 — `deltalake` crate). `table_uri` is a
/// local path or `file://` URI; ADLS/S3/GCS work the same way via
/// deltalake's own storage backends but aren't exercised here.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DeltaConnectorConfig {
    /// Local directory path or `file://` URI where the Delta table lives —
    /// created automatically on first write if it doesn't exist yet
    /// (ADLS/S3/GCS URIs work too via deltalake's own storage backends,
    /// not exercised here).
    pub table_uri: String,
    /// Column used to upsert on write.
    pub primary_key: String,
}
