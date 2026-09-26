use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Reads/writes delimited text files (CSV/TSV/custom-delimiter) in a
/// Google Drive folder via the Drive API v3 — same file-format
/// contract as the public repo's `nexus-connector-csv` and
/// `nexus-connector-dropbox` (`arrow-csv`, same 4 supported column
/// types), just fetched via Drive's `files.list`/`files.get`/
/// `files.create` instead of a local path, `object_store` cloud URL,
/// or the Dropbox API. `folder_id` may resolve to one file or many —
/// the source concatenates every file in the folder (sorted by
/// name), same "directory of files" semantics `nexus-connector-csv`'s
/// local path and `nexus-connector-dropbox`'s folder already have.
///
/// Auth is a pre-obtained OAuth2 access token, same "credential in
/// config, not ambient environment, token refresh out of scope for
/// v1" contract `nexus-connector-google-sheets`/`dropbox` document.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct GoogleDriveConnectorConfig {
    pub access_token: String,
    /// Drive folder id (the long id from the folder's URL, not its
    /// display name — Drive's own identifier, real API contract).
    pub folder_id: String,
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    #[serde(default = "default_quote")]
    pub quote: char,
    #[serde(default)]
    pub escape: Option<char>,
    /// Explicit target schema — left empty (source only), the
    /// connector samples the first file and infers one, same
    /// approach `nexus-connector-csv`'s `schema::infer_schema` uses.
    #[serde(default)]
    pub fields: Vec<GoogleDriveFieldSpec>,
    #[serde(default = "default_schema_sample_rows")]
    pub schema_sample_rows: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// API host for `files.list`/`files.get`/`files.create` (real
    /// default `https://www.googleapis.com`) — field, not hardcoded,
    /// so tests can point it at a mock server.
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    /// Host for the media-upload endpoint (real Drive API also uses
    /// `www.googleapis.com`, just under an `/upload/` path prefix —
    /// kept as its own field anyway, same two-host shape
    /// `nexus-connector-dropbox` has, for a consistent test-override
    /// story even though Drive's two hosts happen to be identical).
    #[serde(default = "default_upload_base_url")]
    pub upload_base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct GoogleDriveFieldSpec {
    pub name: String,
    pub data_type: GoogleDriveDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GoogleDriveDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl GoogleDriveConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "google-drive: access_token is required".to_string(),
            ));
        }
        if self.folder_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "google-drive: folder_id is required".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_delimiter() -> char {
    ','
}

fn default_has_header() -> bool {
    true
}

fn default_quote() -> char {
    '"'
}

fn default_schema_sample_rows() -> usize {
    1000
}

fn default_batch_size() -> usize {
    50000
}

fn default_api_base_url() -> String {
    "https://www.googleapis.com".to_string()
}

fn default_upload_base_url() -> String {
    "https://www.googleapis.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> GoogleDriveConnectorConfig {
        GoogleDriveConnectorConfig {
            access_token: "ya29.test".into(),
            folder_id: "folder123".into(),
            delimiter: ',',
            has_header: true,
            quote: '"',
            escape: None,
            fields: Vec::new(),
            schema_sample_rows: 1000,
            batch_size: 50000,
            api_base_url: "https://www.googleapis.com".into(),
            upload_base_url: "https://www.googleapis.com".into(),
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_access_token() {
        let mut cfg = base_config();
        cfg.access_token = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_folder_id() {
        let mut cfg = base_config();
        cfg.folder_id = "".into();
        assert!(cfg.validate().is_err());
    }
}
