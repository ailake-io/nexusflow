use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Auth is a pre-obtained OAuth2 access token (`Authorization: Bearer
/// ...`) supplied directly in config — same "credential in config,
/// not ambient environment" contract every cloud connector in this
/// repo follows. A full OAuth2 authorization-code/refresh-token flow
/// (needed to obtain and keep that token fresh) is out of scope for
/// v1 — the caller is responsible for minting a valid token before
/// configuring this connector, same deferred-complexity precedent as
/// `nexus-connector-kinesis`'s in-memory cursor.
///
/// Google Sheets API v4 has no pagination on `values.get` — a whole
/// range comes back in one response (real API shape, not a
/// simplification), so `range` should bound the read to something
/// that fits in memory, same expectation the `csv` connector already
/// sets for a whole-file read.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct GoogleSheetsConnectorConfig {
    pub access_token: String,
    pub spreadsheet_id: String,
    /// A1 notation range, e.g. `"Sheet1!A1:Z1000"` (source) or
    /// `"Sheet1"`/`"Sheet1!A1"` (sink append target — Sheets picks the
    /// first empty row after existing data in that sheet/column).
    pub range: String,
    /// Whether the first row of `range` is a header row naming
    /// columns (source only) — when `false`, columns are named
    /// `col_0`, `col_1`, etc.
    #[serde(default = "default_has_header_row")]
    pub has_header_row: bool,
    /// API base URL — field, not hardcoded, so tests can point it at
    /// a mock server.
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl GoogleSheetsConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.access_token.trim().is_empty() {
            return Err(NexusError::Connector(
                "google-sheets: access_token is required".to_string(),
            ));
        }
        if self.spreadsheet_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "google-sheets: spreadsheet_id is required".to_string(),
            ));
        }
        if self.range.trim().is_empty() {
            return Err(NexusError::Connector(
                "google-sheets: range is required".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_has_header_row() -> bool {
    true
}

fn default_base_url() -> String {
    "https://sheets.googleapis.com".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> GoogleSheetsConnectorConfig {
        GoogleSheetsConnectorConfig {
            access_token: "ya29.test".into(),
            spreadsheet_id: "abc123".into(),
            range: "Sheet1!A1:Z100".into(),
            has_header_row: true,
            base_url: "https://sheets.googleapis.com".into(),
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
    fn rejects_empty_spreadsheet_id() {
        let mut cfg = base_config();
        cfg.spreadsheet_id = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_range() {
        let mut cfg = base_config();
        cfg.range = "".into();
        assert!(cfg.validate().is_err());
    }
}
