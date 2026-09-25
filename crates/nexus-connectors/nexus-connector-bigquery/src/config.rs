use nexus_core::NexusError;
use serde::Deserialize;

/// How to authenticate to BigQuery. Option key names below are the real
/// ones the official ADBC driver exposes — confirmed by reading
/// `adbc_driver_bigquery/__init__.py`'s `DatabaseOptions` enum straight
/// out of the `adbc-driver-bigquery` PyPI wheel (2026-08-18), not guessed.
///
/// The driver actually supports 4 auth types (`auth_bigquery` = ambient
/// Application Default Credentials, `json_credential_file`,
/// `json_credential_string`, `user_authentication` = OAuth client_id/
/// secret/refresh_token) — v1 only implements `ServiceAccountJson`
/// (maps to `json_credential_string`, inline JSON rather than a file
/// path so nothing needs mounting into the container). ADC and OAuth are
/// real, documented fast-follows, not a design dead end.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum BigqueryAuth {
    /// `adbc.bigquery.sql.auth_type = "auth_type.json_credential_string"`
    /// — the JSON key content of a GCP service account, inline (not a
    /// file path).
    ServiceAccountJson { credentials_json: String },
}

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct BigqueryConnectorConfig {
    /// GCP project ID.
    pub project_id: String,
    /// Dataset ID within the project.
    pub dataset_id: String,
    /// BigQuery location/region for the dataset (e.g. `US`, `EU`).
    /// Optional — the driver defaults if unset.
    #[serde(default)]
    pub location: Option<String>,
    pub auth: BigqueryAuth,
    /// Table name (unqualified — combined with `project_id`/`dataset_id`
    /// to build the fully-qualified `` `project.dataset.table` `` form
    /// used in every query).
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// side; ignored by the source. Upserts use `MERGE INTO` (BigQuery
    /// supports it natively as a standard SQL DML statement — the ADBC
    /// driver's own bulk-ingest options are query-job-oriented, not a
    /// generic row-insert bulk path like Snowflake's).
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for connecting and for each query. Defaults to
    /// `60` — BigQuery queries run as async jobs and routinely take
    /// longer than the `30`s default most other connectors in this
    /// workspace use.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ADBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl BigqueryConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.project_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "bigquery: project_id is required".to_string(),
            ));
        }
        if self.dataset_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "bigquery: dataset_id is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "bigquery: table is required".to_string(),
            ));
        }
        match &self.auth {
            BigqueryAuth::ServiceAccountJson { credentials_json } => {
                if credentials_json.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "bigquery: credentials_json is required".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn default_timeout_seconds() -> u64 {
    60
}
