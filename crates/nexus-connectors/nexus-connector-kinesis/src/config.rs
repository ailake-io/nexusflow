use nexus_core::NexusError;
use serde::Deserialize;

/// Configuration for the Kinesis Data Streams source. Auth is explicit
/// static credentials in the config (`access_key_id`/
/// `secret_access_key`/`session_token`) — same "credential in config,
/// not ambient environment/IAM role" contract every cloud connector in
/// this workspace already follows (GA4, Vertex AI, ...).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct KinesisConnectorConfig {
    pub stream_name: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    /// Explicit column projection — Kinesis records are an opaque byte
    /// blob (assumed JSON in v1), there's no schema to introspect the
    /// way a table-backed source has.
    pub fields: Vec<KinesisFieldSpec>,
    #[serde(default)]
    pub starting_position: StartingPosition,
    /// Kinesis rate-limits `GetRecords` to ~5 requests/second per
    /// shard — default kept safely above that per-shard budget.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_max_records_per_poll")]
    pub max_records_per_poll: i32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl KinesisConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.stream_name.trim().is_empty() {
            return Err(NexusError::Connector(
                "kinesis: stream_name is required".to_string(),
            ));
        }
        if self.region.trim().is_empty() {
            return Err(NexusError::Connector(
                "kinesis: region is required".to_string(),
            ));
        }
        if self.access_key_id.trim().is_empty() {
            return Err(NexusError::Connector(
                "kinesis: access_key_id is required".to_string(),
            ));
        }
        if self.secret_access_key.trim().is_empty() {
            return Err(NexusError::Connector(
                "kinesis: secret_access_key is required".to_string(),
            ));
        }
        if self.fields.is_empty() {
            return Err(NexusError::Connector(
                "kinesis: at least one field is required".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct KinesisFieldSpec {
    pub name: String,
    /// One of `int64`, `float64`, `boolean`, `utf8` — the same four
    /// primitives `RecordBatchBuilder::from_json_rows` (`nexus-core`)
    /// supports.
    pub r#type: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartingPosition {
    TrimHorizon,
    #[default]
    Latest,
}

fn default_poll_interval_ms() -> u64 {
    1000
}

fn default_max_records_per_poll() -> i32 {
    100
}

fn default_timeout_seconds() -> u64 {
    30
}
