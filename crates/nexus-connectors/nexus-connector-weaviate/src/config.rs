use nexus_core::NexusError;
use serde::Deserialize;

/// Configuration for the Weaviate vector sink.
///
/// The class (collection) must already exist — this sink only writes
/// rows, same contract every vector sink in this workspace follows.
/// Auth is `Authorization: Bearer <api_key>` — confirmed the same
/// mechanism works for both Weaviate Cloud and self-hosted (self-
/// hosted can also run with auth disabled).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct WeaviateConnectorConfig {
    /// Base URL, e.g. `https://localhost:8080` or a Weaviate Cloud
    /// cluster URL.
    pub host: String,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Weaviate class (collection) name — the batch objects API's own
    /// field is still called `class`, confirmed real even in recent
    /// versions.
    pub class_name: String,
    /// Column used as the object `id` (must be a valid UUID string —
    /// Weaviate object IDs are UUIDs, a v1 caller responsibility, not
    /// validated here).
    pub primary_key: String,
    pub embedding_column: String,
    pub dimension: usize,
    /// Timeout in seconds for each HTTP call.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient HTTP failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
    /// Maximum number of IDs to send in a single batch-delete `where`
    /// filter. Weaviate caps the number of objects a single batch-delete
    /// can touch (default 10 000 in recent versions); this setting keeps
    /// the connector on the safe side of that limit.
    #[serde(default = "default_batch_delete_size")]
    pub batch_delete_size: usize,
    /// Maximum number of pages to fetch in parallel when listing objects
    /// (not used by the sink, reserved for future source work).
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
}

impl WeaviateConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector("weaviate: host is required".to_string()));
        }
        if self.class_name.trim().is_empty() {
            return Err(NexusError::Connector(
                "weaviate: class_name is required".to_string(),
            ));
        }
        if self.primary_key.trim().is_empty() {
            return Err(NexusError::Connector(
                "weaviate: primary_key is required".to_string(),
            ));
        }
        if self.embedding_column.trim().is_empty() {
            return Err(NexusError::Connector(
                "weaviate: embedding_column is required".to_string(),
            ));
        }
        if self.dimension == 0 {
            return Err(NexusError::Connector(
                "weaviate: dimension must be greater than 0".to_string(),
            ));
        }
        if self.batch_delete_size == 0 {
            return Err(NexusError::Connector(
                "weaviate: batch_delete_size must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_timeout_seconds() -> u64 {
    30
}

fn default_batch_delete_size() -> usize {
    5000
}

fn default_max_concurrent_requests() -> usize {
    8
}
