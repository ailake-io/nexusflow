use std::future::Future;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum NexusError {
    #[error("connector error: {0}")]
    Connector(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("checkpoint error: {0}")]
    Checkpoint(String),
}

/// Runs `fut` with a hard deadline, turning a stalled connector call (dead
/// TCP connection, unreachable host, wedged server) into a `NexusError`
/// instead of blocking the pipeline forever (C15,
/// docs/PROJECT_REVIEW.md). `op` names the call in the error message
/// (e.g. `"mongodb connect"`) — connectors otherwise share the same
/// `NexusError::Connector` variant for every failure mode.
pub async fn with_timeout<T>(
    seconds: u64,
    op: &str,
    fut: impl Future<Output = Result<T, NexusError>>,
) -> Result<T, NexusError> {
    tokio::time::timeout(Duration::from_secs(seconds), fut)
        .await
        .map_err(|_| NexusError::Connector(format!("{op} timed out after {seconds}s")))?
}
