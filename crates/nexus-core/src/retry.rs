use std::future::Future;
use std::time::Duration;

use crate::NexusError;

/// Shared retry/backoff configuration for connectors.
///
/// Add this to a connector config with `#[serde(flatten)]` or as explicit
/// fields so every network-facing connector gets the same knobs.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct RetryConfig {
    /// Number of retries on transient failures (5xx, timeouts, connect errors).
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Base delay between retries in seconds (exponential backoff).
    #[serde(default = "default_retry_backoff_seconds")]
    pub retry_backoff_seconds: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retries: default_retries(),
            retry_backoff_seconds: default_retry_backoff_seconds(),
        }
    }
}

fn default_retries() -> u32 {
    3
}

fn default_retry_backoff_seconds() -> u64 {
    1
}

/// Returns true when an error looks retryable (network blip or server-side
/// transient failure). This is intentionally conservative: only errors whose
/// message contains well-known transient indicators are retried.
pub fn is_transient_error(err: &NexusError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("timeout")
        || msg.contains("timed out")
        || msg.contains("connect")
        || msg.contains("connection")
        || msg.contains("500")
        || msg.contains("502")
        || msg.contains("503")
        || msg.contains("504")
        || msg.contains("reset")
        || msg.contains("broken pipe")
}

/// Runs `operation` up to `retries + 1` times, sleeping exponentially between
/// attempts when the error is transient.
///
/// The first attempt is immediate. If `retries` is 0 the operation is tried
/// exactly once.
pub async fn retry_with_backoff<T, F, Fut>(
    config: &RetryConfig,
    op_name: &str,
    mut operation: F,
) -> Result<T, NexusError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, NexusError>>,
{
    let mut last_err = None;
    for attempt in 0..=config.retries {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt == config.retries || !is_transient_error(&err) {
                    return Err(err);
                }
                let delay = Duration::from_secs(config.retry_backoff_seconds) * 2u32.pow(attempt);
                tracing::warn!(
                    "{op_name} failed (attempt {}/{}): {err}, retrying in {:?}",
                    attempt + 1,
                    config.retries + 1,
                    delay
                );
                tokio::time::sleep(delay).await;
                last_err = Some(err);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| NexusError::Connector(format!("{op_name} retry exhausted"))))
}
