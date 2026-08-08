use crate::config::{WebhookBodyMode, WebhookMethod, WebhookSinkConfig};
use crate::rows::batch_to_json_rows;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{CheckpointCursor, NexusError, Sink};
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;

/// Generic outbound API sink — no CDC awareness on purpose: an arbitrary
/// external API has no agreed-upon meaning for an `__opcode` column, unlike
/// a database connector's upsert/delete SQL. Every row (including a CDC
/// batch's `__opcode` field, if present) goes out verbatim via the single
/// configured `method`; a receiving API that cares about insert/update/
/// delete semantics needs to read that field itself.
pub struct WebhookSink {
    client: reqwest::Client,
    config: WebhookSinkConfig,
}

impl WebhookSink {
    pub fn connect(config: &WebhookSinkConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()
            .map_err(|e| NexusError::Connector(format!("webhook client build failed: {e}")))?;
        Ok(Self {
            client,
            config: config.clone(),
        })
    }

    fn build_request(&self, body: &Value) -> reqwest::RequestBuilder {
        let mut request = match self.config.method {
            WebhookMethod::Post => self.client.post(&self.config.url),
            WebhookMethod::Put => self.client.put(&self.config.url),
            WebhookMethod::Patch => self.client.patch(&self.config.url),
            WebhookMethod::Delete => self.client.delete(&self.config.url),
        };
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }
        request.json(body)
    }

    async fn send_once(&self, body: &Value) -> Result<(), NexusError> {
        self.build_request(body)
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("webhook request failed: {e}")))?
            .error_for_status()
            .map_err(|e| NexusError::Connector(format!("webhook request failed: {e}")))?;
        Ok(())
    }

    async fn send_with_retry(&self, body: &Value) -> Result<(), NexusError> {
        let mut last_err = None;
        for attempt in 0..=self.config.retries {
            match self.send_once(body).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if attempt == self.config.retries || !is_transient_error(&err) {
                        return Err(err);
                    }
                    let delay =
                        Duration::from_secs(self.config.retry_backoff_seconds) * 2u32.pow(attempt);
                    tracing::warn!(
                        "webhook request failed (attempt {}/{}): {err}, retrying in {:?}",
                        attempt + 1,
                        self.config.retries + 1,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    last_err = Some(err);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| NexusError::Connector("webhook retry exhausted".into())))
    }
}

fn is_transient_error(err: &NexusError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("timeout")
        || msg.contains("connect")
        || msg.contains("500")
        || msg.contains("502")
        || msg.contains("503")
        || msg.contains("504")
}

#[async_trait]
impl Sink for WebhookSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = batch_to_json_rows(&batch)?;

        match self.config.body_mode {
            WebhookBodyMode::Array => self.send_with_retry(&Value::Array(rows)).await,
            WebhookBodyMode::PerRow => {
                let mut last_sent: Option<Instant> = None;
                for row in rows {
                    if self.config.requests_per_second > 0 {
                        let min_interval = Duration::from_secs_f64(
                            1.0 / f64::from(self.config.requests_per_second),
                        );
                        if let Some(prev) = last_sent {
                            let elapsed = prev.elapsed();
                            if elapsed < min_interval {
                                tokio::time::sleep(min_interval - elapsed).await;
                            }
                        }
                        last_sent = Some(Instant::now());
                    }
                    self.send_with_retry(&row).await?;
                }
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
