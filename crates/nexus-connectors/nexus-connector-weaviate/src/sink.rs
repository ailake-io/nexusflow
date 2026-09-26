use crate::config::WeaviateConnectorConfig;
use crate::rows::{batch_to_properties, extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{
    project_column, retry_with_backoff, split_by_opcode, CheckpointCursor, NexusError, Sink,
    OPCODE_COLUMN,
};
use serde_json::Value;

/// Writes via the batch objects API (`POST /v1/batch/objects`) —
/// confirmed real "bring your own vectors" flow: each object carries
/// an explicit `vector`, no automatic embedding generation involved.
/// Class (collection) must already exist — this sink only writes
/// rows, same contract every vector sink in this workspace follows.
///
/// Deletes use Weaviate's batch-delete endpoint (`DELETE
/// /v1/batch/objects`) with an `Or`/`Equal` filter over `id`, rather
/// than one HTTP call per ID. The ID list is chunked to stay below
/// Weaviate's per-request object limit.
pub struct WeaviateSink {
    client: reqwest::Client,
    base_url: String,
    class_name: String,
    primary_key: String,
    embedding_column: String,
    api_key: Option<String>,
    timeout_seconds: u64,
    retry: nexus_core::RetryConfig,
    batch_delete_size: usize,
}

impl WeaviateSink {
    pub async fn connect(cfg: &WeaviateConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_seconds))
            .build()
            .map_err(|e| NexusError::Connector(format!("weaviate client build failed: {e}")))?;

        Ok(Self {
            client,
            base_url: cfg.host.clone(),
            class_name: cfg.class_name.clone(),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
            api_key: cfg.api_key.clone(),
            timeout_seconds: cfg.timeout_seconds,
            retry: cfg.retry.clone(),
            batch_delete_size: cfg.batch_delete_size,
        })
    }

    fn auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(key) => builder.bearer_auth(key),
            None => builder,
        }
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = extract_ids(batch, &self.primary_key)?;
        let embeddings = extract_embeddings(batch, &self.embedding_column)?;
        let properties = batch_to_properties(
            batch,
            &[
                self.embedding_column.as_str(),
                self.primary_key.as_str(),
                OPCODE_COLUMN,
            ],
        )?;

        let objects: Vec<Value> = ids
            .iter()
            .zip(embeddings.iter())
            .zip(properties.iter())
            .map(|((id, embedding), props)| {
                serde_json::json!({
                    "class": self.class_name,
                    "id": id,
                    "properties": props,
                    "vector": embedding,
                })
            })
            .collect();

        let base_url = self.base_url.clone();
        let class_name = self.class_name.clone();
        let request_builder = self.auth(
            self.client
                .post(format!("{}/v1/batch/objects", base_url))
                .json(&serde_json::json!({ "objects": objects })),
        );

        let timeout_seconds = self.timeout_seconds;
        retry_with_backoff(&self.retry, "weaviate batch objects", || {
            let request_builder = request_builder.try_clone();
            let class_name = class_name.clone();
            async move {
                let request = request_builder.ok_or_else(|| {
                    NexusError::Connector(
                        "weaviate: failed to clone batch objects request".to_string(),
                    )
                })?;
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    request.send(),
                )
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("weaviate batch objects request timed out: {e}"))
                })?
                .map_err(|e| {
                    NexusError::Connector(format!("weaviate batch objects request failed: {e}"))
                })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "weaviate batch objects failed for class {class_name} ({status}): {text}"
                    )));
                }

                let results: Vec<Value> = response.json().await.map_err(|e| {
                    NexusError::Connector(format!(
                        "weaviate batch objects response parse failed: {e}"
                    ))
                })?;

                let failures: Vec<&Value> = results
                    .iter()
                    .filter(|item| {
                        item.pointer("/result/errors").is_some()
                            || item.pointer("/result/status").and_then(Value::as_str)
                                == Some("FAILED")
                    })
                    .collect();
                if !failures.is_empty() {
                    return Err(NexusError::Connector(format!(
                        "weaviate batch objects had item-level failures: {failures:?}"
                    )));
                }

                Ok(())
            }
        })
        .await
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = extract_ids(&keys, &self.primary_key)?;

        // Weaviate batch-delete filters have a per-request object limit;
        // chunk the ID list to stay safely below it.
        for chunk in ids.chunks(self.batch_delete_size) {
            self.delete_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn delete_chunk(&self, ids: &[String]) -> Result<(), NexusError> {
        let operands: Vec<Value> = ids
            .iter()
            .map(|id| {
                serde_json::json!({
                    "path": ["id"],
                    "operator": "Equal",
                    "valueText": id,
                })
            })
            .collect();

        let body = serde_json::json!({
            "match": {
                "class": self.class_name,
                "where": {
                    "operator": "Or",
                    "operands": operands,
                }
            },
            "output": "minimal",
            "dryRun": false,
        });

        let base_url = self.base_url.clone();
        let class_name = self.class_name.clone();
        let request_builder = self.auth(
            self.client
                .delete(format!("{}/v1/batch/objects", base_url))
                .json(&body),
        );

        let timeout_seconds = self.timeout_seconds;
        retry_with_backoff(&self.retry, "weaviate batch delete", || {
            let request_builder = request_builder.try_clone();
            let class_name = class_name.clone();
            async move {
                let request = request_builder.ok_or_else(|| {
                    NexusError::Connector(
                        "weaviate: failed to clone batch delete request".to_string(),
                    )
                })?;
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    request.send(),
                )
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("weaviate batch delete request timed out: {e}"))
                })?
                .map_err(|e| {
                    NexusError::Connector(format!("weaviate batch delete request failed: {e}"))
                })?;

                // 404 means the objects are already gone, which is fine for a delete.
                if !response.status().is_success() && response.status().as_u16() != 404 {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "weaviate batch delete failed for class {class_name} ({status}): {text}"
                    )));
                }

                Ok(())
            }
        })
        .await
    }
}

#[async_trait]
impl Sink for WeaviateSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        match split_by_opcode(&batch)? {
            None => self.upsert(&batch).await,
            Some(split) => {
                self.upsert(&split.upserts).await?;
                self.delete(&split.deletes).await?;
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
