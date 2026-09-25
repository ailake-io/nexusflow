use crate::config::AzureAiSearchConnectorConfig;
use crate::rows::{batch_to_properties, extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{project_column, split_by_opcode, CheckpointCursor, NexusError, Sink, OPCODE_COLUMN};
use serde_json::Value;

/// Writes via the Index Documents API (`POST /indexes/{index}/docs/
/// index?api-version=...`) — confirmed real "bring your own vectors"
/// flow, same as every other vector sink in this workspace: each
/// document carries an explicit vector field, no automatic embedding
/// generation. Unlike Weaviate, upsert *and* delete share this same
/// endpoint — each document in the `value` array carries its own
/// `@search.action` (`mergeOrUpload` or `delete`), so there's a single
/// `post_documents` helper instead of separate upsert/delete request
/// shapes.
///
/// Index (with the vector field pre-declared as
/// `Collection(Edm.Single)`, `dimensions` + `vectorSearchProfile` set)
/// must already exist — this sink only writes documents.
pub struct AzureAiSearchSink {
    client: reqwest::Client,
    base_url: String,
    index_name: String,
    api_version: String,
    primary_key: String,
    embedding_column: String,
    api_key: String,
}

impl AzureAiSearchSink {
    pub async fn connect(cfg: &AzureAiSearchConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_seconds))
            .build()
            .map_err(|e| NexusError::Connector(format!("azure ai search client build failed: {e}")))?;

        Ok(Self {
            client,
            base_url: cfg.endpoint.clone(),
            index_name: cfg.index_name.clone(),
            api_version: cfg.api_version.clone(),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
            api_key: cfg.api_key.clone(),
        })
    }

    async fn post_documents(&self, actions: Vec<Value>) -> Result<(), NexusError> {
        if actions.is_empty() {
            return Ok(());
        }

        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/docs/index?api-version={}",
                self.base_url, self.index_name, self.api_version
            ))
            .header("api-key", &self.api_key)
            .json(&serde_json::json!({ "value": actions }))
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("azure ai search docs/index request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "azure ai search docs/index failed ({status}): {text}"
            )));
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| NexusError::Connector(format!("azure ai search docs/index response parse failed: {e}")))?;

        let results = body
            .get("value")
            .and_then(Value::as_array)
            .ok_or_else(|| NexusError::Connector("azure ai search docs/index response missing 'value'".into()))?;

        let failures: Vec<&Value> = results
            .iter()
            .filter(|item| item.get("status").and_then(Value::as_bool) == Some(false))
            .collect();
        if !failures.is_empty() {
            return Err(NexusError::Connector(format!(
                "azure ai search docs/index had item-level failures: {failures:?}"
            )));
        }

        Ok(())
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = extract_ids(batch, &self.primary_key)?;
        let embeddings = extract_embeddings(batch, &self.embedding_column)?;
        let properties = batch_to_properties(
            batch,
            &[self.embedding_column.as_str(), self.primary_key.as_str(), OPCODE_COLUMN],
        )?;

        let actions: Vec<Value> = ids
            .iter()
            .zip(embeddings.iter())
            .zip(properties.iter())
            .map(|((id, embedding), props)| {
                let mut doc = props.as_object().cloned().unwrap_or_default();
                doc.insert("@search.action".to_string(), Value::from("mergeOrUpload"));
                doc.insert(self.primary_key.clone(), Value::from(id.as_str()));
                doc.insert(self.embedding_column.clone(), Value::from(embedding.clone()));
                Value::Object(doc)
            })
            .collect();

        self.post_documents(actions).await
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = extract_ids(&keys, &self.primary_key)?;

        let actions: Vec<Value> = ids
            .iter()
            .map(|id| {
                let mut doc = serde_json::Map::new();
                doc.insert("@search.action".to_string(), Value::from("delete"));
                doc.insert(self.primary_key.clone(), Value::from(id.as_str()));
                Value::Object(doc)
            })
            .collect();

        self.post_documents(actions).await
    }
}

#[async_trait]
impl Sink for AzureAiSearchSink {
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
