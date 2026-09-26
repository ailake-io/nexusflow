use crate::config::ElasticsearchConnectorConfig;
use crate::rows::{batch_to_source, extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{
    project_column, split_by_opcode, CheckpointCursor, NexusError, Sink, OPCODE_COLUMN,
};
use serde_json::Value;

/// Writes via the Bulk API (`POST /_bulk`, NDJSON body) — confirmed
/// real, identical wire format between Elasticsearch and OpenSearch
/// (OpenSearch forked from Elasticsearch 7.10 and kept this format),
/// which is why this one crate registers both `"elasticsearch"` and
/// `"opensearch"` (see `lib.rs`) without any flavor-specific branching
/// in this file.
///
/// Index/collection must already exist with the right vector field
/// mapping — this sink only writes rows, same contract every vector
/// sink in this workspace follows.
pub struct ElasticsearchSink {
    client: reqwest::Client,
    base_url: String,
    index: String,
    primary_key: String,
    embedding_column: String,
    api_key: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl ElasticsearchSink {
    pub async fn connect(cfg: &ElasticsearchConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_seconds))
            .build()
            .map_err(|e| {
                NexusError::Connector(format!("elasticsearch client build failed: {e}"))
            })?;

        Ok(Self {
            client,
            base_url: cfg.base_url()?.to_string(),
            index: cfg.index.clone(),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
            api_key: cfg.api_key.clone(),
            username: cfg.username.clone(),
            password: cfg.password.clone(),
        })
    }

    async fn bulk(&self, body: String) -> Result<(), NexusError> {
        let mut request = self
            .client
            .post(format!("{}/_bulk", self.base_url))
            .header("Content-Type", "application/x-ndjson")
            .body(body);

        if let Some(key) = &self.api_key {
            request = request.header("Authorization", format!("ApiKey {key}"));
        } else if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request.send().await.map_err(|e| {
            NexusError::Connector(format!("elasticsearch bulk request failed: {e}"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "elasticsearch bulk failed ({status}): {text}"
            )));
        }

        let parsed: Value = response.json().await.map_err(|e| {
            NexusError::Connector(format!("elasticsearch bulk response parse failed: {e}"))
        })?;

        if parsed
            .get("errors")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let failures: Vec<&Value> = parsed
                .get("items")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|item| {
                    item.as_object()
                        .is_some_and(|obj| obj.values().any(|v| v.get("error").is_some()))
                })
                .collect();
            return Err(NexusError::Connector(format!(
                "elasticsearch bulk had item-level failures: {failures:?}"
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
        let sources = batch_to_source(batch, &[self.embedding_column.as_str(), OPCODE_COLUMN])?;

        let mut body = String::new();
        for ((id, embedding), source) in ids.iter().zip(embeddings.iter()).zip(sources.iter()) {
            let action = serde_json::json!({ "index": { "_index": self.index, "_id": id } });
            let mut doc = source
                .as_object()
                .cloned()
                .ok_or_else(|| NexusError::Schema("elasticsearch: expected object row".into()))?;
            doc.insert(self.embedding_column.clone(), serde_json::json!(embedding));

            body.push_str(&action.to_string());
            body.push('\n');
            body.push_str(&Value::Object(doc).to_string());
            body.push('\n');
        }

        self.bulk(body).await
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = extract_ids(&keys, &self.primary_key)?;

        let mut body = String::new();
        for id in &ids {
            let action = serde_json::json!({ "delete": { "_index": self.index, "_id": id } });
            body.push_str(&action.to_string());
            body.push('\n');
        }

        self.bulk(body).await
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
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
