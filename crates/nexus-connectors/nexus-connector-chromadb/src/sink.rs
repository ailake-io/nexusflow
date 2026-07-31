use crate::config::ChromaConnectorConfig;
use crate::rows::{batch_to_metadata, extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{project_column, split_by_opcode, CheckpointCursor, NexusError, Sink, OPCODE_COLUMN};
use serde_json::{json, Value};

/// AI Lakehouse sink #6 (last, most complex to operate — ROADMAP.md Fase 5).
/// Talks to ChromaDB's v2 REST API directly (no official/stable Rust client
/// crate to depend on, same call as `nexus-connector-pinecone`). See
/// ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md Marco 5.
pub struct ChromaSink {
    client: reqwest::Client,
    collection_url: String,
    primary_key: String,
    embedding_column: String,
}

impl ChromaSink {
    pub async fn connect(cfg: &ChromaConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::new();
        let host = cfg.host.trim_end_matches('/');
        let get_url = format!(
            "{host}/api/v2/tenants/{}/databases/{}/collections/{}",
            cfg.tenant, cfg.database, cfg.collection
        );
        let response = client
            .get(&get_url)
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("chroma get_collection request failed: {e}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "chroma get_collection failed ({status}): {text}"
            )));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| NexusError::Connector(format!("chroma get_collection response: {e}")))?;
        let collection_id = body["id"]
            .as_str()
            .ok_or_else(|| NexusError::Connector("chroma collection response missing 'id'".to_string()))?;

        Ok(Self {
            client,
            collection_url: format!(
                "{host}/api/v2/tenants/{}/databases/{}/collections/{collection_id}",
                cfg.tenant, cfg.database
            ),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
        })
    }

    async fn post(&self, endpoint: &str, body: Value) -> Result<(), NexusError> {
        let response = self
            .client
            .post(format!("{}/{endpoint}", self.collection_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("chroma {endpoint} request failed: {e}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "chroma {endpoint} failed ({status}): {text}"
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
        let metadatas = batch_to_metadata(batch, &[self.embedding_column.as_str(), OPCODE_COLUMN])?;

        self.post(
            "upsert",
            json!({ "ids": ids, "embeddings": embeddings, "metadatas": metadatas }),
        )
        .await
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = extract_ids(&keys, &self.primary_key)?;
        self.post("delete", json!({ "ids": ids })).await
    }
}

#[async_trait]
impl Sink for ChromaSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes hit the `delete` endpoint instead of being silently
        // upserted. Plain (non-CDC) batches take the unchanged single
        // upsert path.
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
