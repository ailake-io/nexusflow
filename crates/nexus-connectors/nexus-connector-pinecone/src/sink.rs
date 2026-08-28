use crate::config::PineconeConnectorConfig;
use crate::rows::{batch_to_metadata, extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{
    project_column, split_by_opcode, CheckpointCursor, NexusError, Sink, OPCODE_COLUMN,
};
use serde_json::json;

/// Maximum vectors per Pinecone upsert/delete request. Pinecone recommends
/// staying under ~1000 vectors per call; larger batches are split.
const PINECONE_CHUNK_SIZE: usize = 1000;

/// AI Lakehouse sink #5. Pinecone has no self-hosted/Docker option — this
/// talks to the real managed service's data-plane REST API
/// (`/vectors/upsert`, `/vectors/delete`). See ARCHITECTURE.md §4.3,
/// IMPLEMENTATION_PLAN.md Marco 5.
pub struct PineconeSink {
    client: reqwest::Client,
    host: String,
    api_key: String,
    primary_key: String,
    embedding_column: String,
    namespace: Option<String>,
}

impl PineconeSink {
    pub fn connect(cfg: &PineconeConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_seconds))
            .build()
            .map_err(|e| NexusError::Connector(format!("pinecone client build failed: {e}")))?;
        Ok(Self {
            client,
            host: cfg.host(),
            api_key: cfg.api_key.clone(),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
            namespace: cfg.namespace.clone(),
        })
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = extract_ids(batch, &self.primary_key)?;
        let embeddings = extract_embeddings(batch, &self.embedding_column)?;
        let metadata = batch_to_metadata(batch, &[self.embedding_column.as_str(), OPCODE_COLUMN])?;

        let vectors: Vec<_> = ids
            .into_iter()
            .zip(embeddings)
            .zip(metadata)
            .map(|((id, values), metadata)| {
                json!({ "id": id, "values": values, "metadata": metadata })
            })
            .collect();

        for chunk in vectors.chunks(PINECONE_CHUNK_SIZE) {
            let mut body = json!({ "vectors": chunk });
            if let Some(namespace) = &self.namespace {
                body["namespace"] = json!(namespace);
            }

            let response = self
                .client
                .post(format!("{}/vectors/upsert", self.host))
                .header("Api-Key", &self.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("pinecone upsert request failed: {e}"))
                })?;
            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "pinecone upsert failed ({status}): {text}"
                )));
            }
        }
        Ok(())
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = extract_ids(&keys, &self.primary_key)?;

        for chunk in ids.chunks(PINECONE_CHUNK_SIZE) {
            let mut body = json!({ "ids": chunk });
            if let Some(namespace) = &self.namespace {
                body["namespace"] = json!(namespace);
            }

            let response = self
                .client
                .post(format!("{}/vectors/delete", self.host))
                .header("Api-Key", &self.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("pinecone delete request failed: {e}"))
                })?;
            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "pinecone delete failed ({status}): {text}"
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for PineconeSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes hit `/vectors/delete` instead of being silently
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
