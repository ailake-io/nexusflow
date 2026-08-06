use crate::config::QdrantConnectorConfig;
use crate::rows::{batch_to_qdrant_payloads, extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, CheckpointCursor, NexusError, Sink, OPCODE_COLUMN};
use qdrant_client::qdrant::{DeletePointsBuilder, PointStruct, PointsIdsList, UpsertPointsBuilder};
use qdrant_client::Qdrant;

/// AI Lakehouse sink #2. Every non-embedding, non-opcode column becomes the
/// point's JSON payload; `embedding_column` becomes the point vector. See
/// ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md Marco 5.
pub struct QdrantSink {
    client: Qdrant,
    collection: String,
    primary_key: String,
    embedding_column: String,
}

impl QdrantSink {
    pub fn connect(cfg: &QdrantConnectorConfig) -> Result<Self, NexusError> {
        let client = Qdrant::from_url(&cfg.url)
            .build()
            .map_err(|e| NexusError::Connector(format!("qdrant connect failed: {e}")))?;
        Ok(Self {
            client,
            collection: cfg.collection.clone(),
            primary_key: cfg.primary_key.clone(),
            embedding_column: cfg.embedding_column.clone(),
        })
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let embeddings = extract_embeddings(batch, &self.embedding_column)?;
        let ids = extract_ids(batch, &self.primary_key)?;
        let payloads =
            batch_to_qdrant_payloads(batch, &[self.embedding_column.as_str(), OPCODE_COLUMN])?;

        let points: Vec<PointStruct> = ids
            .into_iter()
            .zip(embeddings)
            .zip(payloads)
            .map(|((id, vector), payload)| PointStruct::new(id, vector, payload))
            .collect();

        self.client
            .upsert_points(UpsertPointsBuilder::new(&self.collection, points))
            .await
            .map_err(|e| NexusError::Connector(format!("qdrant upsert failed: {e}")))?;
        Ok(())
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = extract_ids(batch, &self.primary_key)?;
        self.client
            .delete_points(
                DeletePointsBuilder::new(&self.collection).points(PointsIdsList {
                    ids: ids.into_iter().map(Into::into).collect(),
                }),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("qdrant delete failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Sink for QdrantSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real point deletes instead of being
        // silently upserted. Plain (non-CDC) batches take the unchanged
        // single upsert path.
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
