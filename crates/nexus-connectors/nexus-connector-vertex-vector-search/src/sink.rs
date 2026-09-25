use crate::auth::authenticate;
use crate::config::VertexVectorSearchConnectorConfig;
use crate::rows::{extract_embeddings, extract_ids};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{project_column, split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};

/// Writes via `upsertDatapoints` — confirmed real endpoint (`POST
/// .../indexes/{index}:upsertDatapoints`, body `{"datapoints":
/// [{"datapoint_id", "feature_vector"}]}`). Index must already be
/// created in `STREAM_UPDATE` mode and deployed to an `IndexEndpoint`
/// — real operator-side prerequisite, not configured by this
/// connector (see `config.rs`'s doc comment).
///
/// Delete uses `removeDatapoints` (`POST .../indexes/{index}:
/// removeDatapoints`, body `{"datapointIds": [...]}`) — **not
/// independently confirmed against a real call this session**,
/// inferred from the API's own naming convention (the natural pair of
/// `upsertDatapoints`). Flagged as a verification item, same honesty
/// standard every connector in this repo uses for unconfirmed
/// specifics.
pub struct VertexVectorSearchSink {
    client: reqwest::Client,
    cfg: VertexVectorSearchConnectorConfig,
}

impl VertexVectorSearchSink {
    pub async fn connect(cfg: &VertexVectorSearchConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
        })
    }

    async fn post(&self, suffix: &str, body: serde_json::Value) -> Result<(), NexusError> {
        let access_token = with_timeout(self.cfg.timeout_seconds, "vertex-vector-search authenticate", async {
            authenticate(&self.client, &self.cfg).await
        })
        .await?;

        let url = format!("{}:{suffix}", self.cfg.index_url());
        let response = with_timeout(self.cfg.timeout_seconds, "vertex-vector-search request", async {
            self.client
                .post(&url)
                .bearer_auth(&access_token)
                .json(&body)
                .send()
                .await
                .map_err(|e| NexusError::Connector(format!("vertex-vector-search {suffix} request failed: {e}")))
        })
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "vertex-vector-search {suffix} failed ({status}): {text}"
            )));
        }
        Ok(())
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ids = extract_ids(batch, &self.cfg.primary_key)?;
        let embeddings = extract_embeddings(batch, &self.cfg.embedding_column)?;

        let datapoints: Vec<serde_json::Value> = ids
            .iter()
            .zip(embeddings.iter())
            .map(|(id, embedding)| {
                serde_json::json!({ "datapoint_id": id, "feature_vector": embedding })
            })
            .collect();

        self.post("upsertDatapoints", serde_json::json!({ "datapoints": datapoints }))
            .await
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.cfg.primary_key)?;
        let ids = extract_ids(&keys, &self.cfg.primary_key)?;

        self.post("removeDatapoints", serde_json::json!({ "datapointIds": ids }))
            .await
    }
}

#[async_trait]
impl Sink for VertexVectorSearchSink {
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
