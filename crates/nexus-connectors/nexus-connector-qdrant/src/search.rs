use crate::config::QdrantConnectorConfig;
use nexus_core::{with_timeout, NexusError};
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::SearchPointsBuilder;
use qdrant_client::Qdrant;

/// Vector similarity search against a Qdrant collection
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up — RAG multi-vetor) —
/// read-only, ad-hoc query path used by `nexus-server`'s `POST /rag/query`,
/// separate from `QdrantSink` (write-only, pipeline sink) same split
/// `LanceDbSearchClient` already established for LanceDB (Marco L5).
///
/// Unlike LanceDB, Qdrant isn't Arrow-native — a hit is a scored point with
/// a JSON-ish payload, not a `RecordBatch`. `search` returns `(key, text)`
/// pairs directly instead of forcing every non-Arrow vector store into a
/// `RecordBatch` shape that doesn't fit it.
pub struct QdrantSearchClient {
    client: Qdrant,
    collection: String,
    timeout_seconds: u64,
}

impl QdrantSearchClient {
    pub fn connect(cfg: &QdrantConnectorConfig) -> Result<Self, NexusError> {
        let client = Qdrant::from_url(&cfg.url())
            .build()
            .map_err(|e| NexusError::Connector(format!("qdrant connect failed: {e}")))?;
        Ok(Self {
            client,
            collection: cfg.collection_name(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    /// Returns the `limit` nearest points to `query_vector`, each as
    /// `(primary key, text from `source_column`'s payload field)` — the
    /// key comes from the point's own `id` (always present, authoritative),
    /// not re-read from the payload even though `QdrantSink` also stores
    /// the primary key column there. A point missing the text field
    /// (shouldn't happen for anything `QdrantSink` wrote) is skipped rather
    /// than erroring the whole search.
    pub async fn search(
        &self,
        query_vector: Vec<f32>,
        source_column: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let response = with_timeout(self.timeout_seconds, "qdrant search", async {
            self.client
                .search_points(
                    SearchPointsBuilder::new(&self.collection, query_vector, limit as u64)
                        .with_payload(true),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("qdrant search failed: {e}")))
        })
        .await?;

        let mut hits = Vec::with_capacity(response.result.len());
        for point in response.result {
            let Some(key) = point.id.as_ref().and_then(point_id_to_string) else {
                continue;
            };
            let Some(text) = point
                .payload
                .get(source_column)
                .map(|v| serde_json::Value::from(v.clone()))
                .and_then(|v| v.as_str().map(str::to_string))
            else {
                continue;
            };
            hits.push((key, text));
        }
        Ok(hits)
    }
}

fn point_id_to_string(id: &qdrant_client::qdrant::PointId) -> Option<String> {
    match id.point_id_options.as_ref()? {
        PointIdOptions::Num(n) => Some(n.to_string()),
        PointIdOptions::Uuid(s) => Some(s.clone()),
    }
}
