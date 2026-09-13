use crate::config::PineconeConnectorConfig;
use nexus_core::{with_timeout, NexusError};
use serde_json::json;

/// Vector similarity search against a Pinecone index
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up — RAG multi-vetor) —
/// read-only, ad-hoc query path used by `nexus-server`'s `POST /rag/query`,
/// separate from `PineconeSink` (write-only, pipeline sink), same split
/// `LanceDbSearchClient` established for LanceDB (Marco L5). Talks to the
/// real managed service's data-plane `/query` endpoint — same reasoning
/// `PineconeSink` already gives for why there's no self-hosted option to
/// test against (`sink.rs`'s own doc comment); this connector's own tests
/// use a mocked HTTP server instead of a real Pinecone index.
pub struct PineconeSearchClient {
    client: reqwest::Client,
    host: String,
    api_key: String,
    namespace: Option<String>,
    timeout_seconds: u64,
}

impl PineconeSearchClient {
    pub fn connect(cfg: &PineconeConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_seconds))
            .build()
            .map_err(|e| NexusError::Connector(format!("pinecone client build failed: {e}")))?;
        Ok(Self {
            client,
            host: cfg.host(),
            api_key: cfg.api_key.clone(),
            namespace: cfg.namespace.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    /// Returns the `limit` nearest vectors to `query_vector`, each as
    /// `(id, text from `source_column`'s metadata field)`. A match missing
    /// the text field (shouldn't happen for anything `PineconeSink` wrote,
    /// which stores every non-embedding, non-opcode column as metadata) is
    /// skipped rather than erroring the whole search.
    pub async fn search(
        &self,
        query_vector: Vec<f32>,
        source_column: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let mut body = json!({
            "vector": query_vector,
            "topK": limit,
            "includeMetadata": true,
        });
        if let Some(namespace) = &self.namespace {
            body["namespace"] = json!(namespace);
        }

        let response = with_timeout(self.timeout_seconds, "pinecone query", async {
            self.client
                .post(format!("{}/query", self.host))
                .header("Api-Key", &self.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| NexusError::Connector(format!("pinecone query request failed: {e}")))
        })
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "pinecone query failed ({status}): {text}"
            )));
        }

        let payload: serde_json::Value = response
            .json()
            .await
            .map_err(|e| NexusError::Connector(format!("pinecone query response invalid: {e}")))?;

        let matches = payload["matches"].as_array().cloned().unwrap_or_default();
        let mut hits = Vec::with_capacity(matches.len());
        for m in matches {
            let Some(id) = m["id"].as_str() else { continue };
            let Some(text) = m["metadata"][source_column].as_str() else {
                continue;
            };
            hits.push((id.to_string(), text.to_string()));
        }
        Ok(hits)
    }
}
