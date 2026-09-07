use crate::config::ChromaConnectorConfig;
use nexus_core::{with_timeout, NexusError};
use serde_json::{json, Value};

/// Vector similarity search against a ChromaDB collection
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up — RAG multi-vetor) —
/// read-only, ad-hoc query path used by `nexus-server`'s `POST /rag/query`,
/// separate from `ChromaSink` (write-only, pipeline sink), same split
/// `LanceDbSearchClient` established for LanceDB (Marco L5).
///
/// Reads the source text from the `metadatas` response field, not Chroma's
/// native `documents` field — `ChromaSink` never populates `documents`,
/// every non-embedding column (including the source text) is written as
/// `metadatas` instead (confirmed by reading `sink.rs`'s own upsert body
/// before writing this), so `documents` would come back empty for
/// anything this connector wrote.
pub struct ChromaSearchClient {
    client: reqwest::Client,
    collection_url: String,
    authorization_header: Option<String>,
    timeout_seconds: u64,
}

impl ChromaSearchClient {
    pub async fn connect(cfg: &ChromaConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_seconds))
            .build()
            .map_err(|e| NexusError::Connector(format!("chroma client build failed: {e}")))?;
        let base_url = cfg.base_url();
        let get_url = format!(
            "{base_url}/api/v2/tenants/{}/databases/{}/collections/{}",
            cfg.tenant(),
            cfg.database(),
            cfg.collection_name()
        );
        let mut request = client.get(&get_url);
        if let Some(auth) = cfg.authorization_header() {
            request = request.header("Authorization", auth);
        }
        let response = with_timeout(cfg.timeout_seconds, "chroma get_collection", async {
            request
                .send()
                .await
                .map_err(|e| NexusError::Connector(format!("chroma get_collection request failed: {e}")))
        })
        .await?;
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
        let collection_id = body["id"].as_str().ok_or_else(|| {
            NexusError::Connector("chroma collection response missing 'id'".to_string())
        })?;

        Ok(Self {
            client,
            collection_url: format!(
                "{base_url}/api/v2/tenants/{}/databases/{}/collections/{collection_id}",
                cfg.tenant(),
                cfg.database()
            ),
            authorization_header: cfg.authorization_header(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    /// Returns the `limit` nearest rows to `query_vector`, each as
    /// `(id, text from `source_column`'s metadata field)`. A hit missing
    /// the text field is skipped rather than erroring the whole search.
    pub async fn search(
        &self,
        query_vector: Vec<f32>,
        source_column: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let body = json!({
            "query_embeddings": [query_vector],
            "n_results": limit,
        });
        let mut request = self
            .client
            .post(format!("{}/query", self.collection_url))
            .json(&body);
        if let Some(auth) = &self.authorization_header {
            request = request.header("Authorization", auth);
        }

        let response = with_timeout(self.timeout_seconds, "chroma query", async {
            request
                .send()
                .await
                .map_err(|e| NexusError::Connector(format!("chroma query request failed: {e}")))
        })
        .await?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "chroma query failed ({status}): {text}"
            )));
        }

        let payload: Value = response
            .json()
            .await
            .map_err(|e| NexusError::Connector(format!("chroma query response invalid: {e}")))?;

        // Every field is nested one level (one array per query embedding —
        // there's exactly one here) — `[0]` unwraps that outer layer.
        let ids = payload["ids"][0].as_array().cloned().unwrap_or_default();
        let metadatas = payload["metadatas"][0].as_array().cloned().unwrap_or_default();

        let mut hits = Vec::with_capacity(ids.len());
        for (id, metadata) in ids.iter().zip(metadatas.iter()) {
            let Some(id) = id.as_str() else { continue };
            let Some(text) = metadata[source_column].as_str() else {
                continue;
            };
            hits.push((id.to_string(), text.to_string()));
        }
        Ok(hits)
    }
}
