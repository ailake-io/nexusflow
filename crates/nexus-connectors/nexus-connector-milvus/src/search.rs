use crate::config::MilvusConnectorConfig;
use milvus::client::Client;
use milvus::collection::SearchOption;
use milvus::index::MetricType;
use milvus::value::{Value, ValueVec};
use nexus_core::{with_timeout, NexusError};

/// Vector similarity search against a Milvus collection
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up — RAG multi-vetor) —
/// read-only, ad-hoc query path used by `nexus-server`'s `POST /rag/query`,
/// separate from `MilvusSink` (write-only, pipeline sink), same split
/// `LanceDbSearchClient` established for LanceDB (Marco L5).
///
/// `MetricType::L2` is hardcoded — `MilvusSink` never creates the
/// collection/index itself (schema and index come from Milvus, see
/// `sink.rs`'s own doc comment), so this search has no way to discover
/// which metric a given collection's index actually uses. If a collection
/// was indexed with a different metric (e.g. cosine/IP), Milvus rejects
/// the search with a clear server-side error rather than returning wrong
/// results silently — adjust this constant (or thread it through
/// `MilvusConnectorConfig` as a new field) if that happens in practice.
const SEARCH_METRIC: MetricType = MetricType::L2;

pub struct MilvusSearchClient {
    client: Client,
    collection: String,
    primary_key: String,
    timeout_seconds: u64,
}

impl MilvusSearchClient {
    pub async fn connect(cfg: &MilvusConnectorConfig) -> Result<Self, NexusError> {
        let client = with_timeout(cfg.timeout_seconds, "milvus connect", async {
            Client::new(cfg.url())
                .await
                .map_err(|e| NexusError::Connector(format!("milvus connect failed: {e}")))
        })
        .await?;
        Ok(Self {
            client,
            collection: cfg.collection_name(),
            primary_key: cfg.primary_key.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    /// Returns the `limit` nearest rows to `query_vector`, each as
    /// `(primary key, text from `source_column`)`. Requires `source_column`
    /// to be a Milvus `String` field — anything else (or a row where either
    /// output field is missing) is skipped rather than erroring the whole
    /// search.
    pub async fn search(
        &self,
        query_vector: Vec<f32>,
        embedding_column: &str,
        source_column: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let collection = with_timeout(self.timeout_seconds, "milvus get_collection", async {
            self.client
                .get_collection(&self.collection)
                .await
                .map_err(|e| NexusError::Connector(format!("milvus get_collection failed: {e}")))
        })
        .await?;

        let results = with_timeout(self.timeout_seconds, "milvus search", async {
            collection
                .search(
                    vec![Value::from(query_vector)],
                    embedding_column,
                    limit as i32,
                    SEARCH_METRIC,
                    vec![self.primary_key.as_str(), source_column],
                    &SearchOption::new(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("milvus search failed: {e}")))
        })
        .await?;

        let mut hits = Vec::new();
        for result in results {
            let keys = extract_strings(&result.id);
            let Some(text_column) = result.field.iter().find(|f| f.name == source_column) else {
                continue;
            };
            let ValueVec::String(texts) = &text_column.value else {
                continue;
            };
            for (key, text) in keys.into_iter().zip(texts.iter()) {
                hits.push((key, text.clone()));
            }
        }
        Ok(hits)
    }
}

/// Milvus primary keys are either `Int64` or `VarChar` — both are valid
/// primary key types, so both are handled here rather than assuming one.
fn extract_strings(values: &[Value<'_>]) -> Vec<String> {
    values
        .iter()
        .filter_map(|v| match v {
            Value::Long(n) => Some(n.to_string()),
            Value::String(s) => Some(s.to_string()),
            _ => None,
        })
        .collect()
}
