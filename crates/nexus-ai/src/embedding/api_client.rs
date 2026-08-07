use crate::embedding::common::EmbeddingError;
use serde::{Deserialize, Serialize};

/// Talks to an OpenAI-compatible `POST {base_url}/embeddings` endpoint
/// instead of loading a local ONNX model — feature `api` (CLAUDE.md §4.3),
/// alternative backend to `EmbeddingModel` (feature `cpu`/`cuda`/`metal`).
/// `base_url` has no default: this must work against any OpenAI-shaped
/// endpoint (OpenAI itself, Azure OpenAI, a self-hosted vLLM/TEI server),
/// not just one vendor.
#[derive(Debug, Clone)]
pub struct ApiEmbeddingConfig {
    pub base_url: String,
    pub model: String,
    /// Name of the environment variable holding the API key — never the key
    /// itself, per CLAUDE.md §5 (no secret ever lives in the DAG JSON spec
    /// that gets persisted/round-tripped through the UI). `None` means the
    /// endpoint needs no auth (e.g. a local vLLM server with no key check).
    pub api_key_env: Option<String>,
}

pub struct ApiEmbeddingModel {
    client: reqwest::Client,
    cfg: ApiEmbeddingConfig,
}

impl ApiEmbeddingModel {
    pub fn new(cfg: ApiEmbeddingConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            cfg,
        }
    }

    /// One HTTP round trip per call — unlike the local ONNX path, there is
    /// no persistent session to hold across calls, so `&self` (not `&mut
    /// self`) is enough.
    pub async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut request = self
            .client
            .post(format!(
                "{}/embeddings",
                self.cfg.base_url.trim_end_matches('/')
            ))
            .json(&EmbeddingRequest {
                model: &self.cfg.model,
                input: texts,
            });
        if let Some(env_var) = &self.cfg.api_key_env {
            let key = std::env::var(env_var).map_err(|_| {
                EmbeddingError::Api(format!(
                    "environment variable '{env_var}' not set for embedding API key"
                ))
            })?;
            request = request.bearer_auth(key);
        }

        let response = request
            .send()
            .await
            .map_err(|e| EmbeddingError::Api(format!("request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(EmbeddingError::Api(format!(
                "embedding API returned {status}: {body}"
            )));
        }

        let parsed: EmbeddingResponse = response
            .json()
            .await
            .map_err(|e| EmbeddingError::Api(format!("invalid response body: {e}")))?;

        if parsed.data.len() != texts.len() {
            return Err(EmbeddingError::UnexpectedOutputShape(format!(
                "API returned {} embeddings for {} inputs",
                parsed.data.len(),
                texts.len()
            )));
        }

        // The API is not required to return `data` sorted by input order —
        // OpenAI's spec documents an `index` field precisely so callers can
        // restore it.
        let mut ordered: Vec<Option<Vec<f32>>> = vec![None; texts.len()];
        for datum in parsed.data {
            if datum.index >= ordered.len() {
                return Err(EmbeddingError::UnexpectedOutputShape(format!(
                    "API returned out-of-range index {}",
                    datum.index
                )));
            }
            ordered[datum.index] = Some(datum.embedding);
        }
        ordered
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                v.ok_or_else(|| {
                    EmbeddingError::UnexpectedOutputShape(format!(
                        "missing embedding for index {i}"
                    ))
                })
            })
            .collect()
    }
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingDatum>,
}

#[derive(Deserialize)]
struct EmbeddingDatum {
    embedding: Vec<f32>,
    #[serde(default)]
    index: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn embeds_texts_via_configured_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"embedding": [0.1, 0.2], "index": 1},
                    {"embedding": [0.3, 0.4], "index": 0}
                ]
            })))
            .mount(&server)
            .await;

        let model = ApiEmbeddingModel::new(ApiEmbeddingConfig {
            base_url: server.uri(),
            model: "text-embedding-3-small".to_string(),
            api_key_env: None,
        });
        let out = model
            .embed_batch(&["first".to_string(), "second".to_string()])
            .await
            .unwrap();

        // Re-ordered by `index` back to input order despite the API
        // returning them swapped.
        assert_eq!(out, vec![vec![0.3, 0.4], vec![0.1, 0.2]]);
    }

    #[tokio::test]
    async fn empty_input_short_circuits_without_a_request() {
        let server = MockServer::start().await;
        // No mock registered — a request would fail the test.
        let model = ApiEmbeddingModel::new(ApiEmbeddingConfig {
            base_url: server.uri(),
            model: "text-embedding-3-small".to_string(),
            api_key_env: None,
        });
        let out = model.embed_batch(&[]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn missing_api_key_env_var_is_a_clear_error() {
        let server = MockServer::start().await;
        let model = ApiEmbeddingModel::new(ApiEmbeddingConfig {
            base_url: server.uri(),
            model: "text-embedding-3-small".to_string(),
            api_key_env: Some("NEXUS_TEST_EMBEDDING_API_KEY_DOES_NOT_EXIST".to_string()),
        });
        let err = model.embed_batch(&["x".to_string()]).await.unwrap_err();
        assert!(matches!(err, EmbeddingError::Api(_)));
    }

    #[tokio::test]
    async fn non_success_status_is_surfaced_as_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;

        let model = ApiEmbeddingModel::new(ApiEmbeddingConfig {
            base_url: server.uri(),
            model: "text-embedding-3-small".to_string(),
            api_key_env: None,
        });
        let err = model.embed_batch(&["x".to_string()]).await.unwrap_err();
        assert!(matches!(err, EmbeddingError::Api(_)));
    }
}
