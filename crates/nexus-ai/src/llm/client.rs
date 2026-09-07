use crate::llm::common::LlmError;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Talks to an OpenAI-compatible `POST {base_url}/chat/completions` endpoint
/// — same shape as `embedding::ApiEmbeddingConfig`, just a different route
/// and response body. `base_url` has no default: this must work against any
/// OpenAI-shaped endpoint (OpenAI itself, Azure OpenAI, a self-hosted
/// vLLM/TGI server), not just one vendor.
#[derive(Debug, Clone)]
pub struct LlmClientConfig {
    pub base_url: String,
    pub model: String,
    /// Name of the environment variable holding the API key — never the key
    /// itself, per CLAUDE.md §5 (no secret ever lives in the DAG JSON spec
    /// that gets persisted/round-tripped through the UI). `None` means the
    /// endpoint needs no auth (e.g. a local vLLM server with no key check).
    pub api_key_env: Option<String>,
}

/// One LLM call's result plus the metadata the caller needs for tracing
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L1) — nexus-ai never logs anything
/// itself (layering: `RunLogger` lives in nexus-server), it just reports
/// these numbers back up.
#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub text: String,
    pub tokens_prompt: u32,
    pub tokens_completion: u32,
    pub latency_ms: u64,
}

pub struct LlmClient {
    client: reqwest::Client,
    cfg: LlmClientConfig,
}

impl LlmClient {
    pub fn new(cfg: LlmClientConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            cfg,
        }
    }

    pub async fn call(
        &self,
        prompt: &str,
        max_tokens: Option<u32>,
        temperature: Option<f32>,
    ) -> Result<LlmResponse, LlmError> {
        let started = Instant::now();
        let mut request = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.cfg.base_url.trim_end_matches('/')
            ))
            .json(&ChatCompletionRequest {
                model: &self.cfg.model,
                messages: &[ChatMessage {
                    role: "user",
                    content: prompt,
                }],
                max_tokens,
                temperature,
            });
        if let Some(env_var) = &self.cfg.api_key_env {
            let key = std::env::var(env_var).map_err(|_| {
                LlmError::Api(format!(
                    "environment variable '{env_var}' not set for llm API key"
                ))
            })?;
            request = request.bearer_auth(key);
        }

        let response = request
            .send()
            .await
            .map_err(|e| LlmError::Api(format!("request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LlmError::Api(format!(
                "llm API returned {status}: {}",
                truncate(&body, 512)
            )));
        }

        let latency_ms = started.elapsed().as_millis() as u64;
        let parsed: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|e| LlmError::Api(format!("invalid response body: {e}")))?;

        let text = parsed
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| LlmError::Api("llm API returned no choices".to_string()))?;

        Ok(LlmResponse {
            text,
            tokens_prompt: parsed.usage.as_ref().map(|u| u.prompt_tokens).unwrap_or(0),
            tokens_completion: parsed
                .usage
                .as_ref()
                .map(|u| u.completion_tokens)
                .unwrap_or(0),
            latency_ms,
        })
    }
}

/// Caps how much of an upstream error body ends up in `LlmError`, which
/// propagates into pipeline run logs a lower-privilege ("Execute" role)
/// caller can read — without this, an arbitrarily large or sensitive
/// response body would be reflected back in full. Same reasoning and
/// exact copy as `embedding::api_client::truncate`.
fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push_str("…[truncated]");
    out
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: &'a [ChatMessage<'a>],
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
}

#[derive(Deserialize)]
struct ChatResponseMessage {
    content: String,
}

#[derive(Deserialize)]
struct ChatUsage {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn calls_configured_endpoint_and_parses_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "hello there"}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 4}
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(LlmClientConfig {
            base_url: server.uri(),
            model: "gpt-test".to_string(),
            api_key_env: None,
        });
        let resp = client.call("hi", Some(64), Some(0.7)).await.unwrap();

        assert_eq!(resp.text, "hello there");
        assert_eq!(resp.tokens_prompt, 12);
        assert_eq!(resp.tokens_completion, 4);
    }

    #[tokio::test]
    async fn missing_api_key_env_var_is_a_clear_error() {
        let server = MockServer::start().await;
        let client = LlmClient::new(LlmClientConfig {
            base_url: server.uri(),
            model: "gpt-test".to_string(),
            api_key_env: Some("NEXUS_TEST_LLM_API_KEY_DOES_NOT_EXIST".to_string()),
        });
        let err = client.call("hi", None, None).await.unwrap_err();
        assert!(matches!(err, LlmError::Api(_)));
    }

    #[tokio::test]
    async fn non_success_status_is_surfaced_as_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;

        let client = LlmClient::new(LlmClientConfig {
            base_url: server.uri(),
            model: "gpt-test".to_string(),
            api_key_env: None,
        });
        let err = client.call("hi", None, None).await.unwrap_err();
        assert!(matches!(err, LlmError::Api(_)));
    }

    #[tokio::test]
    async fn no_choices_is_a_clear_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"choices": []})),
            )
            .mount(&server)
            .await;

        let client = LlmClient::new(LlmClientConfig {
            base_url: server.uri(),
            model: "gpt-test".to_string(),
            api_key_env: None,
        });
        let err = client.call("hi", None, None).await.unwrap_err();
        assert!(matches!(err, LlmError::Api(_)));
    }

    #[test]
    fn truncate_is_a_no_op_under_the_limit() {
        assert_eq!(truncate("short", 512), "short");
    }
}
