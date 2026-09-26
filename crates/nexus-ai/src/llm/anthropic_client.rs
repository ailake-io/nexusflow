use crate::llm::client::LlmResponse;
use crate::llm::common::{LlmError, LlmTurn, ToolCall, ToolDef, ToolMessage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;

/// Anthropic's native Messages API (`POST {base_url}/v1/messages`) — not
/// OpenAI-shaped: `x-api-key` header instead of `Authorization: Bearer`,
/// `anthropic-version` header required, `max_tokens` required by the API
/// itself (OpenAI treats it as optional), and the response nests text
/// inside a `content` array (`[{type: "text", text: "..."}]`) instead of
/// `choices[0].message.content`.
#[derive(Debug, Clone)]
pub struct AnthropicClientConfig {
    pub base_url: String,
    pub model: String,
    /// Name of the environment variable holding the API key — required
    /// (unlike `ApiClientConfig::api_key_env`, Anthropic's API has no
    /// "no auth" mode to fall back to).
    pub api_key_env: String,
}

/// Anthropic requires `max_tokens`; `LlmNodeSpec.max_tokens` is optional
/// (matches the OpenAI-compatible backend, where it's genuinely optional)
/// — this is the fallback when the spec doesn't set one.
const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Same version pin Anthropic's own docs recommend sending explicitly
/// rather than omitting (the API has no "latest" default).
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicClient {
    client: reqwest::Client,
    cfg: AnthropicClientConfig,
}

impl AnthropicClient {
    pub fn new(cfg: AnthropicClientConfig) -> Self {
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
        let key = std::env::var(&self.cfg.api_key_env).map_err(|_| {
            LlmError::Api(format!(
                "environment variable '{}' not set for llm API key",
                self.cfg.api_key_env
            ))
        })?;

        let started = Instant::now();
        let response = self
            .client
            .post(format!(
                "{}/v1/messages",
                self.cfg.base_url.trim_end_matches('/')
            ))
            .header("x-api-key", key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&MessagesRequest {
                model: &self.cfg.model,
                max_tokens: max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
                temperature,
                messages: &[Message {
                    role: "user",
                    content: prompt,
                }],
            })
            .send()
            .await
            .map_err(|e| LlmError::Api(format!("request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LlmError::Api(format!(
                "anthropic API returned {status}: {}",
                truncate(&body, 512)
            )));
        }

        let latency_ms = started.elapsed().as_millis() as u64;
        let parsed: MessagesResponse = response
            .json()
            .await
            .map_err(|e| LlmError::Api(format!("invalid response body: {e}")))?;

        let text = parsed
            .content
            .into_iter()
            .find_map(|block| (block.block_type == "text").then_some(block.text))
            .ok_or_else(|| LlmError::Api("anthropic API returned no text block".to_string()))?;

        Ok(LlmResponse {
            text,
            tokens_prompt: parsed.usage.input_tokens,
            tokens_completion: parsed.usage.output_tokens,
            latency_ms,
        })
    }

    /// Spike (ROADMAP.md Fase 31): one turn of Anthropic-shaped
    /// tool-calling. `history` is the whole conversation so far (stateless
    /// API, resent every call). Each `ToolMessage` maps to exactly one API
    /// message — fine for the single-tool-call-per-turn shape Fase 31's v1
    /// tools use; a turn with several parallel `tool_use` blocks would need
    /// their results merged into one `user` message instead, which this
    /// doesn't do.
    pub async fn call_with_tools(
        &self,
        history: &[ToolMessage],
        tools: &[ToolDef],
        max_tokens: Option<u32>,
        temperature: Option<f32>,
    ) -> Result<LlmTurn, LlmError> {
        let key = std::env::var(&self.cfg.api_key_env).map_err(|_| {
            LlmError::Api(format!(
                "environment variable '{}' not set for llm API key",
                self.cfg.api_key_env
            ))
        })?;

        let messages = to_anthropic_messages(history);
        let wire_tools: Vec<AnthropicTool> = tools.iter().map(AnthropicTool::from).collect();

        let response = self
            .client
            .post(format!(
                "{}/v1/messages",
                self.cfg.base_url.trim_end_matches('/')
            ))
            .header("x-api-key", key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&ToolMessagesRequest {
                model: &self.cfg.model,
                max_tokens: max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
                temperature,
                messages: &messages,
                tools: &wire_tools,
            })
            .send()
            .await
            .map_err(|e| LlmError::Api(format!("request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LlmError::Api(format!(
                "anthropic API returned {status}: {}",
                truncate(&body, 512)
            )));
        }

        let parsed: ToolMessagesResponse = response
            .json()
            .await
            .map_err(|e| LlmError::Api(format!("invalid response body: {e}")))?;

        let calls: Vec<ToolCall> = parsed
            .content
            .iter()
            .filter(|b| b.block_type == "tool_use")
            .map(|b| ToolCall {
                id: b.id.clone().unwrap_or_default(),
                name: b.name.clone().unwrap_or_default(),
                arguments: b.input.clone().unwrap_or(Value::Null),
            })
            .collect();
        if !calls.is_empty() {
            return Ok(LlmTurn::ToolCalls(calls));
        }

        let text = parsed
            .content
            .into_iter()
            .find_map(|b| (b.block_type == "text").then_some(b.text.unwrap_or_default()))
            .ok_or_else(|| LlmError::Api("anthropic API returned no text block".to_string()))?;
        Ok(LlmTurn::Text(text))
    }
}

/// Rebuilds the Anthropic `messages` array from a `ToolMessage` history.
/// `tool_use`/`tool_result` are content blocks, never top-level fields —
/// unlike OpenAI, Anthropic has no `role: "tool"`, a tool result is a
/// `user` message whose content is a `tool_result` block.
fn to_anthropic_messages(history: &[ToolMessage]) -> Vec<AnthropicMessage> {
    history
        .iter()
        .map(|m| match m {
            ToolMessage::User(text) => AnthropicMessage {
                role: "user",
                content: vec![AnthropicContentBlock::Text { text: text.clone() }],
            },
            ToolMessage::AssistantToolCalls(calls) => AnthropicMessage {
                role: "assistant",
                content: calls
                    .iter()
                    .map(|c| AnthropicContentBlock::ToolUse {
                        id: c.id.clone(),
                        name: c.name.clone(),
                        input: c.arguments.clone(),
                    })
                    .collect(),
            },
            ToolMessage::ToolResult { call_id, content } => AnthropicMessage {
                role: "user",
                content: vec![AnthropicContentBlock::ToolResult {
                    tool_use_id: call_id.clone(),
                    content: content.clone(),
                }],
            },
        })
        .collect()
}

#[derive(Serialize)]
struct ToolMessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    messages: &'a [AnthropicMessage],
    tools: &'a [AnthropicTool],
}

#[derive(Serialize)]
struct AnthropicMessage {
    role: &'static str,
    content: Vec<AnthropicContentBlock>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

#[derive(Serialize)]
struct AnthropicTool {
    name: String,
    description: String,
    input_schema: Value,
}

impl From<&ToolDef> for AnthropicTool {
    fn from(t: &ToolDef) -> Self {
        AnthropicTool {
            name: t.name.clone(),
            description: t.description.clone(),
            input_schema: t.schema.clone(),
        }
    }
}

#[derive(Deserialize)]
struct ToolMessagesResponse {
    content: Vec<ToolContentBlock>,
}

#[derive(Deserialize)]
struct ToolContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

/// Same truncation reasoning as `client::truncate` — bounds how much of an
/// upstream error body reaches a lower-privilege run-log reader.
fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push_str("…[truncated]");
    out
}

#[derive(Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    messages: &'a [Message<'a>],
}

#[derive(Serialize)]
struct Message<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct MessagesResponse {
    content: Vec<ContentBlock>,
    usage: Usage,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct Usage {
    input_tokens: u32,
    output_tokens: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn calls_messages_endpoint_with_api_key_header_and_parses_usage() {
        std::env::set_var("NEXUS_TEST_ANTHROPIC_KEY", "sk-ant-test");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .and(header("x-api-key", "sk-ant-test"))
            .and(header("anthropic-version", ANTHROPIC_VERSION))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{"type": "text", "text": "hello from claude"}],
                "usage": {"input_tokens": 10, "output_tokens": 3}
            })))
            .mount(&server)
            .await;

        let client = AnthropicClient::new(AnthropicClientConfig {
            base_url: server.uri(),
            model: "claude-sonnet-5".to_string(),
            api_key_env: "NEXUS_TEST_ANTHROPIC_KEY".to_string(),
        });
        let resp = client.call("hi", None, None).await.unwrap();

        assert_eq!(resp.text, "hello from claude");
        assert_eq!(resp.tokens_prompt, 10);
        assert_eq!(resp.tokens_completion, 3);
    }

    #[tokio::test]
    async fn missing_api_key_env_var_is_a_clear_error() {
        let server = MockServer::start().await;
        let client = AnthropicClient::new(AnthropicClientConfig {
            base_url: server.uri(),
            model: "claude-sonnet-5".to_string(),
            api_key_env: "NEXUS_TEST_ANTHROPIC_KEY_DOES_NOT_EXIST".to_string(),
        });
        let err = client.call("hi", None, None).await.unwrap_err();
        assert!(matches!(err, LlmError::Api(_)));
    }

    #[tokio::test]
    async fn non_success_status_is_surfaced_as_api_error() {
        std::env::set_var("NEXUS_TEST_ANTHROPIC_KEY_2", "sk-ant-test");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;

        let client = AnthropicClient::new(AnthropicClientConfig {
            base_url: server.uri(),
            model: "claude-sonnet-5".to_string(),
            api_key_env: "NEXUS_TEST_ANTHROPIC_KEY_2".to_string(),
        });
        let err = client.call("hi", None, None).await.unwrap_err();
        assert!(matches!(err, LlmError::Api(_)));
    }

    fn weather_tool() -> ToolDef {
        ToolDef {
            name: "get_weather".to_string(),
            description: "look up current weather for a city".to_string(),
            schema: serde_json::json!({
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"]
            }),
        }
    }

    #[tokio::test]
    async fn call_with_tools_parses_a_tool_use_turn() {
        std::env::set_var("NEXUS_TEST_ANTHROPIC_TOOLS_KEY", "sk-ant-test");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "get_weather",
                    "input": {"city": "Lisboa"}
                }],
                "usage": {"input_tokens": 10, "output_tokens": 3}
            })))
            .mount(&server)
            .await;

        let client = AnthropicClient::new(AnthropicClientConfig {
            base_url: server.uri(),
            model: "claude-sonnet-5".to_string(),
            api_key_env: "NEXUS_TEST_ANTHROPIC_TOOLS_KEY".to_string(),
        });
        let turn = client
            .call_with_tools(
                &[ToolMessage::User("qual o clima em Lisboa?".to_string())],
                &[weather_tool()],
                None,
                None,
            )
            .await
            .unwrap();

        match turn {
            LlmTurn::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].name, "get_weather");
                assert_eq!(calls[0].arguments, serde_json::json!({"city": "Lisboa"}));
            }
            LlmTurn::Text(_) => panic!("expected a tool call turn"),
        }
    }

    #[tokio::test]
    async fn call_with_tools_continuation_after_tool_result_returns_text() {
        std::env::set_var("NEXUS_TEST_ANTHROPIC_TOOLS_KEY_2", "sk-ant-test");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{"type": "text", "text": "Ensolarado, 24°C em Lisboa."}],
                "usage": {"input_tokens": 20, "output_tokens": 8}
            })))
            .mount(&server)
            .await;

        let client = AnthropicClient::new(AnthropicClientConfig {
            base_url: server.uri(),
            model: "claude-sonnet-5".to_string(),
            api_key_env: "NEXUS_TEST_ANTHROPIC_TOOLS_KEY_2".to_string(),
        });
        let history = vec![
            ToolMessage::User("qual o clima em Lisboa?".to_string()),
            ToolMessage::AssistantToolCalls(vec![ToolCall {
                id: "toolu_1".to_string(),
                name: "get_weather".to_string(),
                arguments: serde_json::json!({"city": "Lisboa"}),
            }]),
            ToolMessage::ToolResult {
                call_id: "toolu_1".to_string(),
                content: "24°C, ensolarado".to_string(),
            },
        ];
        let turn = client
            .call_with_tools(&history, &[weather_tool()], None, None)
            .await
            .unwrap();

        assert_eq!(
            turn,
            LlmTurn::Text("Ensolarado, 24°C em Lisboa.".to_string())
        );
    }
}
