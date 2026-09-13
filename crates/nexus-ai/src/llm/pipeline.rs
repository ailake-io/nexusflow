use crate::llm::anthropic_client::{AnthropicClient, AnthropicClientConfig};
use crate::llm::client::{LlmClient, LlmClientConfig};
use crate::llm::common::{append_text_column, cache_key, LlmCache, LlmError};
use arrow_array::RecordBatch;
use arrow_cast::display::array_value_to_string;
use nexus_core::{LlmModelConfig, LlmNodeSpec};

/// A loaded LLM backend, reused across every batch of a run — mirrors
/// `embedding::EmbeddingBackend`'s shape (loaded once via
/// [`load_llm_backend`], not per batch).
pub enum LlmBackend {
    Api(LlmClient),
    Anthropic(AnthropicClient),
}

pub fn load_llm_backend(spec: &LlmNodeSpec) -> LlmBackend {
    match &spec.model {
        LlmModelConfig::Api {
            base_url,
            model,
            api_key_env,
            ..
        } => LlmBackend::Api(LlmClient::new(LlmClientConfig {
            base_url: base_url.clone(),
            model: model.clone(),
            api_key_env: api_key_env.clone(),
        })),
        LlmModelConfig::Anthropic {
            base_url,
            model,
            api_key_env,
            ..
        } => LlmBackend::Anthropic(AnthropicClient::new(AnthropicClientConfig {
            base_url: base_url.clone(),
            model: model.clone(),
            api_key_env: api_key_env.clone(),
        })),
    }
}

/// Metadata for one LLM call, returned alongside the transformed batch so
/// the caller (`nexus-server::runner::apply_llm_stage`) can log each call
/// via `RunLogger` — nexus-ai never logs anything itself (layering:
/// `RunLogStore`/`RunLogger` live only in nexus-server, same reason
/// `EmbeddingError` never touches them either).
#[derive(Debug, Clone)]
pub struct LlmCallStats {
    pub tokens_prompt: u32,
    pub tokens_completion: u32,
    pub latency_ms: u64,
    pub prompt_len_chars: usize,
    pub response_len_chars: usize,
    /// Full prompt/response text — always populated (cheap, already in
    /// memory), but the caller decides whether to actually log it
    /// (`LlmNodeSpec.log_full_content`, off by default). nexus-ai never
    /// makes that logging decision itself (layering: `RunLogger` lives in
    /// nexus-server).
    pub prompt: String,
    pub response: String,
}

pub struct LlmApplyResult {
    pub batch: RecordBatch,
    pub calls: Vec<LlmCallStats>,
}

/// Builds the prompt for one row by replacing every `{column}` placeholder
/// in `template` with that column's stringified value. Pure and testable
/// without network access — same spirit as `chunking.rs` being pure.
/// Unknown placeholders (a `{name}` not in `values`) are left as-is rather
/// than erroring, so a template referencing an optional/future column
/// degrades visibly instead of failing the whole run.
pub fn build_prompt(template: &str, values: &[(&str, String)]) -> String {
    let mut prompt = template.to_string();
    for (column, value) in values {
        prompt = prompt.replace(&format!("{{{column}}}"), value);
    }
    prompt
}

/// Applies the LLM stage to one `RecordBatch`: one call per row (unlike
/// embeddings, chat completion APIs don't batch multiple prompts into one
/// request), 1 row in -> 1 row out — no chunking/expansion. The `backend`
/// must be loaded once per pipeline run (see [`load_llm_backend`]) and
/// reused across all batches.
///
/// `cache`, when `Some`, is checked before every call and written after a
/// miss (LLMOPS_IMPLEMENTATION_PLAN.md Marco L3) — a hit produces an
/// `LlmCallStats` with zero tokens and ~0 latency, since no real call was
/// made. `cache` is a trait object (see `LlmCache`'s doc comment) so this
/// crate never depends on `nexus-connector-redis` directly.
///
/// `template` is the already-resolved prompt text — `spec.prompt` is only
/// a `PromptRef` (name + optional version, Marco L4); resolving that
/// reference against `nexus-server::prompt_template_store` happens in the
/// caller, not here (same layering reasoning as `LlmCache`: this crate
/// never touches a database).
pub async fn apply_llm(
    batch: &RecordBatch,
    spec: &LlmNodeSpec,
    template: &str,
    backend: &LlmBackend,
    cache: Option<&dyn LlmCache>,
) -> Result<LlmApplyResult, LlmError> {
    let model = match &spec.model {
        LlmModelConfig::Api { model, .. } => model,
        LlmModelConfig::Anthropic { model, .. } => model,
    };

    let column_indices: Vec<(String, usize)> = spec
        .input_columns
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|idx| (name.clone(), idx))
                .map_err(|_| {
                    LlmError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(format!(
                        "llm input column '{name}' not found"
                    )))
                })
        })
        .collect::<Result<_, _>>()?;

    let mut responses = Vec::with_capacity(batch.num_rows());
    let mut calls = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(column_indices.len());
        for (name, idx) in &column_indices {
            let value = array_value_to_string(batch.column(*idx), row).map_err(LlmError::Arrow)?;
            values.push((name.as_str(), value));
        }
        let prompt = build_prompt(template, &values);
        let key = cache_key(model, &prompt, spec.max_tokens, spec.temperature);

        let cached = match cache {
            Some(c) => c.get(&key).await,
            None => None,
        };
        let (response_text, tokens_prompt, tokens_completion, latency_ms) = match cached {
            Some(text) => (text, 0, 0, 0),
            None => {
                let resp = match backend {
                    LlmBackend::Api(client) => {
                        client
                            .call(&prompt, spec.max_tokens, spec.temperature)
                            .await?
                    }
                    LlmBackend::Anthropic(client) => {
                        client
                            .call(&prompt, spec.max_tokens, spec.temperature)
                            .await?
                    }
                };
                if let (Some(c), Some(cache_spec)) = (cache, &spec.cache) {
                    c.set(&key, &resp.text, cache_spec.ttl_seconds).await;
                }
                (
                    resp.text,
                    resp.tokens_prompt,
                    resp.tokens_completion,
                    resp.latency_ms,
                )
            }
        };
        calls.push(LlmCallStats {
            tokens_prompt,
            tokens_completion,
            latency_ms,
            prompt_len_chars: prompt.chars().count(),
            response_len_chars: response_text.chars().count(),
            prompt: prompt.clone(),
            response: response_text.clone(),
        });
        responses.push(response_text);
    }

    let batch = append_text_column(batch, &responses, &spec.output_column)?;
    Ok(LlmApplyResult { batch, calls })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolates_known_placeholders() {
        let out = build_prompt(
            "Summarize: {body} (by {author})",
            &[
                ("body", "the quick fox".to_string()),
                ("author", "jane".to_string()),
            ],
        );
        assert_eq!(out, "Summarize: the quick fox (by jane)");
    }

    #[test]
    fn leaves_unknown_placeholders_untouched() {
        let out = build_prompt("Hello {name}, {missing}", &[("name", "bob".to_string())]);
        assert_eq!(out, "Hello bob, {missing}");
    }

    #[tokio::test]
    async fn applies_llm_call_per_row_and_appends_output_column() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "42"}}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 1}
            })))
            .mount(&server)
            .await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("question", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a?", "b?"])),
            ],
        )
        .unwrap();

        let spec = LlmNodeSpec {
            prompt: nexus_core::PromptRef {
                name: "test-prompt".to_string(),
                version: None,
            },
            input_columns: vec!["question".to_string()],
            output_column: "answer".to_string(),
            model: LlmModelConfig::Api {
                base_url: server.uri(),
                model: "gpt-test".to_string(),
                api_key_env: None,
                cost_per_1k_prompt_tokens: None,
                cost_per_1k_completion_tokens: None,
            },
            max_tokens: None,
            temperature: None,
            log_full_content: false,
            cache: None,
            eval: vec![],
            eval_scoring: nexus_core::EvalScoringMode::TokenSimilarity,
        };
        let backend = load_llm_backend(&spec);

        let result = apply_llm(&batch, &spec, "Answer: {question}", &backend, None)
            .await
            .unwrap();
        assert_eq!(result.calls.len(), 2);
        assert_eq!(result.calls[0].tokens_prompt, 3);
        assert_eq!(result.calls[0].tokens_completion, 1);

        let out = result.batch;
        assert_eq!(out.num_columns(), 3);
        let answer_col = out
            .column(out.schema().index_of("answer").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(answer_col.value(0), "42");
        assert_eq!(answer_col.value(1), "42");
    }

    struct InMemoryCache {
        store: std::sync::Mutex<std::collections::HashMap<String, String>>,
        sets: std::sync::atomic::AtomicUsize,
    }

    impl InMemoryCache {
        fn new() -> Self {
            Self {
                store: std::sync::Mutex::new(std::collections::HashMap::new()),
                sets: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl LlmCache for InMemoryCache {
        async fn get(&self, key: &str) -> Option<String> {
            self.store.lock().unwrap().get(key).cloned()
        }

        async fn set(&self, key: &str, value: &str, _ttl_seconds: u64) {
            self.sets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.store
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
        }
    }

    #[tokio::test]
    async fn cache_hit_skips_the_call_and_reports_zero_tokens() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Expect exactly ONE real HTTP call — a second call for the same
        // row (same prompt) must be served entirely from cache.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "cached-answer"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 2}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("question", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["same question?"])),
            ],
        )
        .unwrap();

        let spec = LlmNodeSpec {
            prompt: nexus_core::PromptRef {
                name: "test-prompt".to_string(),
                version: None,
            },
            input_columns: vec!["question".to_string()],
            output_column: "answer".to_string(),
            model: LlmModelConfig::Api {
                base_url: server.uri(),
                model: "gpt-test".to_string(),
                api_key_env: None,
                cost_per_1k_prompt_tokens: None,
                cost_per_1k_completion_tokens: None,
            },
            max_tokens: None,
            temperature: None,
            log_full_content: false,
            cache: Some(nexus_core::LlmCacheSpec {
                url: "redis://unused-in-test".to_string(),
                ttl_seconds: 60,
            }),
            eval: vec![],
            eval_scoring: nexus_core::EvalScoringMode::TokenSimilarity,
        };
        let backend = load_llm_backend(&spec);
        let cache = InMemoryCache::new();
        let template = "Answer: {question}";

        let first = apply_llm(&batch, &spec, template, &backend, Some(&cache))
            .await
            .unwrap();
        assert_eq!(first.calls[0].tokens_prompt, 5);
        assert_eq!(first.calls[0].tokens_completion, 2);

        let second = apply_llm(&batch, &spec, template, &backend, Some(&cache))
            .await
            .unwrap();
        assert_eq!(second.calls[0].tokens_prompt, 0);
        assert_eq!(second.calls[0].tokens_completion, 0);
        assert_eq!(second.calls[0].latency_ms, 0);
        assert_eq!(second.calls[0].response, "cached-answer");

        // Only the miss (first call) ever wrote to the cache.
        assert_eq!(cache.sets.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
