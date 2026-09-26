use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LlmError {
    #[error("llm API error: {0}")]
    Api(String),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("model output shape not appendable: {0}")]
    UnexpectedOutputShape(String),
}

/// Response cache for LLM calls (LLMOPS_IMPLEMENTATION_PLAN.md Marco L3) —
/// a trait, not a concrete Redis dependency, so nexus-ai never needs to
/// know about `nexus-connector-redis` (a connector crate, layered above
/// this one). `nexus-server::runner::apply_llm_stage` implements this over
/// `nexus_connector_redis::RedisKvClient` and passes it in; tests use an
/// in-memory `HashMap`-backed implementation.
#[async_trait]
pub trait LlmCache: Send + Sync {
    async fn get(&self, key: &str) -> Option<String>;
    /// Cache-write failures are the caller's problem to log, not this
    /// trait's — `apply_llm` treats a write failure as non-fatal (a cache
    /// miss next time is a cost/latency regression, not a correctness
    /// bug), so this returns nothing to react to.
    async fn set(&self, key: &str, value: &str, ttl_seconds: u64);
}

/// Cache key for one LLM call — same inputs must always produce the same
/// key, and any of these fields differing must produce a different one
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L3: "sha256(model + prompt +
/// max_tokens + temperature)"). Pure and testable without a real cache.
pub fn cache_key(
    model: &str,
    prompt: &str,
    max_tokens: Option<u32>,
    temperature: Option<f32>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(model.as_bytes());
    hasher.update(b"\0");
    hasher.update(prompt.as_bytes());
    hasher.update(b"\0");
    hasher.update(
        max_tokens
            .map(|v| v.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\0");
    hasher.update(
        temperature
            .map(|v| v.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    format!("nexusflow:llm-cache:{}", hex::encode(hasher.finalize()))
}

/// One tool offered to the model. `schema` is a plain JSON Schema object —
/// both backends want the same content (name/description/parameter shape),
/// only the wire key differs (`parameters` for OpenAI, `input_schema` for
/// Anthropic), so each client serializes this into its own wire shape
/// rather than this type trying to match either one directly.
#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub schema: Value,
}

/// One call the model wants executed, echoed back so the caller (the
/// agent loop, Fase 31) can attribute a result to it on the next turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// One model turn under tool-calling. A turn that mixes reasoning text
/// with tool calls (Anthropic allows text blocks before `tool_use`) still
/// collapses to `ToolCalls` here — the spike only needs to prove the round
/// trip, not preserve incidental commentary the model emits alongside a
/// call.
#[derive(Debug, Clone, PartialEq)]
pub enum LlmTurn {
    Text(String),
    ToolCalls(Vec<ToolCall>),
}

/// `call_with_tools`'s full result — the turn plus token usage, so a
/// multi-turn caller (the agent loop, ROADMAP.md Fase 31) can accumulate
/// cost across every call the way `pipeline_run_llm_stats_store.rs` does
/// for the single-call batch `llm` node, instead of losing usage data on
/// every turn but the last.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolTurn {
    pub turn: LlmTurn,
    pub tokens_prompt: u32,
    pub tokens_completion: u32,
}

/// One entry in a tool-calling conversation. Both APIs are stateless — the
/// full history is resent on every call — so this is the minimum shape a
/// multi-turn tool loop needs regardless of backend; there's no leaner
/// representation that still lets a caller replay a conversation.
#[derive(Debug, Clone)]
pub enum ToolMessage {
    /// The agent's system prompt/persona (ROADMAP.md Fase 31) — at most one
    /// per history, conventionally first. OpenAI serializes this as an
    /// ordinary `role: "system"` message; Anthropic's API has no such
    /// role, it's a separate top-level request field instead — each client
    /// handles the difference itself, see `anthropic_client.rs`'s
    /// `call_with_tools` doc comment.
    System(String),
    User(String),
    AssistantToolCalls(Vec<ToolCall>),
    ToolResult {
        call_id: String,
        content: String,
    },
}

/// Appends `responses` (one string per row of `batch`) as a `Utf8` column
/// named `column_name` — same shape as
/// `embedding::append_embedding_column`, but a plain string column since an
/// LLM stage is 1 row in -> 1 row out (no chunking/expansion).
pub fn append_text_column(
    batch: &RecordBatch,
    responses: &[String],
    column_name: &str,
) -> Result<RecordBatch, LlmError> {
    if responses.len() != batch.num_rows() {
        return Err(LlmError::UnexpectedOutputShape(format!(
            "{} responses for {} rows",
            responses.len(),
            batch.num_rows()
        )));
    }

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    fields.push(Field::new(column_name, DataType::Utf8, false));
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(responses.to_vec())));

    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap()
    }

    #[test]
    fn appends_utf8_column() {
        let batch = sample_batch();
        let responses = vec!["a".to_string(), "b".to_string()];
        let out = append_text_column(&batch, &responses, "answer").unwrap();

        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(1).name(), "answer");
        let col = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "a");
        assert_eq!(col.value(1), "b");
    }

    #[test]
    fn rejects_row_count_mismatch() {
        let batch = sample_batch();
        let responses = vec!["a".to_string()];
        let err = append_text_column(&batch, &responses, "answer").unwrap_err();
        assert!(matches!(err, LlmError::UnexpectedOutputShape(_)));
    }
}
