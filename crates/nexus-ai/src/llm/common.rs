use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
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
