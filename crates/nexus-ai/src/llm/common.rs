use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
