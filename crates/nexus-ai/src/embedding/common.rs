#[cfg(feature = "cpu")]
use crate::embedding::model::ModelError;
use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;
use thiserror::Error;

/// Shared between both embedding backends (`cpu`/`cuda`/`metal`'s local
/// ONNX `EmbeddingModel` and `api`'s `ApiEmbeddingModel`) so `pipeline.rs`
/// can dispatch to either without a backend-specific error type leaking
/// into the DAG execution path.
#[derive(Debug, Error)]
pub enum EmbeddingError {
    #[cfg(feature = "cpu")]
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error("tokenizer error: {0}")]
    Tokenizer(String),
    // `ort::Error` is generic over the operation's typestate marker, so
    // different `ort` calls produce distinct Rust types — normalized to a
    // string here rather than fighting the typestate with per-call `From`s.
    #[error("onnx runtime error: {0}")]
    Ort(String),
    #[error("embedding API error: {0}")]
    Api(String),
    #[error("{0}")]
    UnsupportedBackend(String),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("model output shape not embeddable: {0}")]
    UnexpectedOutputShape(String),
}

/// Appends `embeddings` (one vector per row of `batch`, all `dimension` wide)
/// as a `FixedSizeList<Float32>` column named `column_name` — see
/// ARCHITECTURE.md §4.3.
pub fn append_embedding_column(
    batch: &RecordBatch,
    embeddings: &[Vec<f32>],
    dimension: usize,
    column_name: &str,
) -> Result<RecordBatch, EmbeddingError> {
    if embeddings.len() != batch.num_rows() {
        return Err(EmbeddingError::UnexpectedOutputShape(format!(
            "{} embeddings for {} rows",
            embeddings.len(),
            batch.num_rows()
        )));
    }

    let flat: Float32Array = embeddings
        .iter()
        .flat_map(|v| v.iter().copied())
        .collect::<Vec<f32>>()
        .into();
    let field = Arc::new(Field::new("item", DataType::Float32, false));
    let embedding_array =
        FixedSizeListArray::try_new(field, dimension as i32, Arc::new(flat), None)?;

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    fields.push(Field::new(
        column_name,
        embedding_array.data_type().clone(),
        false,
    ));
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(embedding_array));

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
    fn appends_fixed_size_list_column() {
        let batch = sample_batch();
        let embeddings = vec![vec![0.1, 0.2, 0.3], vec![0.4, 0.5, 0.6]];
        let out = append_embedding_column(&batch, &embeddings, 3, "embedding").unwrap();

        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(1).name(), "embedding");
        let list = out
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(list.value_length(), 3);
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn rejects_row_count_mismatch() {
        let batch = sample_batch();
        let embeddings = vec![vec![0.1, 0.2, 0.3]];
        let err = append_embedding_column(&batch, &embeddings, 3, "embedding").unwrap_err();
        assert!(matches!(err, EmbeddingError::UnexpectedOutputShape(_)));
    }
}
