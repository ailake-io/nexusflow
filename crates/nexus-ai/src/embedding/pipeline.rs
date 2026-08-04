use crate::chunking::{
    chunk_fixed_window, chunk_recursive_character, FixedWindowConfig, RecursiveCharacterConfig,
};
use crate::embedding::{
    append_embedding_column, EmbeddingError, EmbeddingModel, EmbeddingModelConfig,
};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{ChunkingSpec, EmbeddingModelSpec, EmbeddingSpec};
use std::sync::Arc;

/// Applies chunking + ONNX embedding to one `RecordBatch`, returning a new
/// batch where each input row may have become N chunk-rows. Non-text columns
/// are replicated for every chunk produced from their original row.
pub async fn apply_embedding(
    batch: &RecordBatch,
    spec: &EmbeddingSpec,
) -> Result<RecordBatch, EmbeddingError> {
    let source_idx = batch.schema().index_of(&spec.source_column).map_err(|_| {
        EmbeddingError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(format!(
            "embedding source column '{}' not found",
            spec.source_column
        )))
    })?;
    let text_column = batch
        .column(source_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            EmbeddingError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(format!(
                "embedding source column '{}' must be Utf8",
                spec.source_column
            )))
        })?;

    // 1. Chunk every row's text.
    let mut chunks_per_row: Vec<Vec<String>> = Vec::with_capacity(batch.num_rows());
    let mut total_chunks = 0usize;
    for row in 0..batch.num_rows() {
        let text = text_column.value(row);
        let chunks = match &spec.chunking {
            ChunkingSpec::FixedWindow {
                chunk_size,
                overlap,
            } => chunk_fixed_window(
                text,
                &FixedWindowConfig {
                    chunk_size: *chunk_size,
                    overlap: *overlap,
                },
            ),
            ChunkingSpec::RecursiveCharacter {
                chunk_size,
                overlap,
                separators,
            } => chunk_recursive_character(
                text,
                &RecursiveCharacterConfig {
                    chunk_size: *chunk_size,
                    overlap: *overlap,
                    separators: separators.clone().unwrap_or_else(|| {
                        vec![
                            "\n\n".to_string(),
                            "\n".to_string(),
                            ". ".to_string(),
                            " ".to_string(),
                            String::new(),
                        ]
                    }),
                },
            ),
        };
        total_chunks += chunks.len();
        chunks_per_row.push(chunks);
    }

    if total_chunks == 0 {
        // No text produced chunks — return an empty batch with the new schema.
        let schema = append_embedding_schema(&batch.schema(), &spec.output_column, spec.dimension)?;
        return RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|f| new_empty_array(f.data_type()))
                .collect(),
        )
        .map_err(EmbeddingError::Arrow);
    }

    // 2. Build the expanded text column (one entry per chunk).
    let mut chunk_texts: Vec<String> = Vec::with_capacity(total_chunks);
    for chunks in &chunks_per_row {
        chunk_texts.extend(chunks.iter().cloned());
    }

    // 3. Replicate the other columns once per chunk.
    let mut expanded_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        if col_idx == source_idx {
            // Replace source column with the chunked text column.
            expanded_columns.push(Arc::new(StringArray::from(chunk_texts.clone())) as ArrayRef);
            continue;
        }
        let original = batch.column(col_idx);
        expanded_columns.push(replicate_array(original, &chunks_per_row)?);
    }

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    fields[source_idx] = Field::new(&spec.source_column, DataType::Utf8, false);
    let schema_without_embedding = Arc::new(Schema::new(fields));
    let expanded = RecordBatch::try_new(schema_without_embedding, expanded_columns)
        .map_err(EmbeddingError::Arrow)?;

    // 4. Load model and embed all chunks.
    let model_cfg = EmbeddingModelConfig {
        model: ModelConfig::from(&spec.model),
        tokenizer_filename: spec.model.tokenizer_filename.clone(),
        dimension: spec.dimension,
        max_length: spec.model.max_length,
    };
    let mut model = EmbeddingModel::load(&model_cfg).await?;
    let embeddings = model.embed_batch(&chunk_texts)?;

    // 5. Append the embedding column.
    append_embedding_column(&expanded, &embeddings, spec.dimension, &spec.output_column)
}

fn append_embedding_schema(
    schema: &SchemaRef,
    column_name: &str,
    dimension: usize,
) -> Result<SchemaRef, EmbeddingError> {
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let embedding_field = Field::new(
        column_name,
        DataType::FixedSizeList(item_field, dimension as i32),
        false,
    );
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(embedding_field);
    Ok(Arc::new(Schema::new(fields)))
}

fn replicate_array(
    original: &ArrayRef,
    chunks_per_row: &[Vec<String>],
) -> Result<ArrayRef, EmbeddingError> {
    let _len: usize = chunks_per_row.iter().map(|v| v.len()).sum();
    let data_type = original.data_type();

    macro_rules! replicate_primitive {
        ($array_type:ty, $builder:expr) => {{
            let arr = original
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    EmbeddingError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(
                        "unexpected array type".into(),
                    ))
                })?;
            let mut builder = $builder;
            for (row, chunks) in chunks_per_row.iter().enumerate() {
                for _ in 0..chunks.len() {
                    builder.append_value(arr.value(row));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }

    match data_type {
        DataType::Utf8 => {
            let arr = original
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    EmbeddingError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(
                        "unexpected array type for Utf8".into(),
                    ))
                })?;
            let mut builder = arrow_array::builder::StringBuilder::new();
            for (row, chunks) in chunks_per_row.iter().enumerate() {
                for _ in 0..chunks.len() {
                    builder.append_value(arr.value(row));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 => replicate_primitive!(
            arrow_array::Int64Array,
            arrow_array::builder::Int64Builder::new()
        ),
        DataType::Float64 => replicate_primitive!(
            arrow_array::Float64Array,
            arrow_array::builder::Float64Builder::new()
        ),
        DataType::Boolean => replicate_primitive!(
            arrow_array::BooleanArray,
            arrow_array::builder::BooleanBuilder::new()
        ),
        DataType::Float32 => replicate_primitive!(
            arrow_array::Float32Array,
            arrow_array::builder::Float32Builder::new()
        ),
        other => Err(EmbeddingError::Arrow(
            arrow_schema::ArrowError::InvalidArgumentError(format!(
                "replicating arrays of type {other:?} in embedding stage is not supported"
            )),
        )),
    }
}

fn new_empty_array(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        DataType::Int64 => Arc::new(arrow_array::Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        DataType::Float64 => {
            Arc::new(arrow_array::Float64Array::from(Vec::<f64>::new())) as ArrayRef
        }
        DataType::Float32 => {
            Arc::new(arrow_array::Float32Array::from(Vec::<f32>::new())) as ArrayRef
        }
        DataType::Boolean => {
            Arc::new(arrow_array::BooleanArray::from(Vec::<bool>::new())) as ArrayRef
        }
        DataType::FixedSizeList(field, size) => {
            let values = new_empty_array(field.data_type());
            Arc::new(
                FixedSizeListArray::try_new(field.clone(), *size, values, None)
                    .expect("valid empty FixedSizeList"),
            ) as ArrayRef
        }
        other => panic!("new_empty_array not implemented for {other:?}"),
    }
}

use crate::embedding::model::ModelConfig;

impl From<&EmbeddingModelSpec> for ModelConfig {
    fn from(spec: &EmbeddingModelSpec) -> Self {
        Self {
            repo_id: spec.repo.clone(),
            // Pin to the repo's default revision (usually "main") for now;
            // reproducibility could be enhanced by adding an explicit revision
            // field to EmbeddingModelSpec later.
            revision: "main".to_string(),
            filename: spec.filename.clone(),
        }
    }
}
