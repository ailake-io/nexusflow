use crate::chunking::{
    chunk_fixed_window, chunk_recursive_character, FixedWindowConfig, RecursiveCharacterConfig,
};
use crate::embedding::{append_embedding_column, EmbeddingError};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{ChunkingSpec, EmbeddingModelSpec, EmbeddingSpec};
use std::sync::Arc;

/// A loaded embedding backend that can embed multiple batches without
/// reloading the model/session each time. Created once per pipeline run via
/// [`load_embedding_backend`] and reused across every batch of that run.
#[cfg(any(feature = "cpu", feature = "api"))]
pub enum EmbeddingBackend {
    // Boxed: EmbeddingModel holds an ort::Session + tokenizers::Tokenizer
    // (>1KB), versus ApiEmbeddingModel's single reqwest::Client handle
    // (~80 bytes) — an unboxed variant would force every EmbeddingBackend
    // value to be sized for the larger one regardless of which is active
    // (clippy::large_enum_variant, real with cpu+api both compiled in).
    #[cfg(feature = "cpu")]
    Onnx(Box<crate::embedding::EmbeddingModel>),
    #[cfg(feature = "api")]
    Api(crate::embedding::ApiEmbeddingModel),
}

#[cfg(any(feature = "cpu", feature = "api"))]
impl EmbeddingBackend {
    pub async fn embed(&mut self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        match self {
            #[cfg(feature = "cpu")]
            EmbeddingBackend::Onnx(model) => model.embed_batch(texts),
            #[cfg(feature = "api")]
            EmbeddingBackend::Api(model) => model.embed_batch(texts).await,
        }
    }
}

/// Loads whichever backend `spec.model` selects once per pipeline run. Each
/// branch is only compiled when its matching Cargo feature is on; if the DAG
/// spec asks for a backend this binary wasn't built with, that's a runtime
/// config error (clear message), not a compile-time one.
#[cfg(any(feature = "cpu", feature = "api"))]
pub async fn load_embedding_backend(
    spec: &EmbeddingSpec,
) -> Result<EmbeddingBackend, EmbeddingError> {
    match &spec.model {
        #[cfg(feature = "cpu")]
        EmbeddingModelSpec::Onnx {
            repo,
            filename,
            tokenizer_filename,
            max_length,
        } => {
            use crate::embedding::model::ModelConfig;
            use crate::embedding::EmbeddingModelConfig;
            let model_cfg = EmbeddingModelConfig {
                model: ModelConfig {
                    repo_id: repo.clone(),
                    revision: "main".to_string(),
                    filename: filename.clone(),
                },
                tokenizer_filename: tokenizer_filename.clone(),
                dimension: spec.dimension,
                max_length: *max_length,
            };
            let model = crate::embedding::EmbeddingModel::load(&model_cfg).await?;
            Ok(EmbeddingBackend::Onnx(Box::new(model)))
        }
        #[cfg(not(feature = "cpu"))]
        EmbeddingModelSpec::Onnx { .. } => Err(EmbeddingError::UnsupportedBackend(
            "embedding model backend 'onnx' requires nexus-ai's `cpu` feature, which is not compiled into this binary".to_string(),
        )),
        #[cfg(feature = "api")]
        EmbeddingModelSpec::Api {
            base_url,
            model,
            api_key_env,
        } => {
            let client = crate::embedding::ApiEmbeddingModel::new(crate::embedding::ApiEmbeddingConfig {
                base_url: base_url.clone(),
                model: model.clone(),
                api_key_env: api_key_env.clone(),
            });
            Ok(EmbeddingBackend::Api(client))
        }
        #[cfg(not(feature = "api"))]
        EmbeddingModelSpec::Api { .. } => Err(EmbeddingError::UnsupportedBackend(
            "embedding model backend 'api' requires nexus-ai's `api` feature, which is not compiled into this binary".to_string(),
        )),
    }
}

/// Applies chunking + embedding to one `RecordBatch`, returning a new batch
/// where each input row may have become N chunk-rows. Non-text columns are
/// replicated for every chunk produced from their original row. The embedding
/// `backend` must be loaded once per pipeline run (see
/// [`load_embedding_backend`]) and reused across all batches — this avoids
/// reloading the ONNX model or HTTP client for every batch.
#[cfg(any(feature = "cpu", feature = "api"))]
pub async fn apply_embedding(
    batch: &RecordBatch,
    spec: &EmbeddingSpec,
    backend: &mut EmbeddingBackend,
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

    // 1. Chunk every row's text. NULL source text expands to zero chunks so
    // the row is dropped from the embedded output instead of being silently
    // turned into an empty string (A03).
    let mut chunks_per_row: Vec<Vec<String>> = Vec::with_capacity(batch.num_rows());
    let mut total_chunks = 0usize;
    for row in 0..batch.num_rows() {
        if text_column.is_null(row) {
            chunks_per_row.push(Vec::new());
            continue;
        }
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
        let columns = schema
            .fields()
            .iter()
            .map(|f| new_empty_array(f.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        return RecordBatch::try_new(schema.clone(), columns).map_err(EmbeddingError::Arrow);
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

    // 4. Embed all chunks via the pre-loaded backend.
    let embeddings = backend.embed(&chunk_texts).await?;

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
                    if arr.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(row));
                    }
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
                    if arr.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(row));
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let arr = original
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .ok_or_else(|| {
                    EmbeddingError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(
                        "unexpected array type for LargeUtf8".into(),
                    ))
                })?;
            let mut builder = arrow_array::builder::LargeStringBuilder::new();
            for (row, chunks) in chunks_per_row.iter().enumerate() {
                for _ in 0..chunks.len() {
                    if arr.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(row));
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int8 => replicate_primitive!(
            arrow_array::Int8Array,
            arrow_array::builder::Int8Builder::new()
        ),
        DataType::Int16 => replicate_primitive!(
            arrow_array::Int16Array,
            arrow_array::builder::Int16Builder::new()
        ),
        DataType::Int32 => replicate_primitive!(
            arrow_array::Int32Array,
            arrow_array::builder::Int32Builder::new()
        ),
        DataType::Int64 => replicate_primitive!(
            arrow_array::Int64Array,
            arrow_array::builder::Int64Builder::new()
        ),
        DataType::UInt8 => replicate_primitive!(
            arrow_array::UInt8Array,
            arrow_array::builder::UInt8Builder::new()
        ),
        DataType::UInt16 => replicate_primitive!(
            arrow_array::UInt16Array,
            arrow_array::builder::UInt16Builder::new()
        ),
        DataType::UInt32 => replicate_primitive!(
            arrow_array::UInt32Array,
            arrow_array::builder::UInt32Builder::new()
        ),
        DataType::UInt64 => replicate_primitive!(
            arrow_array::UInt64Array,
            arrow_array::builder::UInt64Builder::new()
        ),
        DataType::Float32 => replicate_primitive!(
            arrow_array::Float32Array,
            arrow_array::builder::Float32Builder::new()
        ),
        DataType::Float64 => replicate_primitive!(
            arrow_array::Float64Array,
            arrow_array::builder::Float64Builder::new()
        ),
        DataType::Boolean => replicate_primitive!(
            arrow_array::BooleanArray,
            arrow_array::builder::BooleanBuilder::new()
        ),
        DataType::Date32 => replicate_primitive!(
            arrow_array::Date32Array,
            arrow_array::builder::Date32Builder::new()
        ),
        DataType::Date64 => replicate_primitive!(
            arrow_array::Date64Array,
            arrow_array::builder::Date64Builder::new()
        ),
        DataType::Timestamp(unit, tz) => match unit {
            arrow_schema::TimeUnit::Second => replicate_primitive!(
                arrow_array::TimestampSecondArray,
                arrow_array::builder::TimestampSecondBuilder::new().with_timezone_opt(tz.clone())
            ),
            arrow_schema::TimeUnit::Millisecond => replicate_primitive!(
                arrow_array::TimestampMillisecondArray,
                arrow_array::builder::TimestampMillisecondBuilder::new()
                    .with_timezone_opt(tz.clone())
            ),
            arrow_schema::TimeUnit::Microsecond => replicate_primitive!(
                arrow_array::TimestampMicrosecondArray,
                arrow_array::builder::TimestampMicrosecondBuilder::new()
                    .with_timezone_opt(tz.clone())
            ),
            arrow_schema::TimeUnit::Nanosecond => replicate_primitive!(
                arrow_array::TimestampNanosecondArray,
                arrow_array::builder::TimestampNanosecondBuilder::new()
                    .with_timezone_opt(tz.clone())
            ),
        },
        other => Err(EmbeddingError::Arrow(
            arrow_schema::ArrowError::InvalidArgumentError(format!(
                "replicating arrays of type {other:?} in embedding stage is not supported"
            )),
        )),
    }
}

fn new_empty_array(data_type: &DataType) -> Result<ArrayRef, EmbeddingError> {
    Ok(match data_type {
        DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        DataType::LargeUtf8 => {
            Arc::new(arrow_array::LargeStringArray::from(Vec::<&str>::new())) as ArrayRef
        }
        DataType::Int8 => Arc::new(arrow_array::Int8Array::from(Vec::<i8>::new())) as ArrayRef,
        DataType::Int16 => Arc::new(arrow_array::Int16Array::from(Vec::<i16>::new())) as ArrayRef,
        DataType::Int32 => Arc::new(arrow_array::Int32Array::from(Vec::<i32>::new())) as ArrayRef,
        DataType::Int64 => Arc::new(arrow_array::Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        DataType::UInt8 => Arc::new(arrow_array::UInt8Array::from(Vec::<u8>::new())) as ArrayRef,
        DataType::UInt16 => {
            Arc::new(arrow_array::UInt16Array::from(Vec::<u16>::new())) as ArrayRef
        }
        DataType::UInt32 => {
            Arc::new(arrow_array::UInt32Array::from(Vec::<u32>::new())) as ArrayRef
        }
        DataType::UInt64 => {
            Arc::new(arrow_array::UInt64Array::from(Vec::<u64>::new())) as ArrayRef
        }
        DataType::Float32 => {
            Arc::new(arrow_array::Float32Array::from(Vec::<f32>::new())) as ArrayRef
        }
        DataType::Float64 => {
            Arc::new(arrow_array::Float64Array::from(Vec::<f64>::new())) as ArrayRef
        }
        DataType::Boolean => {
            Arc::new(arrow_array::BooleanArray::from(Vec::<bool>::new())) as ArrayRef
        }
        DataType::Date32 => {
            Arc::new(arrow_array::Date32Array::from(Vec::<i32>::new())) as ArrayRef
        }
        DataType::Date64 => {
            Arc::new(arrow_array::Date64Array::from(Vec::<i64>::new())) as ArrayRef
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, tz) => Arc::new(
            arrow_array::TimestampSecondArray::from(Vec::<i64>::new()).with_timezone_opt(tz.clone()),
        ) as ArrayRef,
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz) => Arc::new(
            arrow_array::TimestampMillisecondArray::from(Vec::<i64>::new())
                .with_timezone_opt(tz.clone()),
        ) as ArrayRef,
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => Arc::new(
            arrow_array::TimestampMicrosecondArray::from(Vec::<i64>::new())
                .with_timezone_opt(tz.clone()),
        ) as ArrayRef,
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz) => Arc::new(
            arrow_array::TimestampNanosecondArray::from(Vec::<i64>::new())
                .with_timezone_opt(tz.clone()),
        ) as ArrayRef,
        DataType::FixedSizeList(field, size) => {
            let values = new_empty_array(field.data_type())?;
            Arc::new(
                FixedSizeListArray::try_new(field.clone(), *size, values, None)
                    .map_err(|e| EmbeddingError::Arrow(e))?,
            ) as ArrayRef
        }
        other => {
            return Err(EmbeddingError::Arrow(
                arrow_schema::ArrowError::InvalidArgumentError(format!(
                    "embedding stage cannot build empty array for type {other:?}"
                )),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::TimeUnit;

    #[test]
    fn replicate_preserves_nulls() {
        let arr = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let chunks_per_row: Vec<Vec<String>> = vec![
            vec!["a".to_string()],
            vec!["b".to_string(), "c".to_string()],
            vec!["d".to_string()],
        ];
        let replicated = replicate_array(&arr, &chunks_per_row).unwrap();
        let replicated = replicated
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(replicated.len(), 4);
        assert_eq!(replicated.value(0), 1);
        assert!(replicated.is_null(1));
        assert!(replicated.is_null(2));
        assert_eq!(replicated.value(3), 3);
    }

    #[test]
    fn replicate_supports_timestamps() {
        let arr = Arc::new(
            arrow_array::TimestampMillisecondArray::from(vec![Some(1_000), None, Some(3_000)])
                .with_timezone("UTC".to_string()),
        ) as ArrayRef;
        let chunks_per_row: Vec<Vec<String>> = vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
        ];
        let replicated = replicate_array(&arr, &chunks_per_row).unwrap();
        assert_eq!(replicated.data_type(), arr.data_type());
        let replicated = replicated
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(replicated.value(0), 1_000);
        assert!(replicated.is_null(1));
        assert_eq!(replicated.value(2), 3_000);
    }

    #[test]
    fn new_empty_array_never_panics_for_supported_types() {
        for dtype in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Boolean,
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 4),
        ] {
            let arr = new_empty_array(&dtype).unwrap_or_else(|_| panic!("failed for {dtype:?}"));
            assert_eq!(arr.data_type(), &dtype, "type mismatch for {dtype:?}");
            assert_eq!(arr.len(), 0, "expected empty array for {dtype:?}");
        }
    }

    #[test]
    fn new_empty_array_rejects_unsupported_type_instead_of_panicking() {
        let err = new_empty_array(&DataType::Binary).unwrap_err();
        assert!(err.to_string().contains("Binary"));
    }

    #[cfg(feature = "api")]
    #[tokio::test]
    async fn null_source_text_drops_row_from_output() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"embedding": [0.1, 0.2], "index": 0},
                    {"embedding": [0.3, 0.4], "index": 1}
                ]
            })))
            .mount(&server)
            .await;

        let backend = EmbeddingBackend::Api(crate::embedding::ApiEmbeddingModel::new(
            crate::embedding::ApiEmbeddingConfig {
                base_url: server.uri(),
                model: "test".to_string(),
                api_key_env: None,
            },
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
            ],
        )
        .unwrap();

        let spec = EmbeddingSpec {
            source_column: "text".to_string(),
            output_column: "embedding".to_string(),
            dimension: 2,
            chunking: ChunkingSpec::FixedWindow {
                chunk_size: 100,
                overlap: 0,
            },
            model: EmbeddingModelSpec::Api {
                base_url: server.uri(),
                model: "test".to_string(),
                api_key_env: None,
            },
        };

        let mut backend = backend;
        let out = apply_embedding(&batch, &spec, &mut backend).await.unwrap();

        // Row with NULL text is dropped; the other two rows produce one chunk
        // each.
        assert_eq!(out.num_rows(), 2);
        let id_col = out
            .column(out.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 3);
    }
}
