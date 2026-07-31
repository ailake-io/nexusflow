use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use nexus_core::NexusError;
use std::sync::Arc;

/// Drops `column_name` from `batch` — `ailake_query::writer::TableWriter`
/// appends its own vector column (from a separate `embeddings` argument)
/// during `write_batch`, so the tabular `RecordBatch` handed to it must not
/// already carry one under the same name or the written file would end up
/// with a duplicate field.
pub fn drop_column(batch: &RecordBatch, column_name: &str) -> Result<RecordBatch, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, f)| (**f).clone())
        .collect();
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, c)| c.clone())
        .collect();
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map_err(|e| NexusError::Schema(format!("failed to drop column '{column_name}': {e}")))
}

/// Reads `column_name` (a `FixedSizeList<Float32>`) as one `Vec<f32>` per row
/// — the shape `nexus-ai::embedding` appends to a batch.
pub fn extract_embeddings(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<Vec<f32>>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let list = batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            NexusError::Schema(format!("column '{column_name}' is not a FixedSizeList"))
        })?;

    let mut embeddings = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        let values = list.value(row);
        let floats = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                NexusError::Schema(format!("column '{column_name}' items are not Float32"))
            })?;
        embeddings.push(floats.values().to_vec());
    }
    Ok(embeddings)
}

/// Reads `column_name` as primary-key strings for `ailake_query::delete_where`
/// — AI-Lake equality deletes are matched by string value regardless of the
/// underlying Iceberg type, so both `Int64` and `Utf8` primary keys are
/// supported (same constraint documented on `nexus-connector-pinecone`).
pub fn extract_pk_strings(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<String>, NexusError> {
    let idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| NexusError::Schema(format!("column '{column_name}' not found")))?;
    let column = batch.column(idx);

    if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i).to_string()).collect());
    }
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok((0..arr.len()).map(|i| arr.value(i).to_string()).collect());
    }
    Err(NexusError::Schema(format!(
        "primary key column '{column_name}' must be Int64 or Utf8"
    )))
}

/// Appends `embeddings` (one vector per row of `batch`, all `dimension`
/// wide) as a `FixedSizeList<Float32>` column named `column_name` — used to
/// reconstruct the full row after `AilakeFileReader::read_parquet` returns
/// the tabular data and the decoded vector separately.
pub fn append_embedding_column(
    batch: &RecordBatch,
    embeddings: &[Vec<f32>],
    dimension: u32,
    column_name: &str,
) -> Result<RecordBatch, NexusError> {
    if embeddings.len() != batch.num_rows() {
        return Err(NexusError::Schema(format!(
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
        FixedSizeListArray::try_new(field, dimension as i32, Arc::new(flat), None)
            .map_err(|e| NexusError::Schema(format!("failed to build embedding column: {e}")))?;

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

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(embedding_array));

    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map_err(|e| NexusError::Schema(format!("failed to append embedding column: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;

    #[test]
    fn extract_embeddings_reads_fixed_size_list() {
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let list = FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            list.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();

        let embeddings = extract_embeddings(&batch, "embedding").unwrap();
        assert_eq!(embeddings, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    }

    #[test]
    fn drop_column_removes_named_field_only() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("embedding", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float32Array::from(vec![0.1, 0.2])),
            ],
        )
        .unwrap();

        let out = drop_column(&batch, "embedding").unwrap();
        assert_eq!(out.num_columns(), 1);
        assert!(out.schema().field_with_name("id").is_ok());
        assert!(out.schema().field_with_name("embedding").is_err());
    }

    #[test]
    fn extract_pk_strings_supports_int64_and_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        assert_eq!(
            extract_pk_strings(&batch, "id").unwrap(),
            vec!["1".to_string(), "2".to_string()]
        );

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
                .unwrap();
        assert_eq!(
            extract_pk_strings(&batch, "id").unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn append_embedding_column_roundtrips() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let embeddings = vec![vec![0.1, 0.2], vec![0.3, 0.4]];
        let out = append_embedding_column(&batch, &embeddings, 2, "embedding").unwrap();
        let back = extract_embeddings(&out, "embedding").unwrap();
        assert_eq!(back, embeddings);
    }
}
