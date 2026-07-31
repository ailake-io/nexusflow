use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use nexus_core::NexusError;

/// Reads `column_name` as primary-key strings — both `Int64` and `Utf8`
/// primary keys are supported (same constraint documented on
/// `nexus-connector-pinecone`/`nexus-connector-ailake`).
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
    fn extract_pk_strings_rejects_unsupported_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Float64Array::from(vec![1.0]))],
        )
        .unwrap();
        let err = extract_pk_strings(&batch, "id").expect_err("float64 pk must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
