use crate::error::NexusError;
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, SchemaRef};
use serde_json::Value;
use std::sync::Arc;

/// Generic fallback adapter: turns heterogeneous rows (JSON objects from a REST
/// API, a `bson::Document` converted to JSON, ...) into a `RecordBatch`, for any
/// connector without native ADBC support. See ARCHITECTURE.md §2.
pub struct RecordBatchBuilder;

impl RecordBatchBuilder {
    pub fn from_json_rows(schema: SchemaRef, rows: &[Value]) -> Result<RecordBatch, NexusError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let name = field.name().as_str();
            let column: ArrayRef = match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|row| row.get(name).and_then(Value::as_i64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Float64 => Arc::new(Float64Array::from(
                    rows.iter()
                        .map(|row| row.get(name).and_then(Value::as_f64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    rows.iter()
                        .map(|row| row.get(name).and_then(Value::as_bool))
                        .collect::<Vec<_>>(),
                )),
                DataType::Utf8 => Arc::new(StringArray::from_iter(
                    rows.iter().map(|row| row.get(name).and_then(Value::as_str)),
                )),
                other => {
                    return Err(NexusError::Schema(format!(
                        "unsupported data type for field '{name}': {other:?}"
                    )));
                }
            };
            columns.push(column);
        }

        RecordBatch::try_new(schema, columns).map_err(|e| NexusError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::{Field, Schema};
    use serde_json::json;

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    #[test]
    fn builds_batch_from_heterogeneous_json_rows() {
        let schema = sample_schema();
        let rows = vec![
            json!({"id": 1, "name": "alice", "active": true, "score": 9.5}),
            json!({"id": 2, "name": "bob", "active": false, "score": 3.25}),
        ];

        let batch = RecordBatchBuilder::from_json_rows(schema, &rows).expect("batch builds");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn missing_field_becomes_null_not_error() {
        let schema = sample_schema();
        let rows = vec![json!({"id": 1})];

        let batch = RecordBatchBuilder::from_json_rows(schema, &rows).expect("batch builds");

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(name_col.is_null(0));
    }

    #[test]
    fn unsupported_data_type_errors() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        )]));

        let err = RecordBatchBuilder::from_json_rows(schema, &[json!({"created_at": 0})])
            .expect_err("unsupported type must error");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
