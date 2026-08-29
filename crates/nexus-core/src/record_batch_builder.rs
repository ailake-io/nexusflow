use crate::error::NexusError;
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Generic fallback adapter: turns heterogeneous rows (JSON objects from a REST
/// API, a `bson::Document` converted to JSON, ...) into a `RecordBatch`, for any
/// connector without native ADBC support. See ARCHITECTURE.md §2.
pub struct RecordBatchBuilder;

impl RecordBatchBuilder {
    /// Infers a schema from a sample of JSON object rows — shared fallback
    /// for every bridging connector whose config lets `fields` be left
    /// empty (mongodb/kafka/mqtt/rest; see e.g.
    /// `nexus-connector-csv`'s `schema::infer_schema` for the equivalent
    /// on plain-text CSV, which can't reuse this since it has no JSON
    /// values to inspect). Scans every row (not just the first) so a field
    /// that's `null` in row 1 but a real value in row 5 still gets typed
    /// correctly — the union of keys across the sample becomes the column
    /// set, in first-seen order for determinism.
    ///
    /// Type per field is decided by the first non-null value seen for it:
    /// a JSON integer becomes `Int64`, any other JSON number becomes
    /// `Float64`, `true`/`false` becomes `Boolean`, a string becomes
    /// `Utf8`, and an array/object (or a field that's `null`/absent in
    /// every sampled row) falls back to `Utf8` — same "never lose the
    /// value" principle as `from_json_rows`'s error path, just resolved by
    /// stringifying instead of failing. All inferred fields are nullable:
    /// a sample can't prove a field is always present.
    pub fn infer_schema(rows: &[Value]) -> SchemaRef {
        let mut seen: BTreeMap<String, Option<DataType>> = BTreeMap::new();
        let mut order: Vec<String> = Vec::new();
        for row in rows {
            let Some(obj) = row.as_object() else { continue };
            for (key, value) in obj {
                if !seen.contains_key(key) {
                    order.push(key.clone());
                }
                let slot = seen.entry(key.clone()).or_insert(None);
                if slot.is_none() {
                    *slot = match value {
                        Value::Number(n) if n.is_i64() || n.is_u64() => Some(DataType::Int64),
                        Value::Number(_) => Some(DataType::Float64),
                        Value::Bool(_) => Some(DataType::Boolean),
                        Value::String(_) => Some(DataType::Utf8),
                        _ => None,
                    };
                }
            }
        }
        Arc::new(Schema::new(
            order
                .into_iter()
                .map(|name| {
                    let data_type = seen.get(&name).cloned().flatten().unwrap_or(DataType::Utf8);
                    Field::new(name, data_type, true)
                })
                .collect::<Vec<_>>(),
        ))
    }

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
    fn infers_types_from_first_non_null_value_per_field() {
        let rows = vec![
            json!({"id": 1, "name": "alice", "active": true, "score": 9.5}),
            json!({"id": 2, "name": "bob", "active": false, "score": 3}),
        ];
        let schema = RecordBatchBuilder::infer_schema(&rows);
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("active").unwrap().data_type(),
            &DataType::Boolean
        );
        // score is 9.5 (float) in row 0 — decided there, row 1's whole-number
        // 3 doesn't retroactively narrow it to Int64.
        assert_eq!(
            schema.field_with_name("score").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[test]
    fn infers_from_a_later_row_when_an_earlier_one_has_null() {
        let rows = vec![json!({"tag": null}), json!({"tag": "urgent"})];
        let schema = RecordBatchBuilder::infer_schema(&rows);
        assert_eq!(
            schema.field_with_name("tag").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn field_null_in_every_row_falls_back_to_utf8() {
        let rows = vec![json!({"maybe": null})];
        let schema = RecordBatchBuilder::infer_schema(&rows);
        assert_eq!(
            schema.field_with_name("maybe").unwrap().data_type(),
            &DataType::Utf8
        );
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
