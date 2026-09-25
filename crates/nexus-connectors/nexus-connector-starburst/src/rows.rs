use crate::client::ColumnMeta;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{NexusError, RecordBatchBuilder};
use serde_json::Value;
use std::sync::Arc;

/// Maps a Trino/Starburst SQL type signature (as returned in the statement
/// protocol's `columns[].type` field, e.g. `"bigint"`, `"varchar(50)"`,
/// `"timestamp(3)"`) to one of the 4 Arrow types
/// `nexus_core::RecordBatchBuilder::from_json_rows` actually supports
/// (Int64/Float64/Boolean/Utf8 — see that function's own match). Anything
/// outside integer/float/boolean falls back to `Utf8`, stringified in
/// `coerce_value` below — same "when in doubt, stringify" rule every other
/// bridging connector in this workspace (mongodb, mysql, csv) already
/// applies to types Arrow's 4-type minimal set can't represent natively
/// (dates, arrays, rows/structs, decimals, ...).
fn trino_type_to_arrow(type_signature: &str) -> DataType {
    let base = type_signature
        .split(['(', ' '])
        .next()
        .unwrap_or(type_signature)
        .to_ascii_lowercase();

    match base.as_str() {
        "bigint" | "integer" | "smallint" | "tinyint" => DataType::Int64,
        "double" | "real" | "decimal" => DataType::Float64,
        "boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

pub fn build_schema(columns: &[ColumnMeta]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, trino_type_to_arrow(&c.type_signature), true))
            .collect::<Vec<_>>(),
    ))
}

/// Coerces one cell to the JSON representation `RecordBatchBuilder::
/// from_json_rows` expects for `target`: it reads `Value::as_i64`/
/// `as_f64`/`as_bool`/`as_str` directly, with no coercion of its own, so a
/// `Utf8` column backed by a JSON number (e.g. a `decimal` cell serialized
/// as `12.5`) must be stringified here first or the builder would silently
/// null it out.
fn coerce_value(value: &Value, target: &DataType) -> Value {
    match (target, value) {
        (_, Value::Null) => Value::Null,
        (DataType::Utf8, Value::String(_)) => value.clone(),
        (DataType::Utf8, other) => Value::String(other.to_string()),
        _ => value.clone(),
    }
}

/// Converts the statement protocol's positional row arrays (`data:
/// [[v1, v2, ...], ...]`) into the keyed JSON objects
/// `RecordBatchBuilder::from_json_rows` requires, coercing each cell to
/// match `schema`'s column type along the way.
fn build_rows(schema: &Schema, columns: &[ColumnMeta], data: &[Vec<Value>]) -> Vec<Value> {
    data.iter()
        .map(|row| {
            let mut object = serde_json::Map::with_capacity(columns.len());
            for (i, column) in columns.iter().enumerate() {
                let field = schema.field(i);
                let cell = row.get(i).unwrap_or(&Value::Null);
                object.insert(column.name.clone(), coerce_value(cell, field.data_type()));
            }
            Value::Object(object)
        })
        .collect()
}

pub fn build_record_batch(
    columns: &[ColumnMeta],
    data: &[Vec<Value>],
) -> Result<RecordBatch, NexusError> {
    let schema = build_schema(columns);
    let rows = build_rows(&schema, columns, data);
    RecordBatchBuilder::from_json_rows(schema, &rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};

    fn cols() -> Vec<ColumnMeta> {
        vec![
            ColumnMeta {
                name: "id".to_string(),
                type_signature: "bigint".to_string(),
            },
            ColumnMeta {
                name: "name".to_string(),
                type_signature: "varchar(50)".to_string(),
            },
            ColumnMeta {
                name: "score".to_string(),
                type_signature: "double".to_string(),
            },
            ColumnMeta {
                name: "active".to_string(),
                type_signature: "boolean".to_string(),
            },
        ]
    }

    #[test]
    fn maps_primitive_trino_types_to_the_4_supported_arrow_types() {
        let schema = build_schema(&cols());
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).data_type(), &DataType::Boolean);
    }

    #[test]
    fn unsupported_types_fall_back_to_utf8() {
        assert_eq!(trino_type_to_arrow("timestamp(3)"), DataType::Utf8);
        assert_eq!(trino_type_to_arrow("array(integer)"), DataType::Utf8);
        assert_eq!(trino_type_to_arrow("row(a integer)"), DataType::Utf8);
        assert_eq!(trino_type_to_arrow("date"), DataType::Utf8);
    }

    #[test]
    fn builds_record_batch_from_positional_rows() {
        let data = vec![
            vec![
                Value::from(1),
                Value::from("alice"),
                Value::from(9.5),
                Value::from(true),
            ],
            vec![
                Value::from(2),
                Value::from("bob"),
                Value::from(3.25),
                Value::from(false),
            ],
        ];
        let batch = build_record_batch(&cols(), &data).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(1), "bob");
        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 9.5);
        let active = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!active.value(1));
    }

    #[test]
    fn stringifies_non_string_values_for_utf8_columns() {
        // `timestamp` falls back to Utf8 (unsupported_types_fall_back_to_utf8
        // above). If the wire ever hands back a non-string cell for such a
        // column, `coerce_value` must stringify it rather than let
        // `RecordBatchBuilder` silently null it out (it only accepts
        // `Value::String` for Utf8 fields).
        let columns = vec![ColumnMeta {
            name: "created_at".to_string(),
            type_signature: "timestamp(3)".to_string(),
        }];
        let data = vec![vec![Value::from(1699999999)]];
        let batch = build_record_batch(&columns, &data).unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "1699999999");
    }

    #[test]
    fn null_cell_stays_null_regardless_of_target_type() {
        let data = vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]];
        let batch = build_record_batch(&cols(), &data).unwrap();
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }
}
