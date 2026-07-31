use crate::config::{KafkaDataType, KafkaEnvelope, KafkaFieldSpec};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{NexusError, Opcode, OPCODE_COLUMN};
use serde_json::Value;
use std::sync::Arc;

/// Decodes one message payload (JSON bytes) into a row object, dispatching
/// on the configured envelope. Kept free of `rdkafka` types so it's testable
/// without a broker — see IMPLEMENTATION_PLAN.md Marco 3 (mockable unit
/// tests per connector).
pub fn parse_payload(bytes: &[u8], envelope: KafkaEnvelope) -> Result<Value, NexusError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| NexusError::Serialization(format!("kafka payload not JSON: {e}")))?;

    match envelope {
        KafkaEnvelope::Raw => Ok(value),
        KafkaEnvelope::Debezium => parse_debezium_envelope(value),
    }
}

/// Decodes a Debezium change event into a row carrying the extra
/// `__opcode` column. Handles both the schema-less form
/// (`{"before": ..., "after": ..., "op": "c"}`) and the Connect
/// `JsonConverter` form with `schemas.enable=true`
/// (`{"schema": ..., "payload": {"before": ..., "after": ..., "op": "c"}}`).
fn parse_debezium_envelope(value: Value) -> Result<Value, NexusError> {
    let envelope = match value {
        Value::Object(ref map) if map.contains_key("payload") => {
            map.get("payload").cloned().unwrap_or(Value::Null)
        }
        other => other,
    };

    let op = envelope
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| NexusError::Serialization("debezium envelope missing 'op'".to_string()))?;

    let opcode = match op {
        "c" | "r" => Opcode::Insert,
        "u" => Opcode::Update,
        "d" => Opcode::Delete,
        other => {
            return Err(NexusError::Serialization(format!(
                "unknown debezium op '{other}'"
            )))
        }
    };

    let row_key = if opcode == Opcode::Delete {
        "before"
    } else {
        "after"
    };
    let row = envelope.get(row_key).cloned().unwrap_or(Value::Null);
    let Value::Object(mut row) = row else {
        return Err(NexusError::Serialization(format!(
            "debezium envelope '{row_key}' missing or not an object"
        )));
    };
    row.insert(
        OPCODE_COLUMN.to_string(),
        Value::String(opcode.as_str().to_string()),
    );
    Ok(Value::Object(row))
}

pub fn build_schema(fields: &[KafkaFieldSpec], envelope: KafkaEnvelope) -> SchemaRef {
    let mut arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let data_type = match f.data_type {
                KafkaDataType::Int64 => DataType::Int64,
                KafkaDataType::Float64 => DataType::Float64,
                KafkaDataType::Boolean => DataType::Boolean,
                KafkaDataType::Utf8 => DataType::Utf8,
            };
            Field::new(&f.name, data_type, f.nullable)
        })
        .collect();

    if envelope == KafkaEnvelope::Debezium {
        arrow_fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));
    }

    Arc::new(Schema::new(arrow_fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_json_object_payload() {
        let bytes = br#"{"id": 1, "name": "alice"}"#;
        assert_eq!(
            parse_payload(bytes, KafkaEnvelope::Raw).unwrap(),
            json!({"id": 1, "name": "alice"})
        );
    }

    #[test]
    fn non_json_payload_errors() {
        let err = parse_payload(b"not json", KafkaEnvelope::Raw).expect_err("must fail");
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn builds_arrow_schema_from_field_specs() {
        let fields = vec![
            KafkaFieldSpec {
                name: "id".into(),
                data_type: KafkaDataType::Int64,
                nullable: false,
            },
            KafkaFieldSpec {
                name: "name".into(),
                data_type: KafkaDataType::Utf8,
                nullable: true,
            },
        ];
        let schema = build_schema(&fields, KafkaEnvelope::Raw);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn debezium_schema_appends_opcode_column() {
        let fields = vec![KafkaFieldSpec {
            name: "id".into(),
            data_type: KafkaDataType::Int64,
            nullable: false,
        }];
        let schema = build_schema(&fields, KafkaEnvelope::Debezium);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), OPCODE_COLUMN);
        assert!(!schema.field(1).is_nullable());
    }

    #[test]
    fn debezium_insert_uses_after_and_maps_c_to_insert() {
        let bytes = br#"{"before": null, "after": {"id": 1, "name": "alice"}, "op": "c"}"#;
        let row = parse_payload(bytes, KafkaEnvelope::Debezium).unwrap();
        assert_eq!(row, json!({"id": 1, "name": "alice", "__opcode": "I"}));
    }

    #[test]
    fn debezium_snapshot_read_maps_r_to_insert() {
        let bytes = br#"{"before": null, "after": {"id": 1}, "op": "r"}"#;
        let row = parse_payload(bytes, KafkaEnvelope::Debezium).unwrap();
        assert_eq!(row["__opcode"], "I");
    }

    #[test]
    fn debezium_update_uses_after_and_maps_u_to_update() {
        let bytes =
            br#"{"before": {"id": 1, "name": "alice"}, "after": {"id": 1, "name": "bob"}, "op": "u"}"#;
        let row = parse_payload(bytes, KafkaEnvelope::Debezium).unwrap();
        assert_eq!(row, json!({"id": 1, "name": "bob", "__opcode": "U"}));
    }

    #[test]
    fn debezium_delete_uses_before_since_after_is_null() {
        let bytes = br#"{"before": {"id": 1, "name": "alice"}, "after": null, "op": "d"}"#;
        let row = parse_payload(bytes, KafkaEnvelope::Debezium).unwrap();
        assert_eq!(row, json!({"id": 1, "name": "alice", "__opcode": "D"}));
    }

    #[test]
    fn debezium_unknown_op_errors() {
        let bytes = br#"{"before": null, "after": {"id": 1}, "op": "t"}"#;
        let err = parse_payload(bytes, KafkaEnvelope::Debezium).expect_err("must fail");
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn debezium_missing_op_errors() {
        let bytes = br#"{"before": null, "after": {"id": 1}}"#;
        let err = parse_payload(bytes, KafkaEnvelope::Debezium).expect_err("must fail");
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn debezium_handles_schema_payload_wrapper() {
        let bytes = br#"{"schema": {"type": "struct"}, "payload": {"before": null, "after": {"id": 1}, "op": "c"}}"#;
        let row = parse_payload(bytes, KafkaEnvelope::Debezium).unwrap();
        assert_eq!(row, json!({"id": 1, "__opcode": "I"}));
    }
}
