use crate::config::{MqttDataType, MqttFieldSpec};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::NexusError;
use serde_json::Value;
use std::sync::Arc;

/// Metadata column carrying the concrete MQTT topic each row arrived on —
/// necessary because `topic_filter` can be a wildcard (`sensors/+/temp`,
/// `sensors/#`) blending many logical sensors into one subscription. Same
/// precedent as CDC's `__opcode` metadata column.
pub const MQTT_TOPIC_COLUMN: &str = "__mqtt_topic";

/// Decodes one message payload (JSON bytes) into a row object. Kept free of
/// `rumqttc` types so it's testable without a broker — see
/// IMPLEMENTATION_PLAN.md Marco 3 (mockable unit tests per connector).
pub fn parse_payload(bytes: &[u8]) -> Result<Value, NexusError> {
    serde_json::from_slice(bytes)
        .map_err(|e| NexusError::Serialization(format!("mqtt payload not JSON: {e}")))
}

/// Builds the output Arrow schema: the user-configured `fields`, plus
/// `__mqtt_topic` appended last (always present, always non-nullable — a
/// message always arrives on some concrete topic).
pub fn build_schema(fields: &[MqttFieldSpec]) -> SchemaRef {
    let mut arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let data_type = match f.data_type {
                MqttDataType::Int64 => DataType::Int64,
                MqttDataType::Float64 => DataType::Float64,
                MqttDataType::Boolean => DataType::Boolean,
                MqttDataType::Utf8 => DataType::Utf8,
            };
            Field::new(&f.name, data_type, f.nullable)
        })
        .collect();
    arrow_fields.push(Field::new(MQTT_TOPIC_COLUMN, DataType::Utf8, false));

    Arc::new(Schema::new(arrow_fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_json_object_payload() {
        let bytes = br#"{"valor": 23.5}"#;
        assert_eq!(parse_payload(bytes).unwrap(), json!({"valor": 23.5}));
    }

    #[test]
    fn non_json_payload_errors() {
        let err = parse_payload(b"not json").expect_err("must fail");
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn builds_arrow_schema_with_topic_column_appended() {
        let fields = vec![MqttFieldSpec {
            name: "valor".into(),
            data_type: MqttDataType::Float64,
            nullable: true,
        }];
        let schema = build_schema(&fields);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "valor");
        assert_eq!(schema.field(1).name(), MQTT_TOPIC_COLUMN);
        assert!(!schema.field(1).is_nullable());
    }
}
