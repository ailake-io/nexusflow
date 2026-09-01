use crate::config::{RabbitmqDataType, RabbitmqFieldSpec};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::NexusError;
use serde_json::Value;
use std::sync::Arc;

/// Decodes one message payload (JSON bytes) into a row object. Kept
/// free of `lapin` types so it's testable without a broker — same
/// rule `nexus-connector-kafka`'s `payload.rs` documents.
pub fn parse_payload(bytes: &[u8]) -> Result<Value, NexusError> {
    serde_json::from_slice(bytes)
        .map_err(|e| NexusError::Serialization(format!("rabbitmq payload not JSON: {e}")))
}

pub fn build_schema(fields: &[RabbitmqFieldSpec]) -> SchemaRef {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let data_type = match f.data_type {
                RabbitmqDataType::Int64 => DataType::Int64,
                RabbitmqDataType::Float64 => DataType::Float64,
                RabbitmqDataType::Boolean => DataType::Boolean,
                RabbitmqDataType::Utf8 => DataType::Utf8,
            };
            Field::new(&f.name, data_type, f.nullable)
        })
        .collect();

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
            parse_payload(bytes).unwrap(),
            json!({"id": 1, "name": "alice"})
        );
    }

    #[test]
    fn non_json_payload_errors() {
        let err = parse_payload(b"not json").expect_err("must fail");
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn builds_arrow_schema_from_field_specs() {
        let fields = vec![
            RabbitmqFieldSpec {
                name: "id".into(),
                data_type: RabbitmqDataType::Int64,
                nullable: false,
            },
            RabbitmqFieldSpec {
                name: "name".into(),
                data_type: RabbitmqDataType::Utf8,
                nullable: true,
            },
        ];
        let schema = build_schema(&fields);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }
}
