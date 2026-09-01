use crate::config::{RedisConnectorConfig, RedisFieldSpec, RedisStartingPosition};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};
use serde_json::Value;

/// Reads a Redis Stream via `XREAD BLOCK` — no consumer group
/// (`XREADGROUP`/`XACK`), see `RedisConnectorConfig::starting_position`'s
/// doc comment for why. `MultiplexedConnection` is `Send`/`Clone`
/// (async-native, no ODBC-style blocking handle), so no
/// `spawn_blocking` needed — same simplicity as `kinesis`/`pulsar`.
pub struct RedisSource {
    config: RedisConnectorConfig,
    connection: MultiplexedConnection,
    last_id: String,
    schema: SchemaRef,
}

fn build_schema(fields: &[RedisFieldSpec]) -> Result<SchemaRef, NexusError> {
    let arrow_fields = fields
        .iter()
        .map(|f| {
            let data_type = match f.r#type.as_str() {
                "int64" => DataType::Int64,
                "float64" => DataType::Float64,
                "boolean" => DataType::Boolean,
                "utf8" => DataType::Utf8,
                other => {
                    return Err(NexusError::Schema(format!(
                        "redis: unsupported field type {other} for field {}",
                        f.name
                    )))
                }
            };
            Ok(Field::new(&f.name, data_type, true))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;
    Ok(std::sync::Arc::new(Schema::new(arrow_fields)))
}

impl RedisSource {
    pub async fn connect(config: &RedisConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        if config.fields.is_empty() {
            return Err(NexusError::Connector(
                "redis source requires at least one field".to_string(),
            ));
        }
        let schema = build_schema(&config.fields)?;
        let client = Client::open(config.url.as_str())
            .map_err(|e| NexusError::Connector(format!("redis client: {e}")))?;
        let connection = with_timeout(config.timeout_seconds, "redis connect", async {
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| NexusError::Connector(format!("redis connect: {e}")))
        })
        .await?;

        let last_id = match config.starting_position {
            RedisStartingPosition::Latest => "$".to_string(),
            RedisStartingPosition::Earliest => "0".to_string(),
        };

        Ok(Self {
            config: config.clone(),
            connection,
            last_id,
            schema,
        })
    }
}

/// Coerces a raw Redis Stream field value (always bytes/string on the
/// wire) into the JSON type `RedisFieldSpec::type` declares — same
/// role `payload.rs`'s JSON decode plays for Kafka/MQTT, except
/// there's no JSON to parse here, just a string to coerce.
fn coerce_field(raw: &redis::Value, field_type: &str) -> Result<Value, NexusError> {
    let text: String = redis::from_redis_value(raw)
        .map_err(|e| NexusError::Connector(format!("redis: field value not a string: {e}")))?;
    Ok(match field_type {
        "int64" => text
            .parse::<i64>()
            .map(Value::from)
            .map_err(|e| NexusError::Connector(format!("redis: field not an int64: {e}")))?,
        "float64" => text
            .parse::<f64>()
            .map(Value::from)
            .map_err(|e| NexusError::Connector(format!("redis: field not a float64: {e}")))?,
        "boolean" => text
            .parse::<bool>()
            .map(Value::from)
            .map_err(|e| NexusError::Connector(format!("redis: field not a boolean: {e}")))?,
        _ => Value::String(text),
    })
}

#[async_trait]
impl Source for RedisSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        Ok(Box::pin(stream::unfold(self, move |source| async move {
            // `XREAD BLOCK` waiting for `idle_timeout_ms` and returning
            // empty means "no new entry yet" (same contract MQTT/Kafka's
            // idle timeout documents), not an error or end-of-stream —
            // loop until there's at least one row to hand back, rather
            // than emitting spurious empty batches downstream.
            loop {
                let options = StreamReadOptions::default()
                    .count(source.config.batch_size)
                    .block(source.config.idle_timeout_ms as usize);
                let reply: Result<StreamReadReply, _> = source
                    .connection
                    .xread_options(&[&source.config.stream_key], &[&source.last_id], &options)
                    .await;

                let reply = match reply {
                    Ok(r) => r,
                    Err(e) => {
                        return Some((
                            Err(NexusError::Connector(format!("redis xread failed: {e}"))),
                            source,
                        ))
                    }
                };

                let mut rows: Vec<Value> = Vec::new();
                for stream_key in &reply.keys {
                    for entry in &stream_key.ids {
                        let mut object = serde_json::Map::new();
                        let mut row_err = None;
                        for field in &source.config.fields {
                            let raw = entry.map.get(&field.name).ok_or_else(|| {
                                NexusError::Connector(format!(
                                    "redis: entry missing field {}",
                                    field.name
                                ))
                            });
                            match raw.and_then(|v| coerce_field(v, &field.r#type)) {
                                Ok(v) => {
                                    object.insert(field.name.clone(), v);
                                }
                                Err(e) => {
                                    row_err = Some(e);
                                    break;
                                }
                            }
                        }
                        if let Some(e) = row_err {
                            return Some((Err(e), source));
                        }
                        rows.push(Value::Object(object));
                        source.last_id = entry.id.clone();
                    }
                }

                if rows.is_empty() {
                    continue;
                }
                let batch = RecordBatchBuilder::from_json_rows(source.schema.clone(), &rows);
                return Some((batch, source));
            }
        })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_schema_from_field_specs() {
        let fields = vec![
            RedisFieldSpec {
                name: "id".into(),
                r#type: "int64".into(),
            },
            RedisFieldSpec {
                name: "name".into(),
                r#type: "utf8".into(),
            },
        ];
        let schema = build_schema(&fields).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn rejects_unknown_field_type() {
        let fields = vec![RedisFieldSpec {
            name: "id".into(),
            r#type: "not-a-type".into(),
        }];
        assert!(build_schema(&fields).is_err());
    }

    #[test]
    fn coerces_int64_field() {
        let raw = redis::Value::BulkString(b"42".to_vec());
        let value = coerce_field(&raw, "int64").unwrap();
        assert_eq!(value, Value::from(42));
    }

    #[test]
    fn coerces_boolean_field() {
        let raw = redis::Value::BulkString(b"true".to_vec());
        let value = coerce_field(&raw, "boolean").unwrap();
        assert_eq!(value, Value::from(true));
    }

    #[test]
    fn coerces_utf8_field_as_is() {
        let raw = redis::Value::BulkString(b"hello".to_vec());
        let value = coerce_field(&raw, "utf8").unwrap();
        assert_eq!(value, Value::from("hello"));
    }

    #[test]
    fn rejects_non_numeric_int64_field() {
        let raw = redis::Value::BulkString(b"not-a-number".to_vec());
        assert!(coerce_field(&raw, "int64").is_err());
    }
}
