use crate::config::{PulsarConnectorConfig, PulsarFieldSpec, SubscriptionType};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use nexus_core::{retry_with_backoff, with_timeout, NexusError, RecordBatchBuilder, Source};
use pulsar::executor::TokioExecutor;
use pulsar::message::proto::command_subscribe::SubType;
use pulsar::{Authentication, Consumer, Pulsar};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;

/// Native source for Apache Pulsar. Async-native SDK (`Send`, no ODBC/
/// ADBC handle involved) — same simplicity as `kinesis`'s
/// `stream::unfold`-with-no-`spawn_blocking` shape, not the
/// dedicated-thread workaround `oracle-cdc` needs.
///
/// Unlike `kinesis`/`mssql-cdc`/`oracle-cdc`, this source doesn't track
/// its own resume cursor: Pulsar's broker remembers a subscription's
/// read position across restarts via `ack` — `subscription_name` *is*
/// the durable cursor, a real property of Pulsar, not a v1
/// simplification like the in-memory-only cursors the other streaming
/// connectors in this repo settle for.
pub struct PulsarSource {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    config: PulsarConnectorConfig,
    schema: SchemaRef,
    /// Retry configuration captured at connect time. Currently used for
    /// the initial broker handshake / consumer setup; the streaming
    /// `try_next` loop below relies on Pulsar's own reconnections.
    #[allow(dead_code)]
    retry: nexus_core::RetryConfig,
}

impl PulsarSource {
    pub async fn connect(config: &PulsarConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let schema = build_schema(&config.fields)?;

        let retry = config.retry.clone();
        let cfg = config.clone();
        let pulsar: Pulsar<TokioExecutor> = retry_with_backoff(&retry, "pulsar connect", move || {
            let cfg = cfg.clone();
            async move {
                let mut builder = Pulsar::builder(cfg.service_url.clone(), TokioExecutor);
                if let Some(token) = &cfg.auth_token {
                    builder = builder.with_auth(Authentication {
                        name: "token".to_string(),
                        data: token.clone().into_bytes(),
                    });
                }
                with_timeout(cfg.timeout_seconds, "pulsar connect", async {
                    builder
                        .build()
                        .await
                        .map_err(|e| NexusError::Connector(format!("pulsar connect failed: {e}")))
                })
                .await
            }
        })
        .await?;

        let sub_type = match config.subscription_type {
            SubscriptionType::Exclusive => SubType::Exclusive,
            SubscriptionType::Shared => SubType::Shared,
            SubscriptionType::Failover => SubType::Failover,
            SubscriptionType::KeyShared => SubType::KeyShared,
        };

        let topic = config.topic.clone();
        let subscription_name = config.subscription_name.clone();
        let timeout_seconds = config.timeout_seconds;
        let retry = config.retry.clone();
        let pulsar = pulsar.clone();
        let consumer: Consumer<Vec<u8>, TokioExecutor> =
            retry_with_backoff(&retry, "pulsar consumer build", move || {
                let pulsar = pulsar.clone();
                let topic = topic.clone();
                let subscription_name = subscription_name.clone();
                async move {
                    with_timeout(timeout_seconds, "pulsar consumer build", async {
                        pulsar
                            .consumer()
                            .with_topic(&topic)
                            .with_subscription(&subscription_name)
                            .with_subscription_type(sub_type)
                            .build()
                            .await
                            .map_err(|e| NexusError::Connector(format!("pulsar consumer build failed: {e}")))
                    })
                    .await
                }
            })
            .await?;

        Ok(Self {
            consumer,
            config: config.clone(),
            schema,
            retry,
        })
    }
}

fn build_schema(fields: &[PulsarFieldSpec]) -> Result<SchemaRef, NexusError> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let data_type = match f.r#type.as_str() {
                "int64" => DataType::Int64,
                "float64" => DataType::Float64,
                "boolean" => DataType::Boolean,
                "utf8" => DataType::Utf8,
                other => {
                    return Err(NexusError::Schema(format!(
                        "pulsar field '{}': unsupported type '{other}' (expected int64, float64, boolean, or utf8)",
                        f.name
                    )))
                }
            };
            Ok(Field::new(&f.name, data_type, true))
        })
        .collect::<Result<_, NexusError>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Decodes a raw message payload (assumed JSON) and projects it
/// through `field_names`. A payload that isn't valid JSON, or isn't a
/// JSON object, fails the whole poll cycle loudly rather than being
/// silently skipped.
fn decode_and_project(payload: &[u8], field_names: &[String]) -> Result<Value, NexusError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|e| NexusError::Serialization(format!("pulsar: message isn't valid JSON: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| NexusError::Schema("pulsar: message isn't a JSON object".to_string()))?;

    let mut projected = Map::new();
    for name in field_names {
        if let Some(v) = object.get(name) {
            projected.insert(name.clone(), v.clone());
        }
    }
    Ok(Value::Object(projected))
}

#[async_trait]
impl Source for PulsarSource {
    async fn read_batches(&mut self) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let field_names: Vec<String> = self.config.fields.iter().map(|f| f.name.clone()).collect();
        let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
        let batch_size = self.config.batch_size;
        let schema = self.schema.clone();

        Ok(Box::pin(stream::unfold(self, move |source| {
            let field_names = field_names.clone();
            let schema = schema.clone();
            async move {
                let mut rows: Vec<Value> = Vec::new();
                let mut acked_ok = true;

                loop {
                    match tokio::time::timeout(idle_timeout, source.consumer.try_next()).await {
                        Ok(Ok(Some(msg))) => {
                            let payload: Vec<u8> = msg.deserialize();
                            match decode_and_project(&payload, &field_names) {
                                Ok(row) => rows.push(row),
                                Err(e) => return Some((Err(e), source)),
                            }
                            if let Err(e) = source.consumer.ack(&msg).await {
                                acked_ok = false;
                                if rows.is_empty() {
                                    return Some((
                                        Err(NexusError::Connector(format!("pulsar ack failed: {e}"))),
                                        source,
                                    ));
                                }
                            }
                            if rows.len() >= batch_size || !acked_ok {
                                break;
                            }
                        }
                        Ok(Ok(None)) => {
                            // Consumer closed — end the stream, flushing
                            // whatever was already buffered first.
                            break;
                        }
                        Ok(Err(e)) => {
                            return Some((Err(NexusError::Connector(format!("pulsar receive failed: {e}"))), source))
                        }
                        Err(_elapsed) => {
                            if !rows.is_empty() {
                                break;
                            }
                            // Idle — no message yet, loop and try again.
                        }
                    }
                }

                if rows.is_empty() {
                    return None;
                }
                match RecordBatchBuilder::from_json_rows(schema, &rows) {
                    Ok(batch) => Some((Ok(batch), source)),
                    Err(e) => Some((Err(e), source)),
                }
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
    fn build_schema_maps_supported_types() {
        let fields = vec![
            PulsarFieldSpec { name: "id".into(), r#type: "int64".into() },
            PulsarFieldSpec { name: "amount".into(), r#type: "float64".into() },
            PulsarFieldSpec { name: "active".into(), r#type: "boolean".into() },
            PulsarFieldSpec { name: "name".into(), r#type: "utf8".into() },
        ];
        let schema = build_schema(&fields).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
    }

    #[test]
    fn build_schema_rejects_unknown_type() {
        let fields = vec![PulsarFieldSpec { name: "ts".into(), r#type: "timestamp".into() }];
        let err = build_schema(&fields).unwrap_err();
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn subscription_type_defaults_to_exclusive() {
        assert_eq!(SubscriptionType::default(), SubscriptionType::Exclusive);
    }

    #[test]
    fn decode_and_project_extracts_only_requested_fields() {
        let payload = br#"{"id": 1, "name": "a", "extra": true}"#;
        let fields = vec!["id".to_string(), "name".to_string()];
        let value = decode_and_project(payload, &fields).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("name"));
        assert!(!obj.contains_key("extra"));
    }

    #[test]
    fn decode_and_project_rejects_non_json() {
        let fields = vec!["id".to_string()];
        let err = decode_and_project(b"not json", &fields).unwrap_err();
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn decode_and_project_rejects_non_object_json() {
        let fields = vec!["id".to_string()];
        let err = decode_and_project(b"[1,2,3]", &fields).unwrap_err();
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
