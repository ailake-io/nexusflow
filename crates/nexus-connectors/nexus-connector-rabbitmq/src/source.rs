use crate::config::RabbitmqConnectorConfig;
use crate::payload::{build_schema, parse_payload};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use lapin::options::{BasicConsumeOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, Consumer};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::time::Duration;

/// Native async source for RabbitMQ (AMQP 0-9-1). `Connection` is kept
/// alive alongside `Channel`/`Consumer` — dropping it would close the
/// underlying socket even though `Channel` is a separate handle.
/// Always auto-acks (`no_ack`) — see `RabbitmqConnectorConfig`'s doc
/// comment for why.
pub struct RabbitmqSource {
    _connection: Connection,
    _channel: Channel,
    consumer: Consumer,
    config: RabbitmqConnectorConfig,
    schema: SchemaRef,
}

pub(crate) async fn connect_channel(
    config: &RabbitmqConnectorConfig,
) -> Result<(Connection, Channel), NexusError> {
    let connection = with_timeout(config.timeout_seconds, "rabbitmq connect", async {
        Connection::connect(&config.url, ConnectionProperties::default())
            .await
            .map_err(|e| NexusError::Connector(format!("rabbitmq connect failed: {e}")))
    })
    .await?;
    let channel = connection
        .create_channel()
        .await
        .map_err(|e| NexusError::Connector(format!("rabbitmq create_channel failed: {e}")))?;
    channel
        .queue_declare(
            &config.queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| NexusError::Connector(format!("rabbitmq queue_declare failed: {e}")))?;
    Ok((connection, channel))
}

impl RabbitmqSource {
    pub async fn connect(config: &RabbitmqConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        if config.fields.is_empty() {
            return Err(NexusError::Connector(
                "rabbitmq source requires at least one field".to_string(),
            ));
        }
        let (connection, channel) = connect_channel(config).await?;
        let consumer = channel
            .basic_consume(
                &config.queue,
                "nexus-rabbitmq-source",
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("rabbitmq basic_consume failed: {e}")))?;

        let schema = build_schema(&config.fields);
        Ok(Self {
            _connection: connection,
            _channel: channel,
            consumer,
            config: config.clone(),
            schema,
        })
    }
}

#[async_trait]
impl Source for RabbitmqSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
        let batch_size = self.config.batch_size;
        let field_names: Vec<String> = self.config.fields.iter().map(|f| f.name.clone()).collect();

        Ok(Box::pin(stream::unfold(self, move |source| {
            let field_names = field_names.clone();
            async move {
                let mut rows: Vec<Value> = Vec::with_capacity(batch_size);
                loop {
                    match tokio::time::timeout(idle_timeout, source.consumer.next()).await {
                        Ok(Some(Ok(delivery))) => {
                            let value = match parse_payload(&delivery.data) {
                                Ok(v) => v,
                                Err(e) => return Some((Err(e), source)),
                            };
                            rows.push(project_fields(&value, &field_names));
                            if rows.len() >= batch_size {
                                break;
                            }
                        }
                        Ok(Some(Err(e))) => {
                            return Some((
                                Err(NexusError::Connector(format!(
                                    "rabbitmq delivery error: {e}"
                                ))),
                                source,
                            ))
                        }
                        Ok(None) => {
                            if rows.is_empty() {
                                return None;
                            }
                            break;
                        }
                        Err(_elapsed) => {
                            if rows.is_empty() {
                                continue;
                            }
                            break;
                        }
                    }
                }
                let batch = RecordBatchBuilder::from_json_rows(source.schema.clone(), &rows);
                Some((batch, source))
            }
        })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Projects only the configured field names out of a decoded JSON
/// payload — same tolerance-of-extra-keys behavior as
/// `nexus-connector-nats`'s `project_fields`.
fn project_fields(value: &Value, field_names: &[String]) -> Value {
    let mut object = serde_json::Map::with_capacity(field_names.len());
    for name in field_names {
        if let Some(v) = value.get(name) {
            object.insert(name.clone(), v.clone());
        }
    }
    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projects_only_configured_fields() {
        let value = json!({"id": 1, "name": "alice", "extra": "ignored"});
        let fields = vec!["id".to_string(), "name".to_string()];
        let projected = project_fields(&value, &fields);
        assert_eq!(projected, json!({"id": 1, "name": "alice"}));
    }
}
