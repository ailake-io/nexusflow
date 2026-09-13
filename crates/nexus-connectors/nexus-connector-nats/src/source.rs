use crate::config::NatsConnectorConfig;
use crate::payload::{build_schema, parse_payload};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_nats::{Client, ConnectOptions, Subscriber};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::time::Duration;

/// Native async source for core NATS pub/sub — `Client`/`Subscriber`
/// are `Send`, no ODBC-style blocking handle, same simplicity as
/// `kinesis`/`pulsar`. No replay/persistence — see
/// `NatsConnectorConfig`'s doc comment for why (core NATS, not
/// JetStream).
pub struct NatsSource {
    subscriber: Subscriber,
    config: NatsConnectorConfig,
    schema: SchemaRef,
}

pub(crate) async fn connect_client(config: &NatsConnectorConfig) -> Result<Client, NexusError> {
    let mut options = ConnectOptions::new();
    if let Some(token) = &config.auth_token {
        options = options.token(token.clone());
    } else if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        options = options.user_and_password(user.clone(), pass.clone());
    }
    with_timeout(config.timeout_seconds, "nats connect", async {
        options
            .connect(&config.server_url)
            .await
            .map_err(|e| NexusError::Connector(format!("nats connect failed: {e}")))
    })
    .await
}

impl NatsSource {
    pub async fn connect(config: &NatsConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        if config.fields.is_empty() {
            return Err(NexusError::Connector(
                "nats source requires at least one field".to_string(),
            ));
        }
        let client = connect_client(config).await?;
        let subscriber = match &config.queue_group {
            Some(group) => client
                .queue_subscribe(config.subject.clone(), group.clone())
                .await
                .map_err(|e| NexusError::Connector(format!("nats queue_subscribe failed: {e}")))?,
            None => client
                .subscribe(config.subject.clone())
                .await
                .map_err(|e| NexusError::Connector(format!("nats subscribe failed: {e}")))?,
        };

        let schema = build_schema(&config.fields);
        Ok(Self {
            subscriber,
            config: config.clone(),
            schema,
        })
    }
}

#[async_trait]
impl Source for NatsSource {
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
                    match tokio::time::timeout(idle_timeout, source.subscriber.next()).await {
                        Ok(Some(msg)) => {
                            let value = match parse_payload(&msg.payload) {
                                Ok(v) => v,
                                Err(e) => return Some((Err(e), source)),
                            };
                            let projected = project_fields(&value, &field_names);
                            rows.push(projected);
                            if rows.len() >= batch_size {
                                break;
                            }
                        }
                        Ok(None) => {
                            // Subscriber closed (connection dropped) — end
                            // the stream, same as Kafka's `Ok(None)` from
                            // its consumer.
                            if rows.is_empty() {
                                return None;
                            }
                            break;
                        }
                        Err(_elapsed) => {
                            // Idle timeout — flush whatever's buffered
                            // rather than blocking forever, same contract
                            // as Kafka's poll_timeout_ms.
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
/// payload — a message can carry extra keys NexusFlow doesn't care
/// about, same behavior `RecordBatchBuilder::from_json_rows` already
/// tolerates for missing/extra keys.
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

    #[test]
    fn missing_field_is_simply_absent() {
        let value = json!({"id": 1});
        let fields = vec!["id".to_string(), "name".to_string()];
        let projected = project_fields(&value, &fields);
        assert_eq!(projected, json!({"id": 1}));
    }
}
