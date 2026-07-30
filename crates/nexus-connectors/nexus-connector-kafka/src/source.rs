use crate::config::KafkaConnectorConfig;
use crate::payload::{build_schema, parse_payload};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use std::time::Duration;

/// Basic Kafka consumer — decodes each message payload as JSON and projects
/// it onto the configured schema. Foundation for Marco 4's Debezium envelope
/// mode (opcode extraction), not implemented here. See ARCHITECTURE.md §4.1.
pub struct KafkaSource {
    consumer: StreamConsumer,
    schema: SchemaRef,
    batch_size: usize,
    poll_timeout: Duration,
    max_messages: usize,
}

impl KafkaSource {
    pub fn connect(config: &KafkaConnectorConfig) -> Result<Self, NexusError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| NexusError::Connector(format!("kafka consumer create failed: {e}")))?;

        consumer
            .subscribe(&[config.topic.as_str()])
            .map_err(|e| NexusError::Connector(format!("kafka subscribe failed: {e}")))?;

        Ok(Self {
            consumer,
            schema: build_schema(&config.fields),
            batch_size: config.batch_size,
            poll_timeout: Duration::from_millis(config.poll_timeout_ms),
            max_messages: config.max_messages,
        })
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let mut message_stream = self.consumer.stream();
        let mut batches = Vec::new();
        let mut buffer = Vec::with_capacity(self.batch_size);
        let mut consumed = 0usize;

        while consumed < self.max_messages {
            let next = tokio::time::timeout(self.poll_timeout, message_stream.next()).await;
            let message = match next {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(e))) => {
                    return Err(NexusError::Connector(format!("kafka poll error: {e}")))
                }
                // Idle cutoff reached or stream closed — topic treated as drained.
                Ok(None) | Err(_) => break,
            };

            let Some(bytes) = message.payload() else {
                continue;
            };
            buffer.push(parse_payload(bytes)?);
            consumed += 1;

            if buffer.len() >= self.batch_size {
                batches.push(RecordBatchBuilder::from_json_rows(
                    self.schema.clone(),
                    &buffer,
                )?);
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            batches.push(RecordBatchBuilder::from_json_rows(
                self.schema.clone(),
                &buffer,
            )?);
        }

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
