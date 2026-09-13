use crate::config::KafkaConnectorConfig;
use crate::rows::batch_to_json_rows;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{CheckpointCursor, NexusError, Sink};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

/// Kafka producer — publishes each row of a batch as its own JSON message,
/// same payload shape `payload.rs::parse_payload` expects on the consumer
/// side, so `KafkaSink` output round-trips through `KafkaSource`. No CDC
/// opcode handling: an arbitrary downstream consumer has no agreed-upon
/// meaning for `__opcode`, same reasoning as `nexus-connector-rest`'s
/// `WebhookSink` — every row goes out verbatim.
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    send_timeout: Duration,
}

impl KafkaSink {
    pub fn connect(config: &KafkaConnectorConfig) -> Result<Self, NexusError> {
        let producer: FutureProducer = config
            .client_config()
            .create()
            .map_err(|e| NexusError::Connector(format!("kafka producer create failed: {e}")))?;
        Ok(Self {
            producer,
            topic: config.topic.clone(),
            // Reuses poll_timeout_ms as the per-message publish budget —
            // there's no separate "producer timeout" field in the shared
            // config, and the two concerns (how long to wait for a message
            // vs. how long to wait for a publish ack) are both "how patient
            // are we with this broker" in practice.
            send_timeout: Duration::from_millis(config.poll_timeout_ms.max(5000)),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = batch_to_json_rows(&batch)?;
        for row in rows {
            let payload = serde_json::to_vec(&row)
                .map_err(|e| NexusError::Serialization(format!("kafka row not JSON: {e}")))?;
            // No `.key()` call, so `K` is unconstrained for the compiler —
            // pin it to `()` (unkeyed message, same as every other
            // partition-agnostic bridging sink in this workspace).
            let record: FutureRecord<'_, (), Vec<u8>> =
                FutureRecord::to(&self.topic).payload(&payload);
            self.producer
                .send(record, self.send_timeout)
                .await
                .map_err(|(e, _)| NexusError::Connector(format!("kafka publish failed: {e}")))?;
        }
        Ok(())
    }

    /// Kafka has no external checkpoint to advance here (unlike a database
    /// sink's upsert commit) — flushing just makes sure every `send` above
    /// has actually round-tripped an ack from the broker before the engine
    /// considers this batch durably written.
    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        self.producer
            .flush(self.send_timeout)
            .map_err(|e| NexusError::Connector(format!("kafka producer flush failed: {e}")))
    }
}
