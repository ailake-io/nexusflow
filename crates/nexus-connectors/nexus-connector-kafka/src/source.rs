use crate::config::KafkaConnectorConfig;
use crate::payload::{build_schema, parse_payload};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::Offset;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Kafka consumer — decodes each message payload as JSON and projects it
/// onto the configured schema. See ARCHITECTURE.md §4.1.
pub struct KafkaSource {
    consumer: StreamConsumer,
    topic: String,
    schema: SchemaRef,
    batch_size: usize,
    poll_timeout: Duration,
    max_messages: usize,
    /// `true` when the consumer was manually assigned via `start_offsets`
    /// instead of subscribed as a group member. In that mode Kafka does not
    /// consider us a group member, so broker-side offset commits are not
    /// possible (and not needed — the engine checkpoint is the source of
    /// truth). See ARCHITECTURE.md §5.
    manual_assignment: bool,
    /// Last consumed offset per partition — read after `read_batches` to
    /// build a `CheckpointCursor` per partition (resume via
    /// `start_offsets`), per ARCHITECTURE.md §5.
    last_offsets: HashMap<i32, i64>,
}

impl KafkaSource {
    pub async fn connect(config: &KafkaConnectorConfig) -> Result<Self, NexusError> {
        let schema = if config.fields.is_empty() {
            infer_schema(config).await?
        } else {
            build_schema(&config.fields)
        };

        // Manual offset commit: the engine's checkpoint is the source of
        // truth, not a background heartbeat. `read_batches` commits once
        // the returned batches have been successfully produced, matching
        // the at-least-once contract of the other sources.
        let consumer: StreamConsumer = config
            .client_config()
            .create()
            .map_err(|e| NexusError::Connector(format!("kafka consumer create failed: {e}")))?;

        if config.start_offsets.is_empty() {
            consumer
                .subscribe(&[config.topic.as_str()])
                .map_err(|e| NexusError::Connector(format!("kafka subscribe failed: {e}")))?;
        } else {
            // Explicit resume: assign exactly the partitions given, at their
            // checkpointed offset — mirrors the "only unfinished partitions
            // get a handle" pattern in nexus-server's runner.rs.
            let mut tpl = TopicPartitionList::new();
            for (&partition, &offset) in &config.start_offsets {
                tpl.add_partition_offset(&config.topic, partition, Offset::Offset(offset))
                    .map_err(|e| {
                        NexusError::Connector(format!("kafka seek offset invalid: {e}"))
                    })?;
            }
            consumer
                .assign(&tpl)
                .map_err(|e| NexusError::Connector(format!("kafka assign failed: {e}")))?;
        }

        Ok(Self {
            consumer,
            topic: config.topic.clone(),
            schema,
            batch_size: config.batch_size,
            poll_timeout: Duration::from_millis(config.poll_timeout_ms),
            max_messages: config.max_messages,
            manual_assignment: !config.start_offsets.is_empty(),
            last_offsets: HashMap::new(),
        })
    }

    /// Last consumed offset per partition, for the caller to persist as a
    /// `CheckpointCursor` per `(pipeline_id, "{topic}-{partition}")`.
    pub fn last_offsets(&self) -> &HashMap<i32, i64> {
        &self.last_offsets
    }

    /// Commits the last consumed offsets synchronously. Called automatically
    /// at the end of a successful `read_batches` call so the engine's
    /// checkpoint and Kafka's consumer group state stay aligned.
    fn commit_offsets(&self) -> Result<(), NexusError> {
        if self.last_offsets.is_empty() || self.manual_assignment {
            return Ok(());
        }
        let mut tpl = TopicPartitionList::new();
        for (&partition, &offset) in &self.last_offsets {
            tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset))
                .map_err(|e| {
                    NexusError::Connector(format!("kafka commit offset list failed: {e}"))
                })?;
        }
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| NexusError::Connector(format!("kafka offset commit failed: {e}")))?;
        Ok(())
    }
}

/// Samples up to `config.schema_sample_rows` messages and infers a schema
/// — source-side fallback for when `fields` is left empty (see
/// `KafkaConnectorConfig::fields` doc comment). Uses its own throwaway
/// consumer group (`{group_id}-schema-sample-{pid}`, unique enough to
/// never collide with a real run) instead of `config.group_id` — this
/// consumer never commits (dropped at the end of the function), so if it
/// *did* share the real group, a rebalance mid-sample could steal
/// partitions from — or hand stale state to — the actual read that
/// follows in `connect`. A dedicated group sidesteps that entirely rather
/// than trying to coordinate the two.
async fn infer_schema(config: &KafkaConnectorConfig) -> Result<SchemaRef, NexusError> {
    let mut sample_config = config.client_config();
    sample_config.set(
        "group.id",
        format!("{}-schema-sample-{}", config.group_id, std::process::id()),
    );
    let consumer: StreamConsumer = sample_config.create().map_err(|e| {
        NexusError::Connector(format!("kafka schema-sample consumer create failed: {e}"))
    })?;
    consumer
        .subscribe(&[config.topic.as_str()])
        .map_err(|e| NexusError::Connector(format!("kafka schema-sample subscribe failed: {e}")))?;

    let poll_timeout = Duration::from_millis(config.poll_timeout_ms);
    let mut message_stream = consumer.stream();
    let mut rows = Vec::new();
    while rows.len() < config.schema_sample_rows {
        match tokio::time::timeout(poll_timeout, message_stream.next()).await {
            Ok(Some(Ok(message))) => {
                if let Some(bytes) = message.payload() {
                    if let Ok(row) = parse_payload(bytes) {
                        rows.push(row);
                    }
                }
            }
            // Idle cutoff, stream closed, or a poll error — treat the
            // sample as complete with whatever was collected so far
            // rather than failing the whole connect over it.
            _ => break,
        }
    }
    Ok(RecordBatchBuilder::infer_schema(&rows))
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
        // A topic is often created lazily by its producer on first message —
        // a consumer that subscribes first sees UnknownTopicOrPartition until
        // then. Tolerate it for up to the same idle-cutoff budget as "no
        // message arrived", instead of hard-failing the whole read.
        let mut waiting_for_topic_since: Option<Instant> = None;

        while consumed < self.max_messages {
            let next = tokio::time::timeout(self.poll_timeout, message_stream.next()).await;
            let message = match next {
                Ok(Some(Ok(message))) => {
                    waiting_for_topic_since = None;
                    message
                }
                Ok(Some(Err(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::UnknownTopicOrPartition,
                )))) => {
                    let since = *waiting_for_topic_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= self.poll_timeout {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
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
            self.last_offsets
                .insert(message.partition(), message.offset());
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

        // Manual commit: only advance Kafka offsets after the batches that
        // contain them have been successfully assembled. If downstream
        // processing fails, the next run resumes from the previous checkpoint
        // rather than from an auto-committed offset that may skip data.
        self.commit_offsets()?;

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
