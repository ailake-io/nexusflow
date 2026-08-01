use crate::checkpoint::CheckpointCursor;
use crate::error::NexusError;
use crate::traits::{Sink, Source, Transform};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// One partition's Source+Sink pair, ready to run. Partitioning is the unit
/// of parallelism — see ARCHITECTURE.md §4.
pub struct PartitionHandle {
    pub partition_id: String,
    pub source: Box<dyn Source>,
    pub sink: Box<dyn Sink>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PartitionStats {
    pub partition_id: String,
    pub batches_written: usize,
    pub rows_written: usize,
}

/// Cumulative progress for one partition/sink, emitted after every batch —
/// nexus-server relays these over a WebSocket per run (Marco 7 task #9).
/// Cumulative rather than per-batch deltas so a client that misses one
/// event (a lagged broadcast receiver) is still consistent on the next.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub partition_id: String,
    pub batches_written: usize,
    pub rows_written: usize,
    pub bytes_written: usize,
}

/// Broadcast so every connected WebSocket client for a run gets every
/// event, not just one of them (see nexus-server's `ProgressHub`). `None`
/// callers (e.g. nexus-core's own tests) pay no cost beyond an `if let`.
pub type ProgressSender = tokio::sync::broadcast::Sender<ProgressEvent>;

/// Runs partitions Source -> mpsc channel -> Sink.
///
/// Checkpoint granularity is per-partition, not per-batch: a checkpoint is
/// committed once, after the whole partition drains successfully. This is
/// deliberate, not a shortcut — every partition's Source query is a bounded,
/// deterministic range (e.g. a PK range) and every Sink write is an upsert
/// (ARCHITECTURE.md §5), so re-running an incomplete partition from scratch
/// after a crash is always safe. Finer-grained mid-partition resumption would
/// add complexity without a correctness payoff at this scale.
pub struct PipelineEngine {
    channel_capacity: usize,
}

impl PipelineEngine {
    pub fn new(channel_capacity: usize) -> Self {
        Self { channel_capacity }
    }

    #[tracing::instrument(skip(self, handle, progress), fields(partition_id = %handle.partition_id))]
    pub async fn run_partition(
        &self,
        handle: PartitionHandle,
        progress: Option<ProgressSender>,
    ) -> Result<PartitionStats, NexusError> {
        let PartitionHandle {
            partition_id,
            mut source,
            mut sink,
        } = handle;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(self.channel_capacity);

        let reader_partition_id = partition_id.clone();
        let reader = tokio::spawn(async move {
            let mut stream = source.read_batches().await?;
            while let Some(item) = stream.next().await {
                let batch = item?;
                if tx.send(batch).await.is_err() {
                    return Err(NexusError::Connector(format!(
                        "partition {reader_partition_id}: writer side of channel closed early"
                    )));
                }
            }
            Ok::<(), NexusError>(())
        });

        let writer_partition_id = partition_id.clone();
        let writer = tokio::spawn(async move {
            let mut batches_written = 0usize;
            let mut rows_written = 0usize;
            let mut bytes_written = 0usize;

            while let Some(batch) = rx.recv().await {
                rows_written += batch.num_rows();
                batches_written += 1;
                bytes_written += batch.get_array_memory_size();
                sink.write_batch(batch).await?;
                if let Some(tx) = &progress {
                    let _ = tx.send(ProgressEvent {
                        partition_id: writer_partition_id.clone(),
                        batches_written,
                        rows_written,
                        bytes_written,
                    });
                }
            }

            sink.commit_checkpoint(CheckpointCursor::new(writer_partition_id))
                .await?;

            Ok::<(usize, usize), NexusError>((batches_written, rows_written))
        });

        let (reader_result, writer_result) = tokio::join!(reader, writer);

        reader_result
            .map_err(|e| NexusError::Connector(format!("reader task panicked: {e}")))??;
        let (batches_written, rows_written) = writer_result
            .map_err(|e| NexusError::Connector(format!("writer task panicked: {e}")))??;

        Ok(PartitionStats {
            partition_id,
            batches_written,
            rows_written,
        })
    }

    /// Runs every partition concurrently and returns each partition's stats
    /// in completion order. A failed partition does not cancel the others —
    /// callers see a `NexusError` for that partition's slot and can retry it
    /// independently (checkpoint is per-partition, so retrying one partition
    /// never touches the others' already-committed state).
    #[tracing::instrument(skip_all, fields(num_partitions = partitions.len()))]
    pub async fn run(
        &self,
        partitions: Vec<PartitionHandle>,
        progress: Option<ProgressSender>,
    ) -> Vec<Result<PartitionStats, NexusError>> {
        let mut set = tokio::task::JoinSet::new();
        for partition in partitions {
            // Reuse run_partition's reader/writer split for each partition,
            // one JoinSet entry per partition so partitions run concurrently.
            let capacity = self.channel_capacity;
            let progress = progress.clone();
            set.spawn(async move {
                PipelineEngine::new(capacity)
                    .run_partition(partition, progress)
                    .await
            });
        }

        let mut results = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(NexusError::Connector(format!(
                    "partition task panicked: {e}"
                )))),
            }
        }
        results
    }

    /// Drains every named source fully (Marco 2's fan-in step) — no
    /// PK-range partitioning here: combining arbitrary joined/aggregated
    /// sources doesn't have a well-defined per-partition correspondence, so
    /// the whole table is read. Split out from `run_transform_pipeline` so
    /// callers can inspect the transform's *output* schema (only known after
    /// running it) before constructing sinks that need to know their column
    /// list upfront — see `nexus-server`'s runner.
    pub async fn drain_sources(
        sources: Vec<(String, Box<dyn Source>)>,
    ) -> Result<Vec<(String, SchemaRef, Vec<RecordBatch>)>, NexusError> {
        let mut inputs = Vec::with_capacity(sources.len());
        for (name, mut source) in sources {
            let schema = source.schema();
            let mut stream = source.read_batches().await?;
            let mut batches = Vec::new();
            while let Some(item) = stream.next().await {
                batches.push(item?);
            }
            drop(stream);
            inputs.push((name, schema, batches));
        }
        Ok(inputs)
    }

    /// Broadcast fan-out: every sink gets the full `batches` output, then
    /// commits one checkpoint. A failed sink doesn't stop the others (same
    /// per-slot-error contract as `run`).
    #[tracing::instrument(skip_all, fields(num_sinks = sinks.len(), num_batches = batches.len()))]
    pub async fn fan_out_write(
        &self,
        batches: &[RecordBatch],
        sinks: Vec<(String, Box<dyn Sink>)>,
        progress: Option<ProgressSender>,
    ) -> Vec<Result<PartitionStats, NexusError>> {
        let rows_in_output: usize = batches.iter().map(|b| b.num_rows()).sum();

        let mut results = Vec::with_capacity(sinks.len());
        for (name, mut sink) in sinks {
            let outcome: Result<PartitionStats, NexusError> = async {
                let mut rows_written = 0usize;
                let mut bytes_written = 0usize;
                for (i, batch) in batches.iter().enumerate() {
                    sink.write_batch(batch.clone()).await?;
                    rows_written += batch.num_rows();
                    bytes_written += batch.get_array_memory_size();
                    if let Some(tx) = &progress {
                        let _ = tx.send(ProgressEvent {
                            partition_id: name.clone(),
                            batches_written: i + 1,
                            rows_written,
                            bytes_written,
                        });
                    }
                }
                sink.commit_checkpoint(CheckpointCursor::new(name.clone()))
                    .await?;
                Ok(PartitionStats {
                    partition_id: name.clone(),
                    batches_written: batches.len(),
                    rows_written: rows_in_output,
                })
            }
            .await;
            results.push(outcome);
        }
        results
    }

    /// Runs a fan-in/fan-out transform pipeline end to end (Marco 2): drains
    /// every source, runs the transform once, writes the output to every
    /// sink. Convenience wrapper over `drain_sources`/`fan_out_write` for
    /// callers whose sinks don't need the transform's output schema to be
    /// constructed (e.g. tests, or fixed-schema sinks).
    #[tracing::instrument(skip_all)]
    pub async fn run_transform_pipeline(
        &self,
        pipeline: TransformPipeline,
        progress: Option<ProgressSender>,
    ) -> Result<Vec<PartitionStats>, NexusError> {
        let inputs = Self::drain_sources(pipeline.sources).await?;
        let output = pipeline.transform.apply(inputs).await?;
        self.fan_out_write(&output, pipeline.sinks, progress)
            .await
            .into_iter()
            .collect()
    }
}

/// N named sources (fan-in) -> 1 transform -> M named sinks (fan-out).
pub struct TransformPipeline {
    pub sources: Vec<(String, Box<dyn Source>)>,
    pub transform: Box<dyn Transform>,
    pub sinks: Vec<(String, Box<dyn Sink>)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use std::sync::{Arc, Mutex};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(arrow_array::Int64Array::from(ids))],
        )
        .unwrap()
    }

    struct VecSource {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    #[async_trait]
    impl Source for VecSource {
        async fn read_batches(
            &mut self,
        ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
            Ok(Box::pin(stream::iter(
                self.batches.clone().into_iter().map(Ok),
            )))
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        received: Arc<Mutex<Vec<RecordBatch>>>,
        checkpoints: Arc<Mutex<Vec<CheckpointCursor>>>,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
            self.received.lock().unwrap().push(batch);
            Ok(())
        }

        async fn commit_checkpoint(&mut self, cursor: CheckpointCursor) -> Result<(), NexusError> {
            self.checkpoints.lock().unwrap().push(cursor);
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_partition_moves_all_rows_and_commits_one_checkpoint() {
        let source = VecSource {
            schema: test_schema(),
            batches: vec![test_batch(vec![1, 2, 3]), test_batch(vec![4, 5])],
        };
        let received = Arc::new(Mutex::new(Vec::new()));
        let checkpoints = Arc::new(Mutex::new(Vec::new()));
        let sink = RecordingSink {
            received: received.clone(),
            checkpoints: checkpoints.clone(),
        };

        let engine = PipelineEngine::new(8);
        let stats = engine
            .run_partition(
                PartitionHandle {
                    partition_id: "p0".to_string(),
                    source: Box::new(source),
                    sink: Box::new(sink),
                },
                None,
            )
            .await
            .expect("partition runs successfully");

        assert_eq!(stats.partition_id, "p0");
        assert_eq!(stats.batches_written, 2);
        assert_eq!(stats.rows_written, 5);
        assert_eq!(received.lock().unwrap().len(), 2);
        assert_eq!(
            checkpoints.lock().unwrap().len(),
            1,
            "checkpoint committed once per partition, not per batch"
        );
    }

    #[tokio::test]
    async fn run_executes_partitions_concurrently_and_collects_all_results() {
        let make_partition = |id: &str, rows: Vec<i64>| PartitionHandle {
            partition_id: id.to_string(),
            source: Box::new(VecSource {
                schema: test_schema(),
                batches: vec![test_batch(rows)],
            }),
            sink: Box::new(RecordingSink::default()),
        };

        let engine = PipelineEngine::new(8);
        let results = engine
            .run(
                vec![
                    make_partition("p0", vec![1, 2]),
                    make_partition("p1", vec![3, 4, 5]),
                ],
                None,
            )
            .await;

        assert_eq!(results.len(), 2);
        let total_rows: usize = results
            .into_iter()
            .map(|r| r.expect("partition succeeds").rows_written)
            .sum();
        assert_eq!(total_rows, 5);
    }

    /// Concatenates whatever it's given — enough to exercise fan-in/fan-out
    /// wiring without pulling DataFusion into nexus-core's own test suite
    /// (that's covered by `transform::tests` against `DataFusionTransform`).
    struct ConcatTransform;

    #[async_trait]
    impl Transform for ConcatTransform {
        async fn apply(
            &self,
            inputs: Vec<(String, SchemaRef, Vec<RecordBatch>)>,
        ) -> Result<Vec<RecordBatch>, NexusError> {
            Ok(inputs
                .into_iter()
                .flat_map(|(_, _, batches)| batches)
                .collect())
        }
    }

    #[tokio::test]
    async fn run_transform_pipeline_fans_in_two_sources_and_fans_out_to_two_sinks() {
        let source_a = VecSource {
            schema: test_schema(),
            batches: vec![test_batch(vec![1, 2])],
        };
        let source_b = VecSource {
            schema: test_schema(),
            batches: vec![test_batch(vec![3, 4, 5])],
        };

        let sink_a_received = Arc::new(Mutex::new(Vec::new()));
        let sink_b_received = Arc::new(Mutex::new(Vec::new()));

        let engine = PipelineEngine::new(8);
        let stats = engine
            .run_transform_pipeline(
                TransformPipeline {
                    sources: vec![
                        ("a".to_string(), Box::new(source_a)),
                        ("b".to_string(), Box::new(source_b)),
                    ],
                    transform: Box::new(ConcatTransform),
                    sinks: vec![
                        (
                            "sink_a".to_string(),
                            Box::new(RecordingSink {
                                received: sink_a_received.clone(),
                                checkpoints: Arc::new(Mutex::new(Vec::new())),
                            }) as Box<dyn Sink>,
                        ),
                        (
                            "sink_b".to_string(),
                            Box::new(RecordingSink {
                                received: sink_b_received.clone(),
                                checkpoints: Arc::new(Mutex::new(Vec::new())),
                            }) as Box<dyn Sink>,
                        ),
                    ],
                },
                None,
            )
            .await
            .expect("transform pipeline runs");

        assert_eq!(stats.len(), 2, "one stats entry per sink");
        for s in &stats {
            assert_eq!(s.rows_written, 5, "fan-in concatenated both sources");
        }

        // Broadcast fan-out: both sinks receive the full (fanned-in) output.
        let rows_in = |received: &Arc<Mutex<Vec<RecordBatch>>>| -> usize {
            received.lock().unwrap().iter().map(|b| b.num_rows()).sum()
        };
        assert_eq!(rows_in(&sink_a_received), 5);
        assert_eq!(rows_in(&sink_b_received), 5);
    }

    #[tokio::test]
    async fn run_partition_emits_cumulative_progress_per_batch() {
        let source = VecSource {
            schema: test_schema(),
            batches: vec![test_batch(vec![1, 2, 3]), test_batch(vec![4, 5])],
        };
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);

        let engine = PipelineEngine::new(8);
        let stats = engine
            .run_partition(
                PartitionHandle {
                    partition_id: "p0".to_string(),
                    source: Box::new(source),
                    sink: Box::new(RecordingSink::default()),
                },
                Some(tx),
            )
            .await
            .expect("partition runs successfully");
        assert_eq!(stats.rows_written, 5);

        let first = rx.recv().await.unwrap();
        assert_eq!(first.partition_id, "p0");
        assert_eq!(first.batches_written, 1);
        assert_eq!(first.rows_written, 3);
        assert!(first.bytes_written > 0);

        let second = rx.recv().await.unwrap();
        assert_eq!(second.batches_written, 2);
        assert_eq!(
            second.rows_written, 5,
            "progress is cumulative, not per-batch"
        );
        assert!(second.bytes_written > first.bytes_written);
    }

    #[tokio::test]
    async fn fan_out_write_emits_progress_per_sink() {
        let batches = vec![test_batch(vec![1, 2, 3])];
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);

        let engine = PipelineEngine::new(8);
        let results = engine
            .fan_out_write(
                &batches,
                vec![(
                    "sink_a".to_string(),
                    Box::new(RecordingSink::default()) as Box<dyn Sink>,
                )],
                Some(tx),
            )
            .await;
        assert_eq!(results.len(), 1);
        results[0].as_ref().expect("sink write succeeds");

        let event = rx.recv().await.unwrap();
        assert_eq!(event.partition_id, "sink_a");
        assert_eq!(event.batches_written, 1);
        assert_eq!(event.rows_written, 3);
        assert!(event.bytes_written > 0);
    }
}
