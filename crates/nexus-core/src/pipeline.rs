use crate::checkpoint::CheckpointCursor;
use crate::error::NexusError;
use crate::traits::{Sink, Source};
use arrow_array::RecordBatch;
use futures::StreamExt;
use serde::Serialize;

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

    pub async fn run_partition(
        &self,
        handle: PartitionHandle,
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

            while let Some(batch) = rx.recv().await {
                rows_written += batch.num_rows();
                batches_written += 1;
                sink.write_batch(batch).await?;
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
    pub async fn run(
        &self,
        partitions: Vec<PartitionHandle>,
    ) -> Vec<Result<PartitionStats, NexusError>> {
        let mut set = tokio::task::JoinSet::new();
        for partition in partitions {
            // Reuse run_partition's reader/writer split for each partition,
            // one JoinSet entry per partition so partitions run concurrently.
            let capacity = self.channel_capacity;
            set.spawn(async move { PipelineEngine::new(capacity).run_partition(partition).await });
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
            .run_partition(PartitionHandle {
                partition_id: "p0".to_string(),
                source: Box::new(source),
                sink: Box::new(sink),
            })
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
            .run(vec![
                make_partition("p0", vec![1, 2]),
                make_partition("p1", vec![3, 4, 5]),
            ])
            .await;

        assert_eq!(results.len(), 2);
        let total_rows: usize = results
            .into_iter()
            .map(|r| r.expect("partition succeeds").rows_written)
            .sum();
        assert_eq!(total_rows, 5);
    }
}
