use crate::checkpoint::CheckpointCursor;
use crate::error::NexusError;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Serialize;

/// How a connector reaches its backend. Decided at node-configuration time,
/// never chosen dynamically at runtime — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorCapability {
    AdbcNative,
    ArrowFlight,
    Bridged,
}

#[async_trait]
pub trait Source: Send {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError>;

    fn schema(&self) -> SchemaRef;
}

#[async_trait]
pub trait Sink: Send {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError>;

    async fn commit_checkpoint(&mut self, cursor: CheckpointCursor) -> Result<(), NexusError>;
}

/// Named, fully-materialized inputs (one entry per fan-in source) go in;
/// output batches come out. Unlike `Source`/`Sink`, this isn't a per-batch
/// streaming operation — a SQL transform needs every input row available to
/// do joins/aggregations, so callers drain each `Source` first (see
/// `PipelineEngine::run_transform_pipeline`). See ARCHITECTURE.md §4.
#[async_trait]
pub trait Transform: Send + Sync {
    async fn apply(
        &self,
        inputs: Vec<(String, SchemaRef, Vec<RecordBatch>)>,
    ) -> Result<Vec<RecordBatch>, NexusError>;
}
