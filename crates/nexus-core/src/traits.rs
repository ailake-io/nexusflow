use crate::checkpoint::CheckpointCursor;
use crate::error::NexusError;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// How a connector reaches its backend. Decided at node-configuration time,
/// never chosen dynamically at runtime — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

pub trait Transform: Send + Sync {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, NexusError>;
}
