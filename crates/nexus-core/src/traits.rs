use crate::checkpoint::CheckpointCursor;
use crate::error::NexusError;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Serialize;
use std::sync::{Arc, Mutex};

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

    /// A shared handle a source can update internally (from inside its own
    /// `read_batches` stream) with its opaque, connector-specific resume
    /// position (e.g. a MySQL `"{binlog_file}:{position}"` pair, a MongoDB
    /// change-stream resume token) as it produces batches.
    /// `PipelineEngine::run_partition` reads the final value back out
    /// after the source's stream ends, to persist via
    /// `CheckpointCursor.resume_state`.
    ///
    /// Shape is `Arc<Mutex<...>>`, not a plain `&self` getter, because of a
    /// real borrow-checker constraint: `read_batches(&mut self)` returns a
    /// stream borrowing `self` for the stream's entire lifetime, so nothing
    /// else can call back into `self` while that stream is still alive (and
    /// it has to stay alive until the source's task finishes). Fetching
    /// this handle *before* calling `read_batches` sidesteps that — the
    /// source updates the shared cell itself, no re-borrow needed.
    ///
    /// `None` by default, so this doesn't break any of the ~20 existing
    /// `Source` implementations that have nothing meaningful to report here
    /// (batch sources that naturally terminate don't need it; Postgres CDC
    /// tracks its own position server-side via the replication slot, see
    /// `nexus-connector-postgres/src/cdc.rs`'s `update_applied_lsn` call —
    /// no client-side state needed for that one).
    fn position_handle(&self) -> Option<Arc<Mutex<Option<String>>>> {
        None
    }
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
