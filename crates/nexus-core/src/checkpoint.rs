use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// I/U/D — CDC opcode carried as a column on the RecordBatch, never a side-channel.
/// See ARCHITECTURE.md §5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Opcode {
    Insert,
    Update,
    Delete,
}

/// One cursor per partition, never per pipeline — see ARCHITECTURE.md §5.
/// Guarantee is at-least-once: Sink implementations must be idempotent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointCursor {
    pub partition_id: String,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub offset: Option<i64>,
    pub opcode: Option<Opcode>,
}

impl CheckpointCursor {
    pub fn new(partition_id: impl Into<String>) -> Self {
        Self {
            partition_id: partition_id.into(),
            last_updated_at: None,
            offset: None,
            opcode: None,
        }
    }
}
