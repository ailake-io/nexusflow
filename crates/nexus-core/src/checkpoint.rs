use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Column name every `Sink` checks for to decide whether a batch carries
/// CDC opcodes at all — absence means "plain data, blind upsert" (backward
/// compatible with every non-CDC pipeline). See ARCHITECTURE.md §5.
pub const OPCODE_COLUMN: &str = "__opcode";

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

impl Opcode {
    /// Canonical single-letter form carried as the opcode column value —
    /// see ARCHITECTURE.md §5 ("nunca como side-channel separado").
    pub const fn as_str(self) -> &'static str {
        match self {
            Opcode::Insert => "I",
            Opcode::Update => "U",
            Opcode::Delete => "D",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "I" => Some(Opcode::Insert),
            "U" => Some(Opcode::Update),
            "D" => Some(Opcode::Delete),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_round_trips_through_canonical_letter() {
        for op in [Opcode::Insert, Opcode::Update, Opcode::Delete] {
            assert_eq!(Opcode::from_str(op.as_str()), Some(op));
        }
    }

    #[test]
    fn unknown_letter_is_not_an_opcode() {
        assert_eq!(Opcode::from_str("X"), None);
    }
}
