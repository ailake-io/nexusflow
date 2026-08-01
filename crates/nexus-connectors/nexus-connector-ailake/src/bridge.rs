//! Cross-version Arrow bridge.
//!
//! `nexus-core`'s `Sink`/`Source` traits carry `arrow_array::RecordBatch`
//! pinned to the workspace's 58.4.0, but the published `ailake-query`/
//! `ailake-file` crates pin 52.2.0 internally — a different, incompatible
//! Rust type even though the data model is identical. The Arrow IPC byte
//! format is stable across arrow-rs versions (both write/read the same
//! Flatbuffers-framed layout), so round-tripping through it here is a safe
//! alternative to transmuting between two independently-versioned FFI
//! struct layouts.

use arrow_array::RecordBatch as NewBatch;
use arrow_array_old::RecordBatch as OldBatch;
use arrow_ipc::reader::StreamReader as NewStreamReader;
use arrow_ipc::writer::StreamWriter as NewStreamWriter;
use arrow_ipc_old::reader::StreamReader as OldStreamReader;
use arrow_ipc_old::writer::StreamWriter as OldStreamWriter;
use nexus_core::NexusError;
use std::io::Cursor;

/// Our (58.4.0) `RecordBatch` → ailake's (52.2.0) `RecordBatch`.
pub fn to_old_batch(batch: &NewBatch) -> Result<OldBatch, NexusError> {
    let mut buf = Vec::new();
    {
        let mut writer = NewStreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (new) failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (new) failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (new) failed: {e}")))?;
    }
    let mut reader = OldStreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| NexusError::Schema(format!("arrow ipc read (old) failed: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| NexusError::Schema("arrow ipc bridge: empty stream".to_string()))?
        .map_err(|e| NexusError::Schema(format!("arrow ipc read (old) failed: {e}")))?;
    Ok(batch)
}

/// ailake's (52.2.0) `RecordBatch` → our (58.4.0) `RecordBatch`.
pub fn to_new_batch(batch: &OldBatch) -> Result<NewBatch, NexusError> {
    let mut buf = Vec::new();
    {
        let mut writer = OldStreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (old) failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (old) failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (old) failed: {e}")))?;
    }
    let mut reader = NewStreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| NexusError::Schema(format!("arrow ipc read (new) failed: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| NexusError::Schema("arrow ipc bridge: empty stream".to_string()))?
        .map_err(|e| NexusError::Schema(format!("arrow ipc read (new) failed: {e}")))?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn round_trips_through_both_bridge_directions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = NewBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let old = to_old_batch(&batch).expect("bridges to old");
        assert_eq!(old.num_rows(), 3);
        assert_eq!(old.num_columns(), 2);

        let back = to_new_batch(&old).expect("bridges back to new");
        assert_eq!(back.num_rows(), 3);
        let ids = back
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }
}
