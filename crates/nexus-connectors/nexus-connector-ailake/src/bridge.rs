//! Cross-version Arrow bridge.
//!
//! `nexus-core`'s `Sink`/`Source` traits carry `arrow_array::RecordBatch`
//! pinned to the workspace's 58.4.0, but the published `ailake-query`/
//! `ailake-file` crates use Arrow 60 internally — a different, incompatible
//! Rust type even though the data model is identical. The Arrow IPC byte
//! format is stable across arrow-rs versions (both write/read the same
//! Flatbuffers-framed layout), so round-tripping through it here is a safe
//! alternative to transmuting between two independently-versioned FFI
//! struct layouts.

use arrow_array::RecordBatch as NewBatch;
use arrow_array_ailake::RecordBatch as AilakeBatch;
use arrow_ipc::reader::StreamReader as NewStreamReader;
use arrow_ipc::writer::StreamWriter as NewStreamWriter;
use arrow_ipc_ailake::reader::StreamReader as AilakeStreamReader;
use arrow_ipc_ailake::writer::StreamWriter as AilakeStreamWriter;
use nexus_core::NexusError;
use std::io::Cursor;

/// Our (58.4.0) `RecordBatch` → ailake's (60.0.0) `RecordBatch`.
pub fn to_ailake_batch(batch: &NewBatch) -> Result<AilakeBatch, NexusError> {
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
    let mut reader = AilakeStreamReader::try_new(Cursor::new(buf), None)
            .map_err(|e| NexusError::Schema(format!("arrow ipc read (ailake) failed: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| NexusError::Schema("arrow ipc bridge: empty stream".to_string()))?
        .map_err(|e| NexusError::Schema(format!("arrow ipc read (ailake) failed: {e}")))?;
    Ok(batch)
}

/// ailake's (60.0.0) `RecordBatch` → our (58.4.0) `RecordBatch`.
pub fn to_nexus_batch(batch: &AilakeBatch) -> Result<NewBatch, NexusError> {
    let mut buf = Vec::new();
    {
        let mut writer = AilakeStreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (ailake) failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (ailake) failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| NexusError::Schema(format!("arrow ipc write (ailake) failed: {e}")))?;
    }
    let mut reader = NewStreamReader::try_new(Cursor::new(buf), None)
            .map_err(|e| NexusError::Schema(format!("arrow ipc read (nexus) failed: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| NexusError::Schema("arrow ipc bridge: empty stream".to_string()))?
        .map_err(|e| NexusError::Schema(format!("arrow ipc read (nexus) failed: {e}")))?;
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

        let ailake = to_ailake_batch(&batch).expect("bridges to ailake");
        assert_eq!(ailake.num_rows(), 3);
        assert_eq!(ailake.num_columns(), 2);

        let back = to_nexus_batch(&ailake).expect("bridges back to nexus");
        assert_eq!(back.num_rows(), 3);
        let ids = back
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }
}
