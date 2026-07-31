//! Shared CDC opcode handling for every `Sink` — see ARCHITECTURE.md §5
//! ("opcode como coluna extra do `RecordBatch`, nunca side-channel").
//! Each `Sink::write_batch` calls [`split_by_opcode`] once: absent column
//! means plain data (existing blind-upsert path, unchanged); present column
//! means the batch must be split so deletes are issued as real deletes
//! instead of being silently upserted.

use crate::checkpoint::{Opcode, OPCODE_COLUMN};
use crate::error::NexusError;
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use std::sync::Arc;

/// `upserts` (I/U rows) and `deletes` (D rows), both with the opcode column
/// stripped so they match the caller's normal data schema/column count.
pub struct CdcSplit {
    pub upserts: RecordBatch,
    pub deletes: RecordBatch,
}

/// Splits `batch` by its `__opcode` column, or returns `None` if the column
/// isn't present (plain, non-CDC data).
pub fn split_by_opcode(batch: &RecordBatch) -> Result<Option<CdcSplit>, NexusError> {
    let Ok(opcode_idx) = batch.schema().index_of(OPCODE_COLUMN) else {
        return Ok(None);
    };
    let opcode_col = batch
        .column(opcode_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| NexusError::Schema(format!("{OPCODE_COLUMN} column must be Utf8")))?;

    let data_fields: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != opcode_idx)
        .map(|(_, f)| f.clone())
        .collect();
    let data_schema: SchemaRef = Arc::new(Schema::new(data_fields));
    let data_columns: Vec<_> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != opcode_idx)
        .map(|(_, c)| c.clone())
        .collect();
    let data_batch = RecordBatch::try_new(data_schema, data_columns)
        .map_err(|e| NexusError::Schema(e.to_string()))?;

    let is_delete: BooleanArray = (0..opcode_col.len())
        .map(|i| (!opcode_col.is_null(i)).then(|| opcode_col.value(i) == Opcode::Delete.as_str()))
        .collect();
    let is_upsert: BooleanArray = (0..opcode_col.len())
        .map(|i| (!opcode_col.is_null(i)).then(|| opcode_col.value(i) != Opcode::Delete.as_str()))
        .collect();

    let deletes = filter_record_batch(&data_batch, &is_delete)
        .map_err(|e| NexusError::Schema(e.to_string()))?;
    let upserts = filter_record_batch(&data_batch, &is_upsert)
        .map_err(|e| NexusError::Schema(e.to_string()))?;

    Ok(Some(CdcSplit { upserts, deletes }))
}

/// Projects `batch` down to just the named column — used to build the
/// parameter batch for a `DELETE ... WHERE pk = $1` out of a full-width
/// delete batch produced by [`split_by_opcode`], since ADBC binds a
/// statement's placeholders positionally against every column of the bound
/// batch (a `DELETE` with one placeholder needs a one-column batch).
pub fn project_column(batch: &RecordBatch, name: &str) -> Result<RecordBatch, NexusError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| NexusError::Schema(format!("column '{name}' not found")))?;
    let schema = Arc::new(Schema::new(vec![batch.schema().field(idx).clone()]));
    RecordBatch::try_new(schema, vec![batch.column(idx).clone()])
        .map_err(|e| NexusError::Schema(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray as Utf8Array};
    use arrow_schema::{DataType, Field};

    fn batch_with_opcodes(ids: &[i64], opcodes: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(OPCODE_COLUMN, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Utf8Array::from(opcodes.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn no_opcode_column_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(split_by_opcode(&batch).unwrap().is_none());
    }

    #[test]
    fn splits_inserts_updates_and_deletes() {
        let batch = batch_with_opcodes(&[1, 2, 3, 4], &["I", "U", "D", "D"]);
        let split = split_by_opcode(&batch).unwrap().expect("has opcode column");

        assert_eq!(split.upserts.num_rows(), 2);
        assert_eq!(
            split.upserts.num_columns(),
            1,
            "opcode column must be stripped"
        );
        let upsert_ids = split
            .upserts
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(upsert_ids.values(), &[1, 2]);

        assert_eq!(split.deletes.num_rows(), 2);
        let delete_ids = split
            .deletes
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(delete_ids.values(), &[3, 4]);
    }
}
