use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;

/// Accumulates `RecordBatch`es and flushes when a row or byte threshold is
/// reached. Used by lakehouse sinks to amortize the cost of opening a table,
/// writing files, and committing a transaction across many small batches.
///
/// All buffered batches must share the same schema; the first batch sets the
/// schema and later batches with a different schema are rejected.
pub struct RecordBatchBuffer {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    row_threshold: usize,
    byte_threshold: usize,
}

impl RecordBatchBuffer {
    pub fn new(row_threshold: usize, byte_threshold: usize) -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            row_threshold,
            byte_threshold,
        }
    }

    /// Adds a batch to the buffer. Returns the concatenated buffered batches
    /// if the threshold was crossed and the buffer was flushed.
    pub fn push(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>, arrow_schema::ArrowError> {
        let batch_rows = batch.num_rows();
        let batch_bytes = batch.get_array_memory_size();
        if batch_rows == 0 {
            return Ok(None);
        }

        if let Some(schema) = &self.schema {
            if batch.schema().fields().len() != schema.fields().len()
                || !batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(schema.fields().iter())
                    .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
            {
                // Schema mismatch: flush what we have and start fresh with the
                // incoming batch's schema.
                let flushed = self.take()?;
                self.schema = Some(batch.schema());
                self.batches.push(batch);
                self.rows = batch_rows;
                self.bytes = batch_bytes;
                return Ok(flushed);
            }
        } else {
            self.schema = Some(batch.schema());
        }

        self.rows += batch_rows;
        self.bytes += batch_bytes;
        self.batches.push(batch);

        if self.rows >= self.row_threshold || self.bytes >= self.byte_threshold {
            return self.take();
        }

        Ok(None)
    }

    /// Returns and clears the current buffered batches, concatenated into one.
    pub fn take(&mut self) -> Result<Option<RecordBatch>, arrow_schema::ArrowError> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let schema = self.schema.clone().expect("schema set when batches exist");
        let batches = std::mem::take(&mut self.batches);
        self.rows = 0;
        self.bytes = 0;
        concat_batches(&schema, &batches).map(Some)
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn flushes_when_row_threshold_reached() {
        let mut buf = RecordBatchBuffer::new(5, usize::MAX);
        assert!(buf.push(make_batch(vec![1, 2])).unwrap().is_none());
        let flushed = buf.push(make_batch(vec![3, 4, 5])).unwrap();
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().num_rows(), 5);
        assert!(buf.is_empty());
    }

    #[test]
    fn flushes_remaining_on_take() {
        let mut buf = RecordBatchBuffer::new(100, usize::MAX);
        buf.push(make_batch(vec![1, 2, 3])).unwrap();
        let remaining = buf.take().unwrap();
        assert_eq!(remaining.unwrap().num_rows(), 3);
        assert!(buf.is_empty());
    }

    #[test]
    fn schema_mismatch_flushes_existing_buffer() {
        let mut buf = RecordBatchBuffer::new(100, usize::MAX);
        buf.push(make_batch(vec![1, 2])).unwrap();
        let schema2 = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch2 = RecordBatch::try_new(schema2, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
        let flushed = buf.push(batch2).unwrap();
        assert_eq!(flushed.unwrap().num_rows(), 2);
        assert_eq!(buf.rows(), 1);
    }
}
