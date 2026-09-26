use crate::client::upload_file;
use crate::config::GoogleDriveConnectorConfig;
use arrow_array::RecordBatch;
use arrow_csv::WriterBuilder;
use async_trait::async_trait;
use nexus_core::{CheckpointCursor, NexusError, Sink};
use std::sync::atomic::{AtomicU64, Ordering};

/// Writes each `write_batch` call as its own new CSV file in the
/// target folder (`client::upload_file` — create + media-upload) —
/// same "one new file per batch, never overwrite/append-into-existing"
/// v1 simplification `nexus-connector-dropbox`'s sink documents (Drive
/// has no "append to this file" primitive either).
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct GoogleDriveSink {
    client: reqwest::Client,
    config: GoogleDriveConnectorConfig,
    file_counter: AtomicU64,
}

impl GoogleDriveSink {
    pub async fn connect(config: &GoogleDriveConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        Ok(Self {
            client: reqwest::Client::new(),
            config: config.clone(),
            file_counter: AtomicU64::new(0),
        })
    }
}

fn batch_to_csv_bytes(
    batch: &RecordBatch,
    delimiter: u8,
    has_header: bool,
) -> Result<Vec<u8>, NexusError> {
    let mut buf = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_delimiter(delimiter)
            .with_header(has_header)
            .build(&mut buf);
        writer
            .write(batch)
            .map_err(|e| NexusError::Connector(format!("google-drive csv encode failed: {e}")))?;
    }
    Ok(buf)
}

#[async_trait]
impl Sink for GoogleDriveSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let delimiter = self.config.delimiter as u8;
        let bytes = batch_to_csv_bytes(&batch, delimiter, self.config.has_header)?;

        let seq = self.file_counter.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let name = format!("nexusflow-{timestamp}-{seq}.csv");

        upload_file(&self.client, &self.config, &name, bytes).await
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn encodes_batch_as_csv_with_header() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let bytes = batch_to_csv_bytes(&batch, b',', true).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("id,name\n"));
        assert!(text.contains("1,a\n"));
        assert!(text.contains("2,b\n"));
    }
}
