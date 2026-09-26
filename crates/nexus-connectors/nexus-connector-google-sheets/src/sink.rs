use crate::config::GoogleSheetsConnectorConfig;
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{retry_with_backoff, CheckpointCursor, NexusError, Sink};
use serde_json::{json, Value};

/// Appends rows via the Sheets API v4's `values.append`
/// (`POST /v4/spreadsheets/{id}/values/{range}:append`, real,
/// documented — Sheets finds the first empty row after existing data
/// in the target sheet/column and inserts after it, never overwrites)
/// — an ETL sink that only ever adds new rows, same "INSERT not
/// REPLACE" semantics every other append-style sink in this repo has.
///
/// All columns are read as strings (`StringArray`) — same
/// "everything's a string on the wire" simplification
/// `nexus-connector-hubspot`/`zendesk`'s sinks document; row order is
/// preserved (a cell-per-column array per row, column order taken
/// from the batch's own schema, not from any header row already in
/// the sheet — the caller is responsible for a schema whose column
/// order matches the target sheet's columns).
///
/// No external checkpoint state: `commit_checkpoint` is a no-op.
pub struct GoogleSheetsSink {
    client: reqwest::Client,
    config: GoogleSheetsConnectorConfig,
}

impl GoogleSheetsSink {
    pub async fn connect(config: &GoogleSheetsConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        Ok(Self {
            client: reqwest::Client::new(),
            config: config.clone(),
        })
    }
}

fn cell_to_string(batch: &RecordBatch, row: usize, col: usize) -> Result<Value, NexusError> {
    let column = batch.column(col);
    let arr = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NexusError::Schema(
                "google-sheets sink only supports Utf8 columns — cast upstream first".to_string(),
            )
        })?;
    Ok(if arr.is_null(row) {
        Value::String(String::new())
    } else {
        Value::String(arr.value(row).to_string())
    })
}

fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<Vec<Value>>, NexusError> {
    let num_cols = batch.num_columns();
    (0..batch.num_rows())
        .map(|row| {
            (0..num_cols)
                .map(|col| cell_to_string(batch, row, col))
                .collect()
        })
        .collect()
}

#[async_trait]
impl Sink for GoogleSheetsSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let values = batch_to_rows(&batch)?;
        let mut url = reqwest::Url::parse(&format!(
            "{}/v4/spreadsheets/{}/values",
            self.config.base_url, self.config.spreadsheet_id
        ))
        .map_err(|e| NexusError::Connector(format!("google-sheets: invalid base_url: {e}")))?;
        url.path_segments_mut()
            .map_err(|_| NexusError::Connector("google-sheets: base_url cannot be a base".into()))?
            .push(&format!("{}:append", self.config.range));
        url.query_pairs_mut().append_pair("valueInputOption", "RAW");

        let body = json!({ "values": values });
        let client = self.client.clone();
        let access_token = self.config.access_token.clone();
        let timeout_seconds = self.config.timeout_seconds;
        retry_with_backoff(&self.config.retry, "google-sheets values.append", || {
            let client = client.clone();
            let url = url.clone();
            let access_token = access_token.clone();
            let body = body.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .post(url)
                        .bearer_auth(&access_token)
                        .json(&body)
                        .send(),
                )
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("google-sheets values.append timed out: {e}"))
                })?
                .map_err(|e| {
                    NexusError::Connector(format!("google-sheets values.append failed: {e}"))
                })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "google-sheets values.append failed ({status}): {text}"
                    )));
                }
                Ok(())
            }
        })
        .await
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn converts_batch_to_row_arrays() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(StringArray::from(vec![Some("1"), None])),
            ],
        )
        .unwrap();

        let rows = batch_to_rows(&batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![Value::String("x".into()), Value::String("1".into())]
        );
        assert_eq!(
            rows[1],
            vec![Value::String("y".into()), Value::String(String::new())]
        );
    }
}
