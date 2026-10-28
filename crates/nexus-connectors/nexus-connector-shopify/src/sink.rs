use crate::bulk::{download_jsonl, poll_until_complete, start_bulk_mutation};
use crate::config::ShopifyConnectorConfig;
use arrow_array::{Array, RecordBatch};
use async_trait::async_trait;
use nexus_core::{with_timeout, CheckpointCursor, NexusError, Sink};

/// Writes via `bulkOperationRunMutation` — real, confirmed async
/// bulk-write job: stage-upload a JSONL of `{"input": {...}}` lines
/// (one per row, columns from the batch's own schema — same
/// "columns derived from each `RecordBatch`" standard every SaaS
/// connector in this repo follows), then run the caller's mutation
/// against every staged row. Row-level failures come back in the
/// result JSONL's `userErrors` — checked and surfaced as a real error,
/// not swallowed, same principle `nexus-connector-salesforce`'s
/// `failedResults` check already applies.
pub struct ShopifySink {
    client: reqwest::Client,
    cfg: ShopifyConnectorConfig,
}

impl ShopifySink {
    pub async fn connect(cfg: &ShopifyConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        cfg.sink_mutation_or_err()?;
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
        })
    }
}

fn batch_to_jsonl(batch: &RecordBatch) -> Result<Vec<u8>, NexusError> {
    let mut rows: Vec<serde_json::Value> =
        vec![serde_json::Value::Object(Default::default()); batch.num_rows()];
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        for (row_idx, row) in rows.iter_mut().enumerate() {
            let value = arrow_cell_to_json(column, row_idx);
            let object = row.as_object_mut().ok_or_else(|| {
                NexusError::Serialization("shopify: jsonl row is not a JSON object".into())
            })?;
            object.insert(field.name().clone(), value);
        }
    }

    let mut out = Vec::new();
    for row in rows {
        let line = serde_json::json!({ "input": row });
        serde_json::to_writer(&mut out, &line)
            .map_err(|e| NexusError::Serialization(format!("shopify: jsonl encode failed: {e}")))?;
        out.push(b'\n');
    }
    Ok(out)
}

fn arrow_cell_to_json(column: &arrow_array::ArrayRef, row: usize) -> serde_json::Value {
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    if column.is_null(row) {
        return serde_json::Value::Null;
    }
    match column.data_type() {
        DataType::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::Value::from(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| serde_json::Value::from(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        _ => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or(serde_json::Value::Null),
    }
}

/// Searches for a non-empty `userErrors` array anywhere in a result
/// row, at any nesting depth — deliberately not pinned to one exact
/// envelope shape, since the real JSONL structure
/// `bulkOperationRunMutation` produces isn't confirmed against a live
/// Shopify instance this session (see README).
fn has_user_errors(rows: &[serde_json::Value]) -> bool {
    rows.iter().any(contains_non_empty_user_errors)
}

fn contains_non_empty_user_errors(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(obj) => obj.iter().any(|(key, v)| {
            if key == "userErrors" {
                v.as_array().is_some_and(|a| !a.is_empty())
            } else {
                contains_non_empty_user_errors(v)
            }
        }),
        serde_json::Value::Array(items) => items.iter().any(contains_non_empty_user_errors),
        _ => false,
    }
}

#[async_trait]
impl Sink for ShopifySink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mutation = self.cfg.sink_mutation_or_err()?.to_string();
        let jsonl = batch_to_jsonl(&batch)?;

        with_timeout(
            self.cfg.timeout_seconds,
            "shopify start bulk mutation",
            async { start_bulk_mutation(&self.client, &self.cfg, &mutation, jsonl).await },
        )
        .await?;

        let url = poll_until_complete(&self.client, &self.cfg).await?;
        if let Some(url) = url {
            let rows = with_timeout(
                self.cfg.timeout_seconds,
                "shopify download mutation results",
                async { download_jsonl(&self.client, &url).await },
            )
            .await?;
            if has_user_errors(&rows) {
                return Err(NexusError::Connector(format!(
                    "shopify bulk mutation had row-level userErrors: {rows:?}"
                )));
            }
        }

        Ok(())
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
    fn batch_to_jsonl_wraps_rows_in_input_object() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Widget"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        let jsonl = batch_to_jsonl(&batch).unwrap();
        let text = String::from_utf8(jsonl).unwrap();
        let line: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(line["input"]["title"], "Widget");
        assert_eq!(line["input"]["price"], 42);
    }

    #[test]
    fn detects_user_errors_in_result_rows() {
        let rows = vec![serde_json::json!({
            "data": {"productCreate": {"userErrors": [{"field": "title", "message": "required"}]}}
        })];
        assert!(has_user_errors(&rows));
    }

    #[test]
    fn no_user_errors_when_empty() {
        let rows = vec![serde_json::json!({
            "data": {"productCreate": {"userErrors": []}}
        })];
        assert!(!has_user_errors(&rows));
    }
}
