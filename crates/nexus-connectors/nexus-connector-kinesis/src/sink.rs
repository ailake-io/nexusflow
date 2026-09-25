use crate::config::KinesisConnectorConfig;
use crate::source::build_client;
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::Client;
use nexus_core::{retry_with_backoff, with_timeout, CheckpointCursor, NexusError, Sink};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Sink for Amazon Kinesis Data Streams. Reuses the same async-native,
/// `Send`-friendly `Client` the source builds (`build_client`) — no
/// `spawn_blocking` needed, same as `KinesisSource`.
///
/// `PutRecords` caps out at 500 records / 5MB per call — this sink only
/// chunks by record count (v1 simplification, same tone as
/// `KinesisSource`'s resharding note: a single pathologically large row
/// could still blow the 5MB request cap, not handled here). A partial
/// failure (`failed_record_count > 0`) retries just the failed entries,
/// up to `config.retry.retries` times with the same exponential backoff
/// `nexus-connector-rest`'s `WebhookSink` uses, rather than resending the
/// whole chunk (which would double-write the records that already
/// succeeded — Kinesis has no idempotency key to de-dupe on).
///
/// No external checkpoint state: `commit_checkpoint` is a no-op, same as
/// `WebhookSink` and every other externally-stateless sink in this
/// workspace.
pub struct KinesisSink {
    client: Client,
    config: KinesisConnectorConfig,
}

const MAX_RECORDS_PER_PUT: usize = 500;

impl KinesisSink {
    pub async fn connect(config: &KinesisConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let client = build_client(config);

        retry_with_backoff(&config.retry, "kinesis sink connect", || async {
            with_timeout(
                config.timeout_seconds,
                "kinesis describe_stream_summary",
                async {
                    client
                        .describe_stream_summary()
                        .stream_name(&config.stream_name)
                        .send()
                        .await
                        .map_err(|e| {
                            NexusError::Connector(format!(
                                "kinesis describe_stream_summary failed: {e}"
                            ))
                        })
                },
            )
            .await
        })
        .await?;

        Ok(Self {
            client,
            config: config.clone(),
        })
    }

    async fn put_chunk(&self, entries: Vec<PutRecordsRequestEntry>) -> Result<(), NexusError> {
        let mut pending = entries;
        let mut attempt = 0u32;

        loop {
            let client = self.client.clone();
            let stream_name = self.config.stream_name.clone();
            let batch = pending.clone();
            let output = with_timeout(self.config.timeout_seconds, "kinesis put_records", async {
                client
                    .put_records()
                    .stream_name(&stream_name)
                    .set_records(Some(batch))
                    .send()
                    .await
                    .map_err(|e| NexusError::Connector(format!("kinesis put_records failed: {e:?}")))
            })
            .await?;

            let failed = output.failed_record_count().unwrap_or(0);
            if failed == 0 {
                return Ok(());
            }

            let results = output.records();
            let mut retry_entries = Vec::with_capacity(failed as usize);
            let mut last_error = String::new();
            for (entry, result) in pending.into_iter().zip(results.iter()) {
                if let Some(code) = result.error_code() {
                    last_error = format!(
                        "{code}: {}",
                        result.error_message().unwrap_or("(no message)")
                    );
                    retry_entries.push(entry);
                }
            }
            pending = retry_entries;

            if attempt >= self.config.retry.retries {
                return Err(NexusError::Connector(format!(
                    "kinesis put_records: {} record(s) still failing after {} attempt(s), last error: {last_error}",
                    pending.len(),
                    attempt + 1
                )));
            }

            let delay = Duration::from_secs(self.config.retry.retry_backoff_seconds)
                * 2u32.saturating_pow(attempt);
            tracing::warn!(
                "kinesis put_records: {} record(s) failed (attempt {}/{}), retrying in {:?}: {last_error}",
                pending.len(),
                attempt + 1,
                self.config.retry.retries + 1,
                delay
            );
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

#[async_trait]
impl Sink for KinesisSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let rows = batch_to_json_rows(&batch)?;
        let partition_key_column = self.config.partition_key_column.as_deref();

        let entries: Vec<PutRecordsRequestEntry> = rows
            .iter()
            .map(|row| {
                let key = partition_key(row, partition_key_column);
                let data = serde_json::to_vec(row).map_err(|e| {
                    NexusError::Serialization(format!("kinesis: failed to encode row: {e}"))
                })?;
                PutRecordsRequestEntry::builder()
                    .data(Blob::new(data))
                    .partition_key(key)
                    .build()
                    .map_err(|e| {
                        NexusError::Connector(format!("kinesis: failed to build record entry: {e}"))
                    })
            })
            .collect::<Result<_, NexusError>>()?;

        for chunk in entries.chunks(MAX_RECORDS_PER_PUT) {
            self.put_chunk(chunk.to_vec()).await?;
        }

        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

/// Stringified value of `partition_key_column` when configured, else a
/// stable hash of the row's JSON encoding — spreads rows across shards
/// without requiring the caller to pick a column.
fn partition_key(row: &Value, column: Option<&str>) -> String {
    if let Some(column) = column {
        if let Some(value) = row.get(column) {
            return match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
        }
    }
    let mut hasher = DefaultHasher::new();
    row.to_string().hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Mirrors `nexus-connector-rest`'s `WebhookSink::batch_to_json_rows` —
/// same four primitive types `KinesisFieldSpec`/`build_schema` (source
/// side) already support, kept as a local copy per this workspace's
/// convention (no shared cross-crate row-conversion helper).
fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Value>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![serde_json::Map::with_capacity(batch.num_columns()); num_rows];

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let name = field.name();

        macro_rules! downcast {
            ($ty:ty) => {
                column.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                    NexusError::Schema(format!("column '{name}' has unexpected array type"))
                })?
            };
        }

        match field.data_type() {
            DataType::Int64 => {
                let arr = downcast!(Int64Array);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            DataType::Float64 => {
                let arr = downcast!(Float64Array);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            DataType::Boolean => {
                let arr = downcast!(BooleanArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            DataType::Utf8 => {
                let arr = downcast!(StringArray);
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(arr.value(i))
                    };
                    row.insert(name.clone(), value);
                }
            }
            other => {
                return Err(NexusError::Schema(format!(
                    "unsupported data type for field '{name}': {other:?}"
                )));
            }
        }
    }

    Ok(rows.into_iter().map(Value::Object).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn batch_to_json_rows_round_trips_typed_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap();

        let rows = batch_to_json_rows(&batch).unwrap();
        assert_eq!(
            rows,
            vec![
                json!({"id": 1, "name": "alice"}),
                json!({"id": 2, "name": null})
            ]
        );
    }

    #[test]
    fn partition_key_uses_configured_column_when_present() {
        let row = json!({"id": 42, "name": "alice"});
        assert_eq!(partition_key(&row, Some("id")), "42");
        assert_eq!(partition_key(&row, Some("name")), "alice");
    }

    #[test]
    fn partition_key_falls_back_to_stable_hash_of_row() {
        let row = json!({"id": 1});
        let a = partition_key(&row, None);
        let b = partition_key(&row, None);
        assert_eq!(a, b, "hash-based fallback must be stable for the same row");

        let other = json!({"id": 2});
        assert_ne!(partition_key(&row, None), partition_key(&other, None));
    }

    #[test]
    fn partition_key_falls_back_when_configured_column_is_missing() {
        let row = json!({"id": 1});
        // "missing" isn't in the row — falls back to the hash, doesn't panic.
        let key = partition_key(&row, Some("missing"));
        assert_eq!(key, partition_key(&row, None));
    }
}
