use crate::config::NatsConnectorConfig;
use crate::source::connect_client;
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use async_nats::Client;
use async_trait::async_trait;
use nexus_core::{with_timeout, CheckpointCursor, NexusError, Sink};

/// Publishes each row of a batch as its own JSON message on `subject`
/// — same payload shape `payload.rs::parse_payload` expects on the
/// source side, so `NatsSink` output round-trips through `NatsSource`.
/// Core NATS has no delivery ack/persistence (see
/// `NatsConnectorConfig`'s doc comment) — `client.flush()` after each
/// publish only guarantees the message left this process for the
/// server, not that any subscriber received or will ever receive it.
/// `commit_checkpoint` is a no-op for the same reason every other
/// stateless streaming sink in this workspace documents.
pub struct NatsSink {
    client: Client,
    config: NatsConnectorConfig,
}

impl NatsSink {
    pub async fn connect(config: &NatsConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let client = connect_client(config).await?;
        Ok(Self {
            client,
            config: config.clone(),
        })
    }
}

fn cell_to_json(
    batch: &RecordBatch,
    row: usize,
    col: usize,
) -> Result<serde_json::Value, NexusError> {
    let column = batch.column(col);
    Ok(match column.data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::from(arr.value(row))
            }
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::from(arr.value(row))
            }
        }
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::from(arr.value(row))
            }
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::from(arr.value(row))
            }
        }
        other => {
            return Err(NexusError::Schema(format!(
                "nats sink does not support arrow type {other:?}"
            )))
        }
    })
}

fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, NexusError> {
    let field_names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    (0..batch.num_rows())
        .map(|row| {
            let mut object = serde_json::Map::with_capacity(field_names.len());
            for (col, name) in field_names.iter().enumerate() {
                object.insert(name.clone(), cell_to_json(batch, row, col)?);
            }
            Ok(serde_json::Value::Object(object))
        })
        .collect()
}

#[async_trait]
impl Sink for NatsSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = batch_to_json_rows(&batch)?;
        for row in rows {
            let payload = serde_json::to_vec(&row)
                .map_err(|e| NexusError::Serialization(format!("nats row not JSON: {e}")))?;
            with_timeout(self.config.timeout_seconds, "nats publish", async {
                self.client
                    .publish(self.config.subject.clone(), payload.into())
                    .await
                    .map_err(|e| NexusError::Connector(format!("nats publish failed: {e}")))
            })
            .await?;
        }
        with_timeout(self.config.timeout_seconds, "nats flush", async {
            self.client
                .flush()
                .await
                .map_err(|e| NexusError::Connector(format!("nats flush failed: {e}")))
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
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn converts_batch_to_json_rows() {
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

        let rows = batch_to_json_rows(&batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[1]["name"], "b");
    }
}
