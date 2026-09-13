use crate::config::RedisConnectorConfig;
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use nexus_core::{with_timeout, CheckpointCursor, NexusError, Sink};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};

/// Writes each `RecordBatch` row as a Redis Stream entry (`XADD`) —
/// unlike the source (which projects a fixed `fields` list), the sink
/// writes every column of whatever schema it's handed, field name =
/// column name. Stream fields are always strings on the wire, so every
/// cell is stringified (`to_string()` for numbers/bools, as-is for
/// utf8) — same "no typed wire format" reality every Redis client
/// faces, not a NexusFlow limitation.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op, same
/// as `KafkaSink`/`KinesisSink`/`PulsarSink` — a stream's own
/// monotonic entry IDs are the durability record, not something this
/// sink needs to track separately.
pub struct RedisSink {
    connection: MultiplexedConnection,
    config: RedisConnectorConfig,
}

impl RedisSink {
    pub async fn connect(config: &RedisConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let client = Client::open(config.url.as_str())
            .map_err(|e| NexusError::Connector(format!("redis client: {e}")))?;
        let connection = with_timeout(config.timeout_seconds, "redis connect", async {
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| NexusError::Connector(format!("redis connect: {e}")))
        })
        .await?;
        Ok(Self {
            connection,
            config: config.clone(),
        })
    }
}

fn cell_to_string(batch: &RecordBatch, row: usize, col: usize) -> Result<String, NexusError> {
    let column = batch.column(col);
    Ok(match column.data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                String::new()
            } else {
                arr.value(row).to_string()
            }
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                String::new()
            } else {
                arr.value(row).to_string()
            }
        }
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                String::new()
            } else {
                arr.value(row).to_string()
            }
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| NexusError::Schema("column has unexpected array type".into()))?;
            if arr.is_null(row) {
                String::new()
            } else {
                arr.value(row).to_string()
            }
        }
        other => {
            return Err(NexusError::Schema(format!(
                "redis sink does not support arrow type {other:?}"
            )))
        }
    })
}

#[async_trait]
impl Sink for RedisSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let field_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        for row in 0..batch.num_rows() {
            let mut pairs: Vec<(String, String)> = Vec::with_capacity(field_names.len());
            for (col, name) in field_names.iter().enumerate() {
                pairs.push((name.clone(), cell_to_string(&batch, row, col)?));
            }
            with_timeout(self.config.timeout_seconds, "redis xadd", async {
                self.connection
                    .xadd::<_, _, _, _, String>(&self.config.stream_key, "*", &pairs)
                    .await
                    .map_err(|e| NexusError::Connector(format!("redis xadd failed: {e}")))
            })
            .await?;
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
    use arrow_array::RecordBatch;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn stringifies_all_four_primitive_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();

        assert_eq!(cell_to_string(&batch, 0, 0).unwrap(), "1");
        assert_eq!(cell_to_string(&batch, 0, 1).unwrap(), "1.5");
        assert_eq!(cell_to_string(&batch, 0, 2).unwrap(), "true");
        assert_eq!(cell_to_string(&batch, 0, 3).unwrap(), "a");
    }

    #[test]
    fn nulls_become_empty_string() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![None]))]).unwrap();
        assert_eq!(cell_to_string(&batch, 0, 0).unwrap(), "");
    }
}
