use crate::config::{OdbcConnectorConfig, OdbcDataType, OdbcFieldSpec};
use crate::sql::build_select_sql;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use odbc_api::{Bit, ConnectionOptions, Cursor, Environment, Nullable};
use serde_json::Value;
use std::sync::Arc;

/// Bridging connector for legacy databases via ODBC — see ARCHITECTURE.md
/// §2/§4.1. Row-wise cursor access (`Cursor::next_row`), not columnar
/// buffer binding: this is the generic/portable tier, not a perf fast-path.
pub struct OdbcSource {
    config: OdbcConnectorConfig,
    schema: SchemaRef,
}

impl OdbcSource {
    pub fn connect(config: &OdbcConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            config: config.clone(),
            schema: build_schema(&config.fields),
        })
    }
}

fn build_schema(fields: &[OdbcFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    OdbcDataType::Int64 => DataType::Int64,
                    OdbcDataType::Float64 => DataType::Float64,
                    OdbcDataType::Boolean => DataType::Boolean,
                    OdbcDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

/// Runs entirely inside one blocking closure: `odbc-api` handles wrap raw
/// (non-`Send`) pointers, so the `Environment`/`Connection`/`Cursor` must
/// never cross an `.await` — see the module-level note in `sink.rs`.
fn fetch_all(
    config: &OdbcConnectorConfig,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, NexusError> {
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("odbc env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&config.connection_string, ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("odbc connect: {e}")))?;

    let sql = build_select_sql(&config.table, &config.fields)?;
    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("odbc query failed: {e}")))?
        .ok_or_else(|| NexusError::Connector("odbc SELECT returned no result set".into()))?;

    let mut batches = Vec::new();
    let mut buffer: Vec<Value> = Vec::with_capacity(config.batch_size);

    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("odbc fetch failed: {e}")))?
    {
        let mut object = serde_json::Map::new();
        for (idx, field) in config.fields.iter().enumerate() {
            let col = (idx + 1) as u16;
            let value = read_column(&mut row, col, field.data_type)?;
            object.insert(field.name.clone(), value);
        }
        buffer.push(Value::Object(object));

        if buffer.len() >= config.batch_size {
            batches.push(RecordBatchBuilder::from_json_rows(schema.clone(), &buffer)?);
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        batches.push(RecordBatchBuilder::from_json_rows(schema.clone(), &buffer)?);
    }

    Ok(batches)
}

fn read_column(
    row: &mut odbc_api::CursorRow<'_>,
    col: u16,
    data_type: OdbcDataType,
) -> Result<Value, NexusError> {
    let value = match data_type {
        OdbcDataType::Int64 => {
            let mut target = Nullable::<i64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("odbc get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        OdbcDataType::Float64 => {
            let mut target = Nullable::<f64>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("odbc get_data failed: {e}")))?;
            target.into_opt().map(Value::from).unwrap_or(Value::Null)
        }
        OdbcDataType::Boolean => {
            let mut target = Nullable::<Bit>::null();
            row.get_data(col, &mut target)
                .map_err(|e| NexusError::Connector(format!("odbc get_data failed: {e}")))?;
            target
                .into_opt()
                .map(|b| Value::from(b.as_bool()))
                .unwrap_or(Value::Null)
        }
        OdbcDataType::Utf8 => {
            let mut buf = Vec::new();
            let has_value = row
                .get_text(col, &mut buf)
                .map_err(|e| NexusError::Connector(format!("odbc get_text failed: {e}")))?;
            if has_value {
                Value::String(
                    String::from_utf8(buf).map_err(|e| NexusError::Serialization(e.to_string()))?,
                )
            } else {
                Value::Null
            }
        }
    };
    Ok(value)
}

#[async_trait]
impl Source for OdbcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let config = self.config.clone();
        let schema = self.schema.clone();

        let batches = tokio::task::spawn_blocking(move || fetch_all(&config, &schema))
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
