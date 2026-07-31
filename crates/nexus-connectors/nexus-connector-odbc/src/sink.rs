use crate::config::{OdbcConnectorConfig, OdbcDataType, OdbcFieldSpec};
use crate::row_mapping::batch_to_json_rows;
use crate::sql::{build_delete_sql, build_insert_sql, build_update_sql, update_param_order};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{CheckpointCursor, NexusError, Opcode, Sink, OPCODE_COLUMN};
use odbc_api::parameter::InputParameter;
use odbc_api::{Bit, Connection, ConnectionOptions, Environment, IntoParameter, Nullable};
use serde_json::Value;

/// Idempotent by construction: every row is `UPDATE` (by `primary_key`)
/// falling back to `INSERT` when zero rows were affected — the closest
/// portable upsert across generic ODBC drivers, matching the `Sink`
/// contract in ARCHITECTURE.md §5.
///
/// `odbc-api`'s `Environment`/`Connection`/statement handles wrap raw ODBC
/// pointers and are not `Send`, so — unlike the ADBC connectors, which keep
/// a live connection as a struct field — every `write_batch` call opens and
/// tears down its own connection inside one `spawn_blocking` closure. The
/// handles never cross an `.await`, only owned JSON rows do.
pub struct OdbcSink {
    config: OdbcConnectorConfig,
}

impl OdbcSink {
    pub fn connect(config: &OdbcConnectorConfig) -> Result<Self, NexusError> {
        Ok(Self {
            config: config.clone(),
        })
    }
}

fn to_param(value: &Value, data_type: OdbcDataType) -> Box<dyn InputParameter> {
    match data_type {
        OdbcDataType::Int64 => {
            let nullable = value
                .as_i64()
                .map(Nullable::new)
                .unwrap_or_else(Nullable::null);
            Box::new(nullable)
        }
        OdbcDataType::Float64 => {
            let nullable = value
                .as_f64()
                .map(Nullable::new)
                .unwrap_or_else(Nullable::null);
            Box::new(nullable)
        }
        OdbcDataType::Boolean => {
            let nullable = value
                .as_bool()
                .map(|b| Bit(u8::from(b)))
                .map(Nullable::new)
                .unwrap_or_else(Nullable::null);
            Box::new(nullable)
        }
        OdbcDataType::Utf8 => {
            let opt = value.as_str().map(str::to_owned);
            Box::new(opt.into_parameter())
        }
    }
}

fn params_for<'a>(
    row: &Value,
    fields: impl Iterator<Item = &'a OdbcFieldSpec>,
) -> Vec<Box<dyn InputParameter>> {
    fields
        .map(|f| to_param(row.get(&f.name).unwrap_or(&Value::Null), f.data_type))
        .collect()
}

fn write_rows(config: &OdbcConnectorConfig, rows: &[Value]) -> Result<(), NexusError> {
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("odbc env: {e}")))?;
    let conn: Connection<'_> = env
        .connect_with_connection_string(&config.connection_string, ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("odbc connect: {e}")))?;

    let update_sql = build_update_sql(&config.table, &config.primary_key, &config.fields)?;
    let insert_sql = build_insert_sql(&config.table, &config.fields)?;
    let delete_sql = build_delete_sql(&config.table, &config.primary_key)?;
    let update_fields = update_param_order(&config.primary_key, &config.fields);
    let pk_field = config
        .fields
        .iter()
        .find(|f| f.name == config.primary_key)
        .ok_or_else(|| {
            NexusError::Schema(format!(
                "primary key '{}' not present in configured fields",
                config.primary_key
            ))
        })?;

    for row in rows {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — a
        // `D` row must be a real `DELETE`, not silently upserted. Plain
        // (non-CDC) rows have no such column and fall through unchanged.
        let opcode = row
            .get(OPCODE_COLUMN)
            .and_then(Value::as_str)
            .and_then(Opcode::from_str);
        if opcode == Some(Opcode::Delete) {
            let pk_value = row.get(&config.primary_key).unwrap_or(&Value::Null);
            let delete_params = vec![to_param(pk_value, pk_field.data_type)];
            let mut delete_stmt = conn
                .preallocate()
                .map_err(|e| NexusError::Connector(format!("odbc preallocate: {e}")))?;
            delete_stmt
                .execute(&delete_sql, delete_params.as_slice())
                .map_err(|e| NexusError::Connector(format!("odbc delete failed: {e}")))?;
            continue;
        }

        let update_params = params_for(row, update_fields.iter().copied());
        let mut update_stmt = conn
            .preallocate()
            .map_err(|e| NexusError::Connector(format!("odbc preallocate: {e}")))?;
        update_stmt
            .execute(&update_sql, update_params.as_slice())
            .map_err(|e| NexusError::Connector(format!("odbc update failed: {e}")))?;
        let updated = update_stmt
            .row_count()
            .map_err(|e| NexusError::Connector(format!("odbc row_count failed: {e}")))?
            .unwrap_or(0);

        if updated == 0 {
            let insert_params = params_for(row, config.fields.iter());
            let mut insert_stmt = conn
                .preallocate()
                .map_err(|e| NexusError::Connector(format!("odbc preallocate: {e}")))?;
            insert_stmt
                .execute(&insert_sql, insert_params.as_slice())
                .map_err(|e| NexusError::Connector(format!("odbc insert failed: {e}")))?;
        }
    }

    Ok(())
}

#[async_trait]
impl Sink for OdbcSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let rows = batch_to_json_rows(&batch)?;
        let config = self.config.clone();

        tokio::task::spawn_blocking(move || write_rows(&config, &rows))
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the
        // update-then-insert above.
        Ok(())
    }
}
