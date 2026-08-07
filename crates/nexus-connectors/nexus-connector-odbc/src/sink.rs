use crate::config::OdbcConnectorConfig;
use crate::sql::{build_delete_sql, build_insert_sql, build_update_sql, update_param_order};
use arrow_array::{Array, RecordBatch, StringArray};
use async_trait::async_trait;
use nexus_core::{CheckpointCursor, NexusError, Opcode, Sink, OPCODE_COLUMN};
use odbc_api::parameter::InputParameter;
use odbc_api::{Connection, ConnectionOptions, Environment};
use std::sync::mpsc;

/// Idempotent by construction: every row is `UPDATE` (by `primary_key`)
/// falling back to `INSERT` when zero rows were affected — the closest
/// portable upsert across generic ODBC drivers, matching the `Sink`
/// contract in ARCHITECTURE.md §5.
///
/// `odbc-api`'s `Environment`/`Connection`/statement handles wrap raw ODBC
/// pointers and are not `Send`. To avoid opening a connection per batch, a
/// dedicated OS thread owns the ODBC environment/connection for the lifetime
/// of the sink. `write_batch` ships batches to that thread over a synchronous
/// channel and waits on a oneshot response.
pub struct OdbcSink {
    tx: mpsc::Sender<BatchRequest>,
}

struct BatchRequest {
    batch: RecordBatch,
    response: tokio::sync::oneshot::Sender<Result<(), NexusError>>,
}

impl OdbcSink {
    pub fn connect(config: &OdbcConnectorConfig) -> Result<Self, NexusError> {
        let config = config.clone();
        let (tx, rx) = mpsc::channel::<BatchRequest>();

        std::thread::Builder::new()
            .name("nexus-odbc-sink".to_string())
            .spawn(move || {
                let result = run_worker(config, rx);
                if let Err(e) = result {
                    tracing::error!("odbc sink worker terminated with error: {e}");
                }
            })
            .map_err(|e| NexusError::Connector(format!("failed to spawn odbc worker: {e}")))?;

        Ok(Self { tx })
    }
}

fn run_worker(
    config: OdbcConnectorConfig,
    rx: mpsc::Receiver<BatchRequest>,
) -> Result<(), NexusError> {
    let env = Environment::new().map_err(|e| NexusError::Connector(format!("odbc env: {e}")))?;
    let conn: Connection<'_> = env
        .connect_with_connection_string(&config.connection_string, ConnectionOptions::default())
        .map_err(|e| NexusError::Connector(format!("odbc connect: {e}")))?;

    conn.set_autocommit(false)
        .map_err(|e| NexusError::Connector(format!("odbc set_autocommit(false): {e}")))?;

    for req in rx {
        let result = write_batch(&config, &conn, &req.batch);
        let _ = req.response.send(result);
    }

    Ok(())
}

fn write_batch(
    config: &OdbcConnectorConfig,
    conn: &Connection<'_>,
    batch: &RecordBatch,
) -> Result<(), NexusError> {
    let update_sql = build_update_sql(&config.table, &config.primary_key, &config.fields)?;
    let insert_sql = build_insert_sql(&config.table, &config.fields)?;
    let delete_sql = build_delete_sql(&config.table, &config.primary_key)?;

    let ordered_fields = update_param_order(&config.primary_key, &config.fields);
    let update_field_indices: Vec<_> = ordered_fields
        .iter()
        .map(|f| {
            batch
                .schema()
                .index_of(&f.name)
                .map_err(|_| NexusError::Schema(format!("column '{}' not found", f.name)))
        })
        .collect::<Result<_, _>>()?;
    let field_spec_by_name: std::collections::HashMap<_, _> =
        config.fields.iter().map(|f| (&f.name, f)).collect();
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

    let mut update_stmt = conn
        .preallocate()
        .map_err(|e| NexusError::Connector(format!("odbc preallocate update: {e}")))?;
    let mut insert_stmt = conn
        .preallocate()
        .map_err(|e| NexusError::Connector(format!("odbc preallocate insert: {e}")))?;
    let mut delete_stmt = conn
        .preallocate()
        .map_err(|e| NexusError::Connector(format!("odbc preallocate delete: {e}")))?;

    let result: Result<(), NexusError> = (|| {
        for row_idx in 0..batch.num_rows() {
            if opcode_for_row(batch, row_idx) == Some(Opcode::Delete) {
                let delete_params = vec![pk_param(batch, row_idx, pk_field)?];
                delete_stmt
                    .execute(&delete_sql, delete_params.as_slice())
                    .map_err(|e| NexusError::Connector(format!("odbc delete failed: {e}")))?;
                continue;
            }

            let update_params: Vec<Box<dyn InputParameter>> = update_field_indices
                .iter()
                .zip(&ordered_fields)
                .map(|(&col, f)| {
                    let spec = field_spec_by_name.get(&f.name).ok_or_else(|| {
                        NexusError::Schema(format!("field '{}' not configured", f.name))
                    })?;
                    crate::row_mapping::cell_to_param(batch, row_idx, col, spec.data_type)
                })
                .collect::<Result<_, _>>()?;
            update_stmt
                .execute(&update_sql, update_params.as_slice())
                .map_err(|e| NexusError::Connector(format!("odbc update failed: {e}")))?;
            let updated = update_stmt
                .row_count()
                .map_err(|e| NexusError::Connector(format!("odbc row_count failed: {e}")))?
                .unwrap_or(0);

            if updated == 0 {
                let insert_params: Vec<Box<dyn InputParameter>> = config
                    .fields
                    .iter()
                    .map(|f| {
                        let col = batch.schema().index_of(&f.name).map_err(|_| {
                            NexusError::Schema(format!("column '{}' not found", f.name))
                        })?;
                        crate::row_mapping::cell_to_param(batch, row_idx, col, f.data_type)
                    })
                    .collect::<Result<_, _>>()?;
                insert_stmt
                    .execute(&insert_sql, insert_params.as_slice())
                    .map_err(|e| NexusError::Connector(format!("odbc insert failed: {e}")))?;
            }
        }
        Ok(())
    })();

    match result {
        Ok(()) => conn
            .commit()
            .map_err(|e| NexusError::Connector(format!("odbc commit failed: {e}"))),
        Err(e) => {
            let _ = conn.rollback();
            Err(e)
        }
    }
}

fn pk_param(
    batch: &RecordBatch,
    row: usize,
    pk_field: &crate::config::OdbcFieldSpec,
) -> Result<Box<dyn InputParameter>, NexusError> {
    let col = batch.schema().index_of(&pk_field.name).map_err(|_| {
        NexusError::Schema(format!("primary key column '{}' not found", pk_field.name))
    })?;
    crate::row_mapping::cell_to_param(batch, row, col, pk_field.data_type)
}

fn opcode_for_row(batch: &RecordBatch, row: usize) -> Option<Opcode> {
    let idx = batch.schema().index_of(OPCODE_COLUMN).ok()?;
    let column = batch.column(idx);
    let arr = column.as_any().downcast_ref::<StringArray>()?;
    if arr.is_null(row) {
        None
    } else {
        Opcode::from_letter(arr.value(row))
    }
}

#[async_trait]
impl Sink for OdbcSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(BatchRequest { batch, response: tx })
            .map_err(|_| NexusError::Connector("odbc sink worker has terminated".to_string()))?;

        rx.await
            .map_err(|e| NexusError::Connector(format!("odbc worker response dropped: {e}")))?
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the
        // update-then-insert above.
        Ok(())
    }
}
