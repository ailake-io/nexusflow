use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, SchemaRef};
use nexus_core::quote_identifier;
use nexus_core::NexusError;
use std::sync::Arc;

/// Introspects the table's Arrow schema, in source-column order — used by
/// `nexus-server` to build the Sink's parameterized upsert (column order must
/// match `SELECT *`'s order, since ADBC binds parameters positionally).
pub async fn table_schema(cfg: &PostgresConnectorConfig) -> Result<SchemaRef, NexusError> {
    let cfg = cfg.clone();
    tokio::task::spawn_blocking(move || {
        quote_identifier(&cfg.table)?;
        let connection = open_connection(&cfg.uri)?;
        connection
            .get_table_schema(None, None, &cfg.table)
            .map(Arc::new)
            .map_err(|e| NexusError::Schema(e.to_string()))
    })
    .await
    .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
}

/// How the primary key can be partitioned for the Marco 1 linear path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkPartitionKind {
    /// Integer primary key with inclusive numeric bounds.
    Int64(i64, i64),
    /// Primary key is present but not a single integer column (e.g. text,
    /// uuid, composite). The source falls back to one unpartitioned read.
    NonNumeric,
}

/// Min/max of the primary key, used to split the table into partition
/// ranges. Returns `None` for an empty table.
pub async fn primary_key_bounds(
    cfg: &PostgresConnectorConfig,
) -> Result<Option<PkPartitionKind>, NexusError> {
    let cfg = cfg.clone();
    tokio::task::spawn_blocking(move || {
        // `pk`/`table` come from the pipeline spec (attacker-controlled) and
        // get spliced into SQL text — ADBC's `bind` only covers values, not
        // identifiers. See nexus_core::sql.
        let pk = quote_identifier(&cfg.primary_key)?;
        let table = quote_identifier(&cfg.table)?;

        let mut connection = open_connection(&cfg.uri)?;
        let schema = connection
            .get_table_schema(None, None, &cfg.table)
            .map_err(|e| NexusError::Schema(e.to_string()))?;
        let pk_type = schema
            .fields()
            .iter()
            .find(|f| f.name() == &cfg.primary_key)
            .map(|f| f.data_type().clone())
            .ok_or_else(|| NexusError::Schema(format!("primary key column '{}' not found", cfg.primary_key)))?;

        let cast = match pk_type {
            DataType::Int64 => "",
            DataType::Int32 | DataType::Int16 | DataType::Int8 => "::bigint",
            _ => return Ok(Some(PkPartitionKind::NonNumeric)),
        };

        let mut statement = connection
            .new_statement()
            .map_err(|e| NexusError::Connector(e.to_string()))?;
        statement
            .set_sql_query(format!(
                "SELECT MIN({pk}){cast}, MAX({pk}){cast} FROM {table}"
            ))
            .map_err(|e| NexusError::Connector(e.to_string()))?;
        let reader = statement
            .execute()
            .map_err(|e| NexusError::Connector(e.to_string()))?;
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| NexusError::Serialization(e.to_string()))?;

        let batch = match batches.into_iter().find(|b| b.num_rows() > 0) {
            Some(b) => b,
            None => return Ok(None),
        };

        let min_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| NexusError::Schema("MIN(pk) is not Int64".into()))?;
        let max_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| NexusError::Schema("MAX(pk) is not Int64".into()))?;

        if min_arr.is_null(0) || max_arr.is_null(0) {
            return Ok(None);
        }

        Ok(Some(PkPartitionKind::Int64(min_arr.value(0), max_arr.value(0))))
    })
    .await
    .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
}
