//! Row-level conversions between MySQL's wire types (`mysql_async::Row`/
//! `Value`) and Arrow (`RecordBatch`), plus SQL string building for the
//! batch source/sink. Same 4-primitive-type ceiling as the CDC side
//! (`MySqlCdcDataType`) — this crate has no ADBC fast-path to fall back to,
//! see `config.rs`'s module doc.

use crate::config::{MySqlCdcDataType, MySqlCdcFieldSpec};
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use mysql_async::{Row, Value as MySqlValue};
use nexus_core::NexusError;
use serde_json::Value as JsonValue;

/// Backtick-quotes a MySQL identifier — `quote_identifier` in `nexus-core`
/// is ANSI double-quote (Postgres) syntax, which MySQL doesn't accept by
/// default. `validate_identifier` still rejects anything that isn't a plain
/// `[A-Za-z_][A-Za-z0-9_]*` up to 128 bytes, so there's nothing to escape
/// inside the backticks — this only wraps.
fn quote_ident(name: &str) -> Result<String, NexusError> {
    nexus_core::validate_identifier(name).map(|valid| format!("`{valid}`"))
}

/// `SELECT col1, col2, ... FROM table`, columns named (not `*`) and in
/// `fields` order so the resulting `RecordBatch` schema matches `fields`
/// exactly regardless of the table's actual column order.
pub fn build_select_sql(table: &str, fields: &[MySqlCdcFieldSpec]) -> Result<String, NexusError> {
    let table = quote_ident(table)?;
    let columns = fields
        .iter()
        .map(|f| quote_ident(&f.name))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!("SELECT {columns} FROM {table}"))
}

/// `INSERT INTO table (...) VALUES (?, ...) ON DUPLICATE KEY UPDATE
/// col = VALUES(col), ...` — every non-PK field is re-assigned on conflict,
/// making this an unconditional upsert (last write wins), same semantics as
/// every other bridging sink's `replace`/`ON CONFLICT DO UPDATE`.
///
/// `fields` is the connector's full declared column list — same convention
/// as `mysql-cdc`'s positional field spec, it already includes the primary
/// key as an ordinary entry, so this doesn't bind it a second time.
pub fn build_upsert_sql(
    table: &str,
    primary_key: &str,
    fields: &[MySqlCdcFieldSpec],
) -> Result<String, NexusError> {
    let table = quote_ident(table)?;
    let mut columns = Vec::with_capacity(fields.len());
    let mut updates = Vec::with_capacity(fields.len());
    for f in fields {
        let quoted = quote_ident(&f.name)?;
        columns.push(quoted.clone());
        if f.name != primary_key {
            updates.push(format!("{quoted} = VALUES({quoted})"));
        }
    }
    let placeholders = vec!["?"; columns.len()].join(", ");
    let columns_sql = columns.join(", ");
    let updates_sql = updates.join(", ");
    Ok(format!(
        "INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) \
         ON DUPLICATE KEY UPDATE {updates_sql}"
    ))
}

/// Arrow-ceiling type -> MySQL column type. `tinyint(1)` is the MySQL
/// convention for boolean (see `source::mysql_type_to_data_type`'s inverse
/// mapping for the same convention read back on the source side).
fn mysql_column_type(data_type: MySqlCdcDataType) -> &'static str {
    match data_type {
        MySqlCdcDataType::Int64 => "BIGINT",
        MySqlCdcDataType::Float64 => "DOUBLE",
        MySqlCdcDataType::Boolean => "TINYINT(1)",
        MySqlCdcDataType::Utf8 => "TEXT",
    }
}

/// `CREATE TABLE IF NOT EXISTS` from the connector's declared `fields` — a
/// target table that doesn't exist yet is created instead of the sink
/// failing outright on the first upsert with a bare "table doesn't exist".
/// A table that already exists is left alone (no reconciliation), same
/// posture as `nexus-connector-postgres`/`nexus-connector-sqlite`'s
/// equivalent.
pub fn build_create_table_sql(
    table: &str,
    primary_key: &str,
    fields: &[MySqlCdcFieldSpec],
) -> Result<String, NexusError> {
    let quoted_table = quote_ident(table)?;
    let columns = fields
        .iter()
        .map(|f| {
            let quoted_name = quote_ident(&f.name)?;
            let sql_type = mysql_column_type(f.data_type);
            let pk_suffix = if f.name == primary_key {
                " PRIMARY KEY"
            } else {
                ""
            };
            Ok(format!("{quoted_name} {sql_type}{pk_suffix}"))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {quoted_table} ({})",
        columns.join(", ")
    ))
}

/// `DELETE FROM table WHERE pk = ?`.
pub fn build_delete_sql(table: &str, primary_key: &str) -> Result<String, NexusError> {
    let table = quote_ident(table)?;
    let quoted_pk = quote_ident(primary_key)?;
    Ok(format!("DELETE FROM {table} WHERE {quoted_pk} = ?"))
}

/// Converts one `mysql_async::Row` into a JSON object keyed by `fields`'
/// names, so it can be handed to `RecordBatchBuilder::from_json_rows`
/// (same bridging pattern as `mongodb`'s bson->json step).
pub fn row_to_json(row: &Row, fields: &[MySqlCdcFieldSpec]) -> Result<JsonValue, NexusError> {
    let mut obj = serde_json::Map::with_capacity(fields.len());
    for f in fields {
        let value = match f.data_type {
            MySqlCdcDataType::Int64 => get_opt::<Option<i64>>(row, &f.name)?
                .map(JsonValue::from)
                .unwrap_or(JsonValue::Null),
            MySqlCdcDataType::Float64 => get_opt::<Option<f64>>(row, &f.name)?
                .and_then(serde_json::Number::from_f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            MySqlCdcDataType::Boolean => get_opt::<Option<bool>>(row, &f.name)?
                .map(JsonValue::from)
                .unwrap_or(JsonValue::Null),
            MySqlCdcDataType::Utf8 => get_opt::<Option<String>>(row, &f.name)?
                .map(JsonValue::from)
                .unwrap_or(JsonValue::Null),
        };
        obj.insert(f.name.clone(), value);
    }
    Ok(JsonValue::Object(obj))
}

fn get_opt<T>(row: &Row, name: &str) -> Result<T, NexusError>
where
    T: mysql_async::prelude::FromValue,
{
    row.get_opt::<T, &str>(name)
        .ok_or_else(|| NexusError::Schema(format!("column '{name}' not found in MySQL row")))?
        .map_err(|e| {
            NexusError::Schema(format!("column '{name}' has an unexpected MySQL type: {e}"))
        })
}

/// One row's worth of `exec_batch` parameters, in `fields` order — matching
/// `build_upsert_sql`'s column list (which is built from the same `fields`,
/// primary key included as an ordinary entry).
pub fn batch_to_upsert_params(
    batch: &RecordBatch,
    fields: &[MySqlCdcFieldSpec],
) -> Result<Vec<Vec<MySqlValue>>, NexusError> {
    let field_indices = fields
        .iter()
        .map(|f| {
            let idx = batch
                .schema()
                .index_of(&f.name)
                .map_err(|_| NexusError::Schema(format!("column '{}' not found", f.name)))?;
            Ok((idx, f.data_type))
        })
        .collect::<Result<Vec<_>, NexusError>>()?;

    (0..batch.num_rows())
        .map(|row| {
            field_indices
                .iter()
                .map(|&(idx, data_type)| arrow_value_at(batch.column(idx), row, data_type))
                .collect()
        })
        .collect()
}

/// One row's worth of `exec_batch` parameters for the `DELETE ... WHERE pk =
/// ?` statement — `batch` is already projected down to just the PK column
/// (via `nexus_core::project_column`).
pub fn batch_to_delete_params(
    batch: &RecordBatch,
    primary_key: &str,
) -> Result<Vec<Vec<MySqlValue>>, NexusError> {
    let pk_idx = batch
        .schema()
        .index_of(primary_key)
        .map_err(|_| NexusError::Schema(format!("primary key column '{primary_key}' not found")))?;
    let pk_type = arrow_data_type_of(batch, pk_idx)?;
    (0..batch.num_rows())
        .map(|row| Ok(vec![arrow_value_at(batch.column(pk_idx), row, pk_type)?]))
        .collect()
}

fn arrow_data_type_of(batch: &RecordBatch, idx: usize) -> Result<MySqlCdcDataType, NexusError> {
    match batch.schema().field(idx).data_type() {
        arrow_schema::DataType::Int64 => Ok(MySqlCdcDataType::Int64),
        arrow_schema::DataType::Float64 => Ok(MySqlCdcDataType::Float64),
        arrow_schema::DataType::Boolean => Ok(MySqlCdcDataType::Boolean),
        arrow_schema::DataType::Utf8 => Ok(MySqlCdcDataType::Utf8),
        other => Err(NexusError::Schema(format!(
            "unsupported column type '{other:?}' for MySQL — only Int64/Float64/Boolean/Utf8 \
             are supported"
        ))),
    }
}

fn arrow_value_at(
    column: &dyn Array,
    row: usize,
    data_type: MySqlCdcDataType,
) -> Result<MySqlValue, NexusError> {
    if column.is_null(row) {
        return Ok(MySqlValue::NULL);
    }
    Ok(match data_type {
        MySqlCdcDataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| NexusError::Schema("column is not Int64".into()))?;
            MySqlValue::from(arr.value(row))
        }
        MySqlCdcDataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| NexusError::Schema("column is not Float64".into()))?;
            MySqlValue::from(arr.value(row))
        }
        MySqlCdcDataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| NexusError::Schema("column is not Boolean".into()))?;
            MySqlValue::from(arr.value(row))
        }
        MySqlCdcDataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| NexusError::Schema("column is not Utf8".into()))?;
            MySqlValue::from(arr.value(row))
        }
    })
}
