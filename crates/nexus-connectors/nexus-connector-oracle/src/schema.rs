use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{quote_identifier, NexusError};
use odbc_api::{Connection, Cursor, Nullable};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct OracleColumn {
    pub name: String,
    pub arrow_type: DataType,
}

/// Describes a table via Oracle's own catalog view (`ALL_TAB_COLUMNS`)
/// instead of requiring an explicit `fields` list in the config the way
/// `nexus-connector-odbc` (public repo, generic-driver tier) does —
/// Oracle's dialect is known here, so real introspection is reliable.
///
/// The table name is validated through `quote_identifier` (rejects
/// anything outside `[A-Za-z_][A-Za-z0-9_]*`) and then embedded
/// uppercased into the query as a string literal rather than bound as
/// an ODBC parameter — safe specifically because that validation
/// already rules out quotes/semicolons/any injection-capable
/// character, and it sidesteps an unconfirmed detail of `odbc-api`'s
/// text-parameter binding API that wasn't worth guessing at for a
/// literal-safe case.
pub(crate) fn describe_table(
    conn: &Connection<'_>,
    table: &str,
) -> Result<Vec<OracleColumn>, NexusError> {
    quote_identifier(table)?;
    let upper = table.to_uppercase();
    let sql = format!(
        "SELECT column_name, data_type, data_precision, data_scale, nullable \
         FROM all_tab_columns WHERE table_name = '{upper}' ORDER BY column_id"
    );

    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("oracle describe_table query failed: {e}")))?
        .ok_or_else(|| NexusError::Connector("oracle ALL_TAB_COLUMNS returned no result set".into()))?;

    let mut columns = Vec::new();
    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("oracle describe_table fetch failed: {e}")))?
    {
        let mut name_buf = Vec::new();
        row.get_text(1, &mut name_buf)
            .map_err(|e| NexusError::Connector(format!("oracle describe_table column_name read failed: {e}")))?;
        let name = String::from_utf8(name_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let mut type_buf = Vec::new();
        row.get_text(2, &mut type_buf)
            .map_err(|e| NexusError::Connector(format!("oracle describe_table data_type read failed: {e}")))?;
        let data_type = String::from_utf8(type_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let mut precision = Nullable::<i64>::null();
        row.get_data(3, &mut precision)
            .map_err(|e| NexusError::Connector(format!("oracle describe_table data_precision read failed: {e}")))?;

        let mut scale = Nullable::<i64>::null();
        row.get_data(4, &mut scale)
            .map_err(|e| NexusError::Connector(format!("oracle describe_table data_scale read failed: {e}")))?;

        let arrow_type = oracle_type_to_arrow(&data_type, precision.into_opt(), scale.into_opt())?;
        columns.push(OracleColumn { name, arrow_type });
    }

    if columns.is_empty() {
        return Err(NexusError::Schema(format!(
            "oracle table {upper} not found in ALL_TAB_COLUMNS (or has no columns) — note: unquoted \
             table names are stored uppercase by Oracle, a table created with a quoted lowercase \
             name won't be found"
        )));
    }

    Ok(columns)
}

pub(crate) fn build_schema(columns: &[OracleColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Maps an Oracle `ALL_TAB_COLUMNS.data_type` to an Arrow type.
/// `NUMBER` is Oracle's arbitrary-precision decimal — there's no exact
/// Arrow equivalent, so a `NUMBER` with scale `0` and a precision that
/// fits comfortably in an `i64` (≤18 digits) maps to `Int64`; anything
/// wider or with a fractional scale maps to `Float64` (same
/// approximate-fallback spirit BigQuery/Salesforce already document
/// for their own imperfect-but-safe type mappings in this workspace).
/// `RAW`/`BLOB`/`LONG RAW` are rejected outright — no Arrow binary
/// column type is wired up in v1, so silently mangling binary data
/// into text would be worse than a clear error.
fn oracle_type_to_arrow(
    data_type: &str,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Result<DataType, NexusError> {
    match data_type {
        "NUMBER" => Ok(match (precision, scale) {
            (Some(p), Some(0)) if p <= 18 => DataType::Int64,
            _ => DataType::Float64,
        }),
        "VARCHAR2" | "CHAR" | "NVARCHAR2" | "NCHAR" | "CLOB" | "NCLOB" | "LONG" => Ok(DataType::Utf8),
        t if t == "DATE" || t.starts_with("TIMESTAMP") => Ok(DataType::Utf8),
        "RAW" | "BLOB" | "LONG RAW" => Err(NexusError::Schema(format!(
            "oracle connector does not support binary column type {data_type} yet"
        ))),
        other => Err(NexusError::Schema(format!(
            "oracle connector does not know how to map data type {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_with_zero_scale_and_small_precision_is_int64() {
        assert_eq!(oracle_type_to_arrow("NUMBER", Some(10), Some(0)).unwrap(), DataType::Int64);
    }

    #[test]
    fn number_with_fractional_scale_is_float64() {
        assert_eq!(oracle_type_to_arrow("NUMBER", Some(10), Some(2)).unwrap(), DataType::Float64);
    }

    #[test]
    fn number_with_no_precision_is_float64() {
        assert_eq!(oracle_type_to_arrow("NUMBER", None, None).unwrap(), DataType::Float64);
    }

    #[test]
    fn number_wider_than_i64_is_float64() {
        assert_eq!(oracle_type_to_arrow("NUMBER", Some(38), Some(0)).unwrap(), DataType::Float64);
    }

    #[test]
    fn text_types_map_to_utf8() {
        for t in ["VARCHAR2", "CHAR", "NVARCHAR2", "CLOB", "LONG"] {
            assert_eq!(oracle_type_to_arrow(t, None, None).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn date_and_timestamp_variants_map_to_utf8() {
        for t in ["DATE", "TIMESTAMP(6)", "TIMESTAMP(6) WITH TIME ZONE"] {
            assert_eq!(oracle_type_to_arrow(t, None, None).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn binary_types_are_rejected() {
        for t in ["RAW", "BLOB", "LONG RAW"] {
            assert!(oracle_type_to_arrow(t, None, None).is_err());
        }
    }
}
