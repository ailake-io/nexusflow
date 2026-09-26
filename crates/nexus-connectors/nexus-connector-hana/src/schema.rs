use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{quote_identifier, NexusError};
use odbc_api::{Connection, Cursor};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct HanaColumn {
    pub name: String,
    pub arrow_type: DataType,
}

/// Describes a table via HANA's own catalog view (`SYS.TABLE_COLUMNS`,
/// confirmed via SAP Help Portal) instead of requiring an explicit
/// `fields` list — same reasoning as the Oracle connector's
/// `describe_table` (this repo).
///
/// The table name is validated through `quote_identifier` (rejects
/// anything outside `[A-Za-z_][A-Za-z0-9_]*`) and embedded uppercased
/// as a string literal rather than bound as an ODBC parameter — same
/// safe-by-construction approach the Oracle connector uses.
pub(crate) fn describe_table(
    conn: &Connection<'_>,
    table: &str,
) -> Result<Vec<HanaColumn>, NexusError> {
    quote_identifier(table)?;
    let upper = table.to_uppercase();
    let sql = format!(
        "SELECT column_name, data_type_name, scale, is_nullable \
         FROM sys.table_columns WHERE table_name = '{upper}' ORDER BY position"
    );

    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("hana describe_table query failed: {e}")))?
        .ok_or_else(|| {
            NexusError::Connector("hana SYS.TABLE_COLUMNS returned no result set".into())
        })?;

    let mut columns = Vec::new();
    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("hana describe_table fetch failed: {e}")))?
    {
        let mut name_buf = Vec::new();
        row.get_text(1, &mut name_buf).map_err(|e| {
            NexusError::Connector(format!("hana describe_table column_name read failed: {e}"))
        })?;
        let name =
            String::from_utf8(name_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let mut type_buf = Vec::new();
        row.get_text(2, &mut type_buf).map_err(|e| {
            NexusError::Connector(format!(
                "hana describe_table data_type_name read failed: {e}"
            ))
        })?;
        let data_type =
            String::from_utf8(type_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let arrow_type = hana_type_to_arrow(&data_type)?;
        columns.push(HanaColumn { name, arrow_type });
    }

    if columns.is_empty() {
        return Err(NexusError::Schema(format!(
            "hana table {upper} not found in SYS.TABLE_COLUMNS (or has no columns) — note: unquoted \
             table names are stored uppercase by HANA, a table created with a quoted lowercase \
             name won't be found"
        )));
    }

    Ok(columns)
}

pub(crate) fn build_schema(columns: &[HanaColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Maps a HANA `SYS.TABLE_COLUMNS.data_type_name` to an Arrow type.
/// Unlike Oracle's `NUMBER` (a single arbitrary-precision type needing
/// precision/scale inspection to guess Int64 vs Float64), HANA has
/// distinct integer types, so the mapping is direct. `BLOB`/`VARBINARY`
/// are rejected outright — no Arrow binary column type wired up in v1,
/// same spirit as Oracle's `RAW`/`BLOB` rejection.
fn hana_type_to_arrow(data_type: &str) -> Result<DataType, NexusError> {
    match data_type {
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" => Ok(DataType::Int64),
        "DECIMAL" | "DOUBLE" | "REAL" | "FLOAT" => Ok(DataType::Float64),
        "BOOLEAN" => Ok(DataType::Boolean),
        "VARCHAR" | "NVARCHAR" | "CHAR" | "NCHAR" | "CLOB" | "NCLOB" | "TEXT" | "SHORTTEXT" => {
            Ok(DataType::Utf8)
        }
        "DATE" | "TIME" | "TIMESTAMP" | "SECONDDATE" => Ok(DataType::Utf8),
        "BLOB" | "VARBINARY" => Err(NexusError::Schema(format!(
            "hana connector does not support binary column type {data_type} yet"
        ))),
        other => Err(NexusError::Schema(format!(
            "hana connector does not know how to map data type {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_types_map_to_int64() {
        for t in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT"] {
            assert_eq!(hana_type_to_arrow(t).unwrap(), DataType::Int64);
        }
    }

    #[test]
    fn decimal_types_map_to_float64() {
        for t in ["DECIMAL", "DOUBLE", "REAL", "FLOAT"] {
            assert_eq!(hana_type_to_arrow(t).unwrap(), DataType::Float64);
        }
    }

    #[test]
    fn boolean_maps_directly_unlike_oracle() {
        assert_eq!(hana_type_to_arrow("BOOLEAN").unwrap(), DataType::Boolean);
    }

    #[test]
    fn text_types_map_to_utf8() {
        for t in [
            "VARCHAR",
            "NVARCHAR",
            "CHAR",
            "NCHAR",
            "CLOB",
            "NCLOB",
            "TEXT",
            "SHORTTEXT",
        ] {
            assert_eq!(hana_type_to_arrow(t).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn date_and_time_variants_map_to_utf8() {
        for t in ["DATE", "TIME", "TIMESTAMP", "SECONDDATE"] {
            assert_eq!(hana_type_to_arrow(t).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn binary_types_are_rejected() {
        for t in ["BLOB", "VARBINARY"] {
            assert!(hana_type_to_arrow(t).is_err());
        }
    }
}
