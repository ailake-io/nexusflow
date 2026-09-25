use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{quote_identifier, NexusError};
use odbc_api::{Connection, Cursor};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct TeradataColumn {
    pub name: String,
    pub arrow_type: DataType,
}

/// Describes a table via Teradata's own catalog view (`DBC.ColumnsV`,
/// real/documented — Teradata Database Data Dictionary), same
/// approach the Oracle/HANA connectors' `describe_table` (this repo)
/// use. Filters by `TableName` only (not `DatabaseName`) — the
/// connection's default database (set via the `Database=` ODBC
/// option in `driver.rs`) resolves it, same simplification the HANA
/// connector accepts.
///
/// The table name is validated through `quote_identifier` (rejects
/// anything outside `[A-Za-z_][A-Za-z0-9_]*`) and embedded uppercased
/// as a string literal rather than bound as an ODBC parameter — same
/// safe-by-construction approach every ODBC connector in this repo
/// uses.
pub(crate) fn describe_table(
    conn: &Connection<'_>,
    table: &str,
) -> Result<Vec<TeradataColumn>, NexusError> {
    quote_identifier(table)?;
    let upper = table.to_uppercase();
    let sql = format!(
        "SELECT ColumnName, ColumnType \
         FROM DBC.ColumnsV WHERE TableName = '{upper}' ORDER BY ColumnId"
    );

    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("teradata describe_table query failed: {e}")))?
        .ok_or_else(|| {
            NexusError::Connector("teradata DBC.ColumnsV returned no result set".into())
        })?;

    let mut columns = Vec::new();
    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("teradata describe_table fetch failed: {e}")))?
    {
        let mut name_buf = Vec::new();
        row.get_text(1, &mut name_buf).map_err(|e| {
            NexusError::Connector(format!(
                "teradata describe_table ColumnName read failed: {e}"
            ))
        })?;
        let name = String::from_utf8(name_buf.clone())
            .map_err(|e| NexusError::Serialization(e.to_string()))?
            .trim_end()
            .to_string();

        let mut type_buf = Vec::new();
        row.get_text(2, &mut type_buf).map_err(|e| {
            NexusError::Connector(format!(
                "teradata describe_table ColumnType read failed: {e}"
            ))
        })?;
        let column_type = String::from_utf8(type_buf)
            .map_err(|e| NexusError::Serialization(e.to_string()))?
            .trim()
            .to_string();

        let arrow_type = teradata_type_to_arrow(&column_type)?;
        columns.push(TeradataColumn { name, arrow_type });
    }

    if columns.is_empty() {
        return Err(NexusError::Schema(format!(
            "teradata table {upper} not found in DBC.ColumnsV (or has no columns) — note: \
             unquoted table names are stored uppercase by Teradata, a table created with a \
             quoted lowercase name won't be found"
        )));
    }

    Ok(columns)
}

pub(crate) fn build_schema(columns: &[TeradataColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Maps a `DBC.ColumnsV.ColumnType` 2-char code to an Arrow type — real
/// codes per Teradata's Data Dictionary documentation. No native
/// boolean type in Teradata (commonly emulated with `BYTEINT`/
/// `CHAR(1)`, indistinguishable from a real byte/char column at the
/// catalog level, so not special-cased here — same spirit as Oracle's
/// `NUMBER` needing no separate boolean path). `BF`/`BV`/`BO` (byte/
/// varbyte/blob) are rejected outright — no Arrow binary column type
/// wired up in v1, same as every other ODBC connector in this repo.
fn teradata_type_to_arrow(column_type: &str) -> Result<DataType, NexusError> {
    match column_type {
        "I1" | "I2" | "I" | "I8" => Ok(DataType::Int64),
        "F" | "D" | "N" => Ok(DataType::Float64),
        "CF" | "CV" | "CO" => Ok(DataType::Utf8),
        "DA" | "AT" | "TS" | "SZ" | "AZ" => Ok(DataType::Utf8),
        "BF" | "BV" | "BO" => Err(NexusError::Schema(format!(
            "teradata connector does not support binary column type {column_type} yet"
        ))),
        other => Err(NexusError::Schema(format!(
            "teradata connector does not know how to map column type {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_types_map_to_int64() {
        for t in ["I1", "I2", "I", "I8"] {
            assert_eq!(teradata_type_to_arrow(t).unwrap(), DataType::Int64);
        }
    }

    #[test]
    fn decimal_and_float_types_map_to_float64() {
        for t in ["F", "D", "N"] {
            assert_eq!(teradata_type_to_arrow(t).unwrap(), DataType::Float64);
        }
    }

    #[test]
    fn character_types_map_to_utf8() {
        for t in ["CF", "CV", "CO"] {
            assert_eq!(teradata_type_to_arrow(t).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn date_and_time_variants_map_to_utf8() {
        for t in ["DA", "AT", "TS", "SZ", "AZ"] {
            assert_eq!(teradata_type_to_arrow(t).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn binary_types_are_rejected() {
        for t in ["BF", "BV", "BO"] {
            assert!(teradata_type_to_arrow(t).is_err());
        }
    }

    #[test]
    fn unknown_type_is_rejected() {
        assert!(teradata_type_to_arrow("XX").is_err());
    }
}
