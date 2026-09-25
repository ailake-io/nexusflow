use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::{quote_identifier, NexusError};
use odbc_api::{Connection, Cursor};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct VerticaColumn {
    pub name: String,
    pub arrow_type: DataType,
}

/// Describes a table via Vertica's own catalog view (`v_catalog.columns`,
/// real/documented — Vertica System Tables Reference), same approach
/// every ODBC connector in this repo uses. Unlike Oracle/HANA/
/// Teradata's short type codes, `v_catalog.columns.data_type` returns
/// a full, human-readable type name (optionally with precision, e.g.
/// `"varchar(80)"`), so the mapping below matches by prefix instead of
/// exact code.
///
/// The table name is validated through `quote_identifier` (rejects
/// anything outside `[A-Za-z_][A-Za-z0-9_]*`) and embedded as a
/// string literal rather than bound as an ODBC parameter — same
/// safe-by-construction approach every ODBC connector in this repo
/// uses. Unlike Oracle/HANA/Teradata, Vertica preserves the case a
/// table was created with rather than folding to uppercase, so the
/// name is matched as-given (not uppercased).
pub(crate) fn describe_table(
    conn: &Connection<'_>,
    table: &str,
) -> Result<Vec<VerticaColumn>, NexusError> {
    quote_identifier(table)?;
    let sql = format!(
        "SELECT column_name, data_type \
         FROM v_catalog.columns WHERE table_name = '{table}' ORDER BY ordinal_position"
    );

    let mut cursor = conn
        .execute(&sql, (), None)
        .map_err(|e| NexusError::Connector(format!("vertica describe_table query failed: {e}")))?
        .ok_or_else(|| {
            NexusError::Connector("vertica v_catalog.columns returned no result set".into())
        })?;

    let mut columns = Vec::new();
    while let Some(mut row) = cursor
        .next_row()
        .map_err(|e| NexusError::Connector(format!("vertica describe_table fetch failed: {e}")))?
    {
        let mut name_buf = Vec::new();
        row.get_text(1, &mut name_buf).map_err(|e| {
            NexusError::Connector(format!(
                "vertica describe_table column_name read failed: {e}"
            ))
        })?;
        let name =
            String::from_utf8(name_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let mut type_buf = Vec::new();
        row.get_text(2, &mut type_buf).map_err(|e| {
            NexusError::Connector(format!("vertica describe_table data_type read failed: {e}"))
        })?;
        let data_type =
            String::from_utf8(type_buf).map_err(|e| NexusError::Serialization(e.to_string()))?;

        let arrow_type = vertica_type_to_arrow(&data_type)?;
        columns.push(VerticaColumn { name, arrow_type });
    }

    if columns.is_empty() {
        return Err(NexusError::Schema(format!(
            "vertica table {table} not found in v_catalog.columns (or has no columns)"
        )));
    }

    Ok(columns)
}

pub(crate) fn build_schema(columns: &[VerticaColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Maps a `v_catalog.columns.data_type` string to an Arrow type, by
/// prefix (case-insensitive) since Vertica includes precision/scale
/// inline (e.g. `"numeric(18,4)"`, `"varchar(80)"`). Real type names
/// per the Vertica SQL Reference's "SQL Data Types" page.
/// `binary`/`varbinary`/`long varbinary` are rejected outright — no
/// Arrow binary column type wired up in v1, same as every other ODBC
/// connector in this repo.
fn vertica_type_to_arrow(data_type: &str) -> Result<DataType, NexusError> {
    let lower = data_type.to_lowercase();
    if lower.starts_with("int")
        || lower.starts_with("bigint")
        || lower.starts_with("smallint")
        || lower.starts_with("tinyint")
    {
        Ok(DataType::Int64)
    } else if lower.starts_with("float")
        || lower.starts_with("double precision")
        || lower.starts_with("real")
        || lower.starts_with("numeric")
        || lower.starts_with("decimal")
        || lower.starts_with("number")
    {
        Ok(DataType::Float64)
    } else if lower.starts_with("boolean") {
        Ok(DataType::Boolean)
    } else if lower.starts_with("varchar")
        || lower.starts_with("char")
        || lower.starts_with("long varchar")
        || lower.starts_with("date")
        || lower.starts_with("time")
        || lower.starts_with("timestamp")
        || lower.starts_with("interval")
    {
        Ok(DataType::Utf8)
    } else if lower.starts_with("binary")
        || lower.starts_with("varbinary")
        || lower.starts_with("long varbinary")
    {
        Err(NexusError::Schema(format!(
            "vertica connector does not support binary column type {data_type} yet"
        )))
    } else {
        Err(NexusError::Schema(format!(
            "vertica connector does not know how to map data type {data_type}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_types_map_to_int64() {
        for t in ["int", "bigint", "smallint", "tinyint"] {
            assert_eq!(vertica_type_to_arrow(t).unwrap(), DataType::Int64);
        }
    }

    #[test]
    fn numeric_types_map_to_float64_with_precision_suffix() {
        for t in [
            "float",
            "double precision",
            "numeric(18,4)",
            "decimal(10,2)",
        ] {
            assert_eq!(vertica_type_to_arrow(t).unwrap(), DataType::Float64);
        }
    }

    #[test]
    fn boolean_maps_directly() {
        assert_eq!(vertica_type_to_arrow("boolean").unwrap(), DataType::Boolean);
    }

    #[test]
    fn character_types_map_to_utf8_with_precision_suffix() {
        for t in ["varchar(80)", "char(10)", "long varchar(1024)"] {
            assert_eq!(vertica_type_to_arrow(t).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn date_and_time_variants_map_to_utf8() {
        for t in ["date", "timestamp", "timestamptz", "time"] {
            assert_eq!(vertica_type_to_arrow(t).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn binary_types_are_rejected() {
        for t in ["binary(8)", "varbinary(80)", "long varbinary(1024)"] {
            assert!(vertica_type_to_arrow(t).is_err());
        }
    }
}
