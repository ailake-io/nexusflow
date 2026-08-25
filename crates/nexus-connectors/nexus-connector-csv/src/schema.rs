use crate::config::{CsvConnectorConfig, CsvDataType, CsvFieldSpec};
use arrow_csv::reader::Format;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::NexusError;
use std::io::Cursor;
use std::sync::Arc;

/// Samples up to `max_records` rows of `bytes` and infers a schema —
/// source-side fallback for when `fields` is left empty (see
/// `CsvConnectorConfig::fields` doc comment). Narrows whatever `arrow-csv`
/// infers down to this connector's 4 supported types: `Int64`/`Float64`/
/// `Boolean` map straight across, everything else (dates, timestamps,
/// wider int/float widths) falls back to `Utf8` so a value is never
/// dropped for lack of a matching variant — same "never lose the value"
/// principle `build_schema`'s 4-way match already commits to.
pub(crate) fn infer_schema(
    bytes: &[u8],
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    has_header: bool,
    max_records: usize,
) -> Result<SchemaRef, NexusError> {
    let mut format = Format::default()
        .with_header(has_header)
        .with_delimiter(delimiter)
        .with_quote(quote);
    if let Some(escape) = escape {
        format = format.with_escape(escape);
    }
    let (inferred, _records_read) = format
        .infer_schema(Cursor::new(bytes), Some(max_records))
        .map_err(|e| NexusError::Schema(format!("csv schema inference failed: {e}")))?;
    Ok(Arc::new(Schema::new(
        inferred
            .fields()
            .iter()
            .map(|f| {
                let data_type = match f.data_type() {
                    DataType::Int64 => DataType::Int64,
                    DataType::Float64 => DataType::Float64,
                    DataType::Boolean => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                Field::new(f.name(), data_type, true)
            })
            .collect::<Vec<_>>(),
    )))
}

pub(crate) fn build_schema(fields: &[CsvFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    CsvDataType::Int64 => DataType::Int64,
                    CsvDataType::Float64 => DataType::Float64,
                    CsvDataType::Boolean => DataType::Boolean,
                    CsvDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

/// `arrow-csv` binds the delimiter as a single byte — reject anything that
/// isn't plain ASCII up front instead of silently truncating a multi-byte
/// UTF-8 char (e.g. a curly quote) down to its first byte.
pub(crate) fn delimiter_byte(delimiter: char) -> Result<u8, NexusError> {
    if delimiter.is_ascii() {
        Ok(delimiter as u8)
    } else {
        Err(NexusError::Schema(format!(
            "delimiter {delimiter:?} must be a single ASCII character"
        )))
    }
}

/// Same contract as `delimiter_byte` but for the quote / escape characters
/// configured on the CSV reader/writer.
pub(crate) fn quote_byte(quote: char) -> Result<u8, NexusError> {
    if quote.is_ascii() {
        Ok(quote as u8)
    } else {
        Err(NexusError::Schema(format!(
            "quote {quote:?} must be a single ASCII character"
        )))
    }
}

pub(crate) fn primary_key_or_err(cfg: &CsvConnectorConfig) -> Result<String, NexusError> {
    cfg.primary_key
        .clone()
        .ok_or_else(|| NexusError::Schema("csv sink requires primary_key".into()))
}
