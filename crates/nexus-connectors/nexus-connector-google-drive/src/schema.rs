use crate::config::{GoogleDriveDataType, GoogleDriveFieldSpec};
use arrow_csv::reader::Format;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::NexusError;
use std::io::Cursor;
use std::sync::Arc;

/// Samples up to `max_records` rows of `bytes` and infers a schema —
/// identical approach to `nexus-connector-csv`'s `schema::infer_schema`
/// (public repo) and `nexus-connector-dropbox`'s copy of it.
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
        .map_err(|e| NexusError::Schema(format!("google-drive schema inference failed: {e}")))?;
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

pub(crate) fn build_schema(fields: &[GoogleDriveFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    GoogleDriveDataType::Int64 => DataType::Int64,
                    GoogleDriveDataType::Float64 => DataType::Float64,
                    GoogleDriveDataType::Boolean => DataType::Boolean,
                    GoogleDriveDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

/// `arrow-csv` binds the delimiter as a single byte — reject anything
/// that isn't plain ASCII, same rule `nexus-connector-csv`'s
/// `schema::delimiter_byte` documents.
pub(crate) fn ascii_byte(c: char, field: &str) -> Result<u8, NexusError> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        Err(NexusError::Schema(format!(
            "{field} {c:?} must be a single ASCII character"
        )))
    }
}
