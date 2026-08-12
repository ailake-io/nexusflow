use crate::config::{CsvConnectorConfig, CsvDataType, CsvFieldSpec};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::NexusError;
use std::sync::Arc;

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
