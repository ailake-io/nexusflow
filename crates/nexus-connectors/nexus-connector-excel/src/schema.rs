use crate::config::{ExcelConnectorConfig, ExcelDataType, ExcelFieldSpec};
use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use calamine::DataType as _;
use calamine::{Data, Range};
use nexus_core::NexusError;
use std::sync::Arc;

/// Explicit schema from `ExcelFieldSpec` — same shape as
/// `nexus-connector-csv`'s `build_schema`.
pub(crate) fn build_schema(fields: &[ExcelFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    ExcelDataType::Int64 => DataType::Int64,
                    ExcelDataType::Float64 => DataType::Float64,
                    ExcelDataType::Boolean => DataType::Boolean,
                    ExcelDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn primary_key_or_err(cfg: &ExcelConnectorConfig) -> Result<String, NexusError> {
    cfg.primary_key
        .clone()
        .ok_or_else(|| NexusError::Schema("excel sink requires primary_key".into()))
}

/// Resolves which sheet to use: `sheet_name` wins when set (must match a
/// real sheet), otherwise `sheet_index` (default `0`, the first sheet).
pub(crate) fn resolve_sheet<'a>(
    sheet_names: &'a [String],
    sheet_name: Option<&str>,
    sheet_index: usize,
) -> Result<&'a str, NexusError> {
    if let Some(name) = sheet_name {
        return sheet_names
            .iter()
            .find(|s| s.as_str() == name)
            .map(|s| s.as_str())
            .ok_or_else(|| NexusError::Connector(format!("excel sheet {name:?} not found")));
    }
    sheet_names
        .get(sheet_index)
        .map(|s| s.as_str())
        .ok_or_else(|| {
            NexusError::Connector(format!(
                "excel sheet index {sheet_index} out of range ({} sheets)",
                sheet_names.len()
            ))
        })
}

/// Column names for the sheet: from the header row when `has_header`,
/// otherwise generated as `col_0`, `col_1`, ...
fn column_names(range: &Range<Data>, has_header: bool) -> Vec<String> {
    let width = range.width();
    if has_header {
        if let Some(row) = range.rows().next() {
            return (0..width)
                .map(|i| {
                    row.get(i)
                        .map(cell_display_string)
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| format!("col_{i}"))
                })
                .collect();
        }
    }
    (0..width).map(|i| format!("col_{i}")).collect()
}

#[derive(Clone, Copy, PartialEq)]
enum ColumnKind {
    Unknown,
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl ColumnKind {
    fn widen(self, cell: &Data) -> Self {
        let observed = match cell {
            Data::Int(_) => ColumnKind::Int64,
            Data::Float(_) => ColumnKind::Float64,
            Data::Bool(_) => ColumnKind::Boolean,
            // String, DateTime, Duration, Error and anything else land as
            // text — DateTime is deliberately not its own Arrow type here,
            // see `ExcelDataType`'s doc comment.
            _ => ColumnKind::Utf8,
        };
        match self {
            ColumnKind::Unknown => observed,
            _ if self == observed => self,
            ColumnKind::Int64 if observed == ColumnKind::Float64 => ColumnKind::Float64,
            ColumnKind::Float64 if observed == ColumnKind::Int64 => ColumnKind::Float64,
            _ => ColumnKind::Utf8,
        }
    }

    fn into_arrow(self) -> DataType {
        match self {
            // No non-empty cell seen in the sample — default to a
            // nullable Utf8 column rather than failing the whole read.
            ColumnKind::Unknown => DataType::Utf8,
            ColumnKind::Int64 => DataType::Int64,
            ColumnKind::Float64 => DataType::Float64,
            ColumnKind::Boolean => DataType::Boolean,
            ColumnKind::Utf8 => DataType::Utf8,
        }
    }
}

/// Infers a schema by sampling up to `sample_rows` data rows per column —
/// calamine cells are already typed, unlike raw CSV text, so this doesn't
/// need `ExcelConnectorConfig.fields` to be set. Every inferred field is
/// nullable (only a sample was scanned, never a strict guarantee).
pub(crate) fn infer_schema(range: &Range<Data>, has_header: bool, sample_rows: usize) -> SchemaRef {
    let names = column_names(range, has_header);
    let width = names.len();
    let mut kinds = vec![ColumnKind::Unknown; width];

    let data_rows = range.rows().skip(usize::from(has_header)).take(sample_rows);
    for row in data_rows {
        for (i, kind) in kinds.iter_mut().enumerate() {
            if let Some(cell) = row.get(i) {
                if !matches!(cell, Data::Empty) {
                    *kind = kind.widen(cell);
                }
            }
        }
    }

    Arc::new(Schema::new(
        names
            .into_iter()
            .zip(kinds)
            .map(|(name, kind)| Field::new(name, kind.into_arrow(), true))
            .collect::<Vec<_>>(),
    ))
}

fn cell_display_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(s) => s.clone(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) => f.to_string(),
        Data::Bool(b) => b.to_string(),
        Data::DateTime(_) => cell
            .as_datetime()
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
            .unwrap_or_default(),
        Data::Error(e) => format!("{e:?}"),
        other => format!("{other:?}"),
    }
}

/// Parses `range` into a `RecordBatch` matching `schema` exactly — used by
/// both `ExcelSource` (fresh read) and `ExcelSink` (re-reading the
/// existing file before a read-filter-rewrite, same pattern
/// `nexus-connector-csv`'s `CsvSink` already uses for whole-file sinks).
pub(crate) fn parse_range_to_batch(
    range: &Range<Data>,
    schema: SchemaRef,
    has_header: bool,
) -> Result<RecordBatch, NexusError> {
    let data_rows: Vec<&[Data]> = range.rows().skip(usize::from(has_header)).collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in &data_rows {
                    push_int(&mut builder, row.get(col_idx), field.name(), col_idx)?;
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in &data_rows {
                    push_float(&mut builder, row.get(col_idx), field.name(), col_idx)?;
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in &data_rows {
                    push_bool(&mut builder, row.get(col_idx), field.name(), col_idx)?;
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in &data_rows {
                    push_utf8(&mut builder, row.get(col_idx));
                }
                Arc::new(builder.finish())
            }
            other => {
                return Err(NexusError::Schema(format!(
                    "excel connector does not support arrow type {other:?}"
                )))
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| NexusError::Schema(format!("excel record batch build failed: {e}")))
}

fn push_int(
    builder: &mut Int64Builder,
    cell: Option<&Data>,
    col: &str,
    idx: usize,
) -> Result<(), NexusError> {
    match cell {
        None | Some(Data::Empty) => builder.append_null(),
        Some(c) => match c.get_int().or_else(|| c.get_float().map(|f| f as i64)) {
            Some(v) => builder.append_value(v),
            None => {
                return Err(NexusError::Schema(format!(
                    "column '{col}' (index {idx}) expected int64, got {c:?}"
                )))
            }
        },
    }
    Ok(())
}

fn push_float(
    builder: &mut Float64Builder,
    cell: Option<&Data>,
    col: &str,
    idx: usize,
) -> Result<(), NexusError> {
    match cell {
        None | Some(Data::Empty) => builder.append_null(),
        Some(c) => match c.get_float().or_else(|| c.get_int().map(|i| i as f64)) {
            Some(v) => builder.append_value(v),
            None => {
                return Err(NexusError::Schema(format!(
                    "column '{col}' (index {idx}) expected float64, got {c:?}"
                )))
            }
        },
    }
    Ok(())
}

fn push_bool(
    builder: &mut BooleanBuilder,
    cell: Option<&Data>,
    col: &str,
    idx: usize,
) -> Result<(), NexusError> {
    match cell {
        None | Some(Data::Empty) => builder.append_null(),
        Some(c) => match c.get_bool() {
            Some(v) => builder.append_value(v),
            None => {
                return Err(NexusError::Schema(format!(
                    "column '{col}' (index {idx}) expected boolean, got {c:?}"
                )))
            }
        },
    }
    Ok(())
}

fn push_utf8(builder: &mut StringBuilder, cell: Option<&Data>) {
    match cell {
        None | Some(Data::Empty) => builder.append_null(),
        Some(c) => builder.append_value(cell_display_string(c)),
    }
}
