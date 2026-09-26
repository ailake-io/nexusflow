use crate::config::ExcelConnectorConfig;
use crate::rows::extract_pk_strings;
use crate::schema::{parse_range_to_batch, primary_key_or_err, resolve_sheet};
use crate::store::open_store;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, SchemaRef};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use calamine::{Reader, Xlsx};
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use rust_xlsxwriter::{Workbook, Worksheet};
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

/// `.xlsx` has no update/delete or append of its own — same whole-file
/// read-filter-rewrite trade-off `nexus-connector-csv`'s `CsvSink` and
/// `nexus-connector-parquet` already make. Unlike `CsvSink`, the working
/// schema isn't fixed at `connect()` time (`ExcelConnectorConfig.fields`
/// is optional here — see `config.rs`): each `write_batch` call derives it
/// from the incoming `RecordBatch` instead, since every batch for a given
/// node config carries the same schema in practice.
pub struct ExcelSink {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    sheet_name: Option<String>,
    sheet_index: usize,
    has_header: bool,
    primary_key: String,
    timeout_seconds: u64,
}

impl ExcelSink {
    pub fn connect(cfg: &ExcelConnectorConfig) -> Result<Self, NexusError> {
        let primary_key = primary_key_or_err(cfg)?;
        let (store, path) = open_store(&cfg.uri()?, &cfg.storage_options())?;
        Ok(Self {
            store,
            path,
            sheet_name: cfg.sheet_name.clone(),
            sheet_index: cfg.sheet_index,
            has_header: cfg.has_header,
            primary_key,
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    async fn read_existing(&self, schema: SchemaRef) -> Result<Option<RecordBatch>, NexusError> {
        let bytes = match self.store.get(&self.path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|e| NexusError::Connector(format!("excel read body failed: {e}")))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(NexusError::Connector(format!("excel get failed: {e}"))),
        };

        let has_header = self.has_header;
        let sheet_name = self.sheet_name.clone();
        let sheet_index = self.sheet_index;

        let batch = tokio::task::spawn_blocking(move || -> Result<RecordBatch, NexusError> {
            let mut workbook: Xlsx<_> = Xlsx::new(Cursor::new(bytes))
                .map_err(|e| NexusError::Connector(format!("excel open failed: {e}")))?;
            let sheet_names = workbook.sheet_names().to_owned();
            let sheet =
                resolve_sheet(&sheet_names, sheet_name.as_deref(), sheet_index)?.to_string();
            let range = workbook
                .worksheet_range(&sheet)
                .map_err(|e| NexusError::Connector(format!("excel sheet read failed: {e}")))?;
            parse_range_to_batch(&range, schema, has_header)
        })
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Some(batch))
    }

    fn write_all(
        &self,
        schema: &SchemaRef,
        row_groups: &[RecordBatch],
    ) -> Result<Vec<u8>, NexusError> {
        let mut workbook = Workbook::new();
        let sheet_title = self.sheet_name.as_deref().unwrap_or("Sheet1");
        let worksheet = workbook
            .add_worksheet()
            .set_name(sheet_title)
            .map_err(|e| NexusError::Connector(format!("excel sheet name failed: {e}")))?;

        let mut row_idx: u32 = 0;
        if self.has_header {
            for (col_idx, field) in schema.fields().iter().enumerate() {
                worksheet
                    .write_string(row_idx, col_idx as u16, field.name())
                    .map_err(|e| {
                        NexusError::Connector(format!("excel header write failed: {e}"))
                    })?;
            }
            row_idx += 1;
        }

        for batch in row_groups {
            if batch.num_rows() == 0 {
                continue;
            }
            for r in 0..batch.num_rows() {
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    write_cell(
                        worksheet,
                        row_idx,
                        col_idx as u16,
                        field.data_type(),
                        batch.column(col_idx),
                        r,
                    )?;
                }
                row_idx += 1;
            }
        }

        workbook
            .save_to_buffer()
            .map_err(|e| NexusError::Connector(format!("excel write failed: {e}")))
    }

    async fn apply(&self, upserts: RecordBatch, deletes: &RecordBatch) -> Result<(), NexusError> {
        if upserts.num_rows() == 0 && deletes.num_rows() == 0 {
            return Ok(());
        }
        let schema = upserts.schema();

        let upsert_pks = if upserts.num_rows() > 0 {
            extract_pk_strings(&upserts, &self.primary_key)?
        } else {
            vec![]
        };
        let delete_pks = if deletes.num_rows() > 0 {
            extract_pk_strings(deletes, &self.primary_key)?
        } else {
            vec![]
        };
        let remove: HashSet<String> = upsert_pks.into_iter().chain(delete_pks).collect();

        let existing = self.read_existing(schema.clone()).await?;

        let mut row_groups = Vec::new();
        if let Some(batch) = existing {
            let filtered = if remove.is_empty() {
                batch
            } else {
                let pk_values = extract_pk_strings(&batch, &self.primary_key)?;
                let keep: Vec<bool> = pk_values.iter().map(|v| !remove.contains(v)).collect();
                filter_record_batch(&batch, &BooleanArray::from(keep))
                    .map_err(|e| NexusError::Schema(format!("excel delete filter failed: {e}")))?
            };
            if filtered.num_rows() > 0 {
                row_groups.push(filtered);
            }
        }
        if upserts.num_rows() > 0 {
            row_groups.push(upserts);
        }

        let bytes = self.write_all(&schema, &row_groups)?;
        with_timeout(self.timeout_seconds, "excel put", async {
            self.store
                .put(&self.path, PutPayload::from(bytes))
                .await
                .map_err(|e| NexusError::Connector(format!("excel put failed: {e}")))
        })
        .await?;
        Ok(())
    }
}

fn write_cell(
    worksheet: &mut Worksheet,
    row: u32,
    col: u16,
    data_type: &DataType,
    column: &ArrayRef,
    row_in_batch: usize,
) -> Result<(), NexusError> {
    if column.is_null(row_in_batch) {
        return Ok(());
    }
    match data_type {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    NexusError::Schema("excel: column declared Int64 is not an Int64Array".into())
                })?;
            worksheet
                .write_number(row, col, arr.value(row_in_batch) as f64)
                .map_err(|e| NexusError::Connector(format!("excel cell write failed: {e}")))?;
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    NexusError::Schema(
                        "excel: column declared Float64 is not a Float64Array".into(),
                    )
                })?;
            worksheet
                .write_number(row, col, arr.value(row_in_batch))
                .map_err(|e| NexusError::Connector(format!("excel cell write failed: {e}")))?;
        }
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    NexusError::Schema(
                        "excel: column declared Boolean is not a BooleanArray".into(),
                    )
                })?;
            worksheet
                .write_boolean(row, col, arr.value(row_in_batch))
                .map_err(|e| NexusError::Connector(format!("excel cell write failed: {e}")))?;
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    NexusError::Schema("excel: column declared Utf8 is not a StringArray".into())
                })?;
            worksheet
                .write_string(row, col, arr.value(row_in_batch))
                .map_err(|e| NexusError::Connector(format!("excel cell write failed: {e}")))?;
        }
        other => {
            return Err(NexusError::Schema(format!(
                "excel connector does not support arrow type {other:?}"
            )))
        }
    }
    Ok(())
}

#[async_trait]
impl Sink for ExcelSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) —
        // split it so deletes are issued as real row removals instead of
        // being silently kept. Plain (non-CDC) batches take the
        // all-upsert path.
        match split_by_opcode(&batch)? {
            None => {
                let empty = batch.slice(0, 0);
                self.apply(batch, &empty).await
            }
            Some(split) => self.apply(split.upserts, &split.deletes).await,
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
