use crate::config::LanceDbConnectorConfig;
use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use async_trait::async_trait;
use lancedb::connection::Connection;
use nexus_core::{project_column, split_by_opcode, CheckpointCursor, NexusError, Sink};

/// AI Lakehouse sink #3. LanceDB is Arrow-native — batches are handed over
/// with no JSON row conversion, unlike the other bridging sinks. Table
/// schema is established from the first batch written (LanceDB has no
/// separate DDL step). See ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md
/// Marco 5.
pub struct LanceDbSink {
    connection: Connection,
    table: String,
    primary_key: String,
    table_exists: bool,
}

impl LanceDbSink {
    pub async fn connect(cfg: &LanceDbConnectorConfig) -> Result<Self, NexusError> {
        let connection = lancedb::connect(&cfg.uri)
            .execute()
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb connect failed: {e}")))?;
        let table_names = connection
            .table_names()
            .execute()
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb list tables failed: {e}")))?;
        let table_exists = table_names.iter().any(|n| n == &cfg.table);

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            primary_key: cfg.primary_key.clone(),
            table_exists,
        })
    }

    async fn upsert(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        if !self.table_exists {
            self.connection
                .create_table(
                    &self.table,
                    Box::new(reader) as Box<dyn RecordBatchReader + Send>,
                )
                .execute()
                .await
                .map_err(|e| NexusError::Connector(format!("lancedb create_table failed: {e}")))?;
            self.table_exists = true;
            return Ok(());
        }

        let table = self
            .connection
            .open_table(&self.table)
            .execute()
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb open_table failed: {e}")))?;
        let mut merge = table.merge_insert(&[self.primary_key.as_str()]);
        merge.when_matched_update_all(None);
        merge.when_not_matched_insert_all();
        merge
            .execute(Box::new(reader) as Box<dyn RecordBatchReader + Send>)
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb merge_insert failed: {e}")))?;
        Ok(())
    }

    async fn delete(&mut self, batch: &RecordBatch) -> Result<(), NexusError> {
        if !self.table_exists || batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let ids = keys
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                NexusError::Schema(format!(
                    "primary key column '{}' must be Int64",
                    self.primary_key
                ))
            })?;
        let id_list = (0..ids.len())
            .map(|i| ids.value(i).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = format!("{} IN ({id_list})", self.primary_key);

        let table = self
            .connection
            .open_table(&self.table)
            .execute()
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb open_table failed: {e}")))?;
        table
            .delete(&predicate)
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb delete failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Sink for LanceDbSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real deletes instead of being silently
        // upserted. Plain (non-CDC) batches take the unchanged single
        // upsert (merge_insert) path.
        match split_by_opcode(&batch)? {
            None => self.upsert(batch).await,
            Some(split) => {
                self.delete(&split.deletes).await?;
                self.upsert(split.upserts).await?;
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}
