use crate::config::PgVectorConnectorConfig;
use crate::rows::{extract_embeddings, row_params};
use crate::sql::{build_bulk_delete_sql, build_bulk_upsert_sql};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{project_column, split_by_opcode, CheckpointCursor, NexusError, Sink};
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

/// AI Lakehouse sink: writes chunk+embedding batches into a pgvector-backed
/// table. Uses multi-row `INSERT ... ON CONFLICT` and `DELETE ... IN (...)`
/// instead of one round-trip per row. See ARCHITECTURE.md §4.3,
/// IMPLEMENTATION_PLAN.md Marco 5.
pub struct PgVectorSink {
    client: tokio_postgres::Client,
    table: String,
    columns: Vec<String>,
    embedding_column: String,
    primary_key: String,
}

impl PgVectorSink {
    /// `columns` are the non-embedding data columns (schema order, primary
    /// key included) — must match every `RecordBatch` passed to
    /// `write_batch`, same contract as the ADBC sinks.
    pub async fn connect(
        cfg: &PgVectorConnectorConfig,
        columns: &[String],
    ) -> Result<Self, NexusError> {
        let (client, connection) = tokio_postgres::connect(&cfg.uri, NoTls)
            .await
            .map_err(|e| NexusError::Connector(format!("pgvector connect failed: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pgvector connection error: {e}");
            }
        });

        Ok(Self {
            client,
            table: cfg.table.clone(),
            columns: columns.to_vec(),
            embedding_column: cfg.embedding_column.clone(),
            primary_key: cfg.primary_key.clone(),
        })
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let embeddings = extract_embeddings(batch, &self.embedding_column)?;
        let mut params: Vec<Box<dyn ToSql + Sync + Send + '_>> =
            Vec::with_capacity(batch.num_rows() * (self.columns.len() + 1));
        for (row, embedding) in embeddings.into_iter().enumerate() {
            params.extend(row_params(batch, &self.columns, row)?);
            params.push(Box::new(embedding));
        }

        let sql = build_bulk_upsert_sql(
            &self.table,
            &self.primary_key,
            &self.columns,
            &self.embedding_column,
            batch.num_rows(),
        )?;
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        self.client
            .execute(&sql, &param_refs)
            .await
            .map_err(|e| NexusError::Connector(format!("pgvector upsert failed: {e}")))?;
        Ok(())
    }

    async fn delete(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, &self.primary_key)?;
        let mut params: Vec<Box<dyn ToSql + Sync + Send + '_>> =
            Vec::with_capacity(keys.num_rows());
        for row in 0..keys.num_rows() {
            params.extend(row_params(
                &keys,
                std::slice::from_ref(&self.primary_key),
                row,
            )?);
        }

        let sql = build_bulk_delete_sql(&self.table, &self.primary_key, params.len())?;
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        self.client
            .execute(&sql, &param_refs)
            .await
            .map_err(|e| NexusError::Connector(format!("pgvector delete failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Sink for PgVectorSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // CDC batches carry an `__opcode` column (ARCHITECTURE.md §5) — split
        // it so deletes are issued as real `DELETE`s instead of being
        // silently upserted. Plain (non-CDC) batches take the unchanged
        // single upsert path.
        match split_by_opcode(&batch)? {
            None => self.upsert(&batch).await,
            Some(split) => {
                self.upsert(&split.upserts).await?;
                self.delete(&split.deletes).await?;
                Ok(())
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the upsert
        // above.
        Ok(())
    }
}
