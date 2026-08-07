use crate::config::PgVectorConnectorConfig;
use crate::rows::{extract_embeddings, row_params};
use crate::sql::{build_bulk_delete_sql, build_bulk_upsert_sql};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use nexus_core::{project_column, split_by_opcode, CheckpointCursor, NexusError, Sink};
use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, NoTls};

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
                tracing::error!(error = %e, "pgvector connection task exited");
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

    /// Runs against either `Client` or a `Transaction` — `write_batch` needs
    /// both statements (upsert + delete for a CDC batch) to commit or roll
    /// back together (C12), so this takes whatever `GenericClient` the
    /// caller opened. Free function (not `&self`) because the caller holds
    /// its `Transaction` as a mutable borrow of `self.client` for the whole
    /// call — a `&self` method here would conflict with that borrow.
    async fn upsert(
        conn: &impl GenericClient,
        table: &str,
        primary_key: &str,
        columns: &[String],
        embedding_column: &str,
        batch: &RecordBatch,
    ) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let embeddings = extract_embeddings(batch, embedding_column)?;
        let mut params: Vec<Box<dyn ToSql + Sync + Send + '_>> =
            Vec::with_capacity(batch.num_rows() * (columns.len() + 1));
        for (row, embedding) in embeddings.into_iter().enumerate() {
            params.extend(row_params(batch, columns, row)?);
            params.push(Box::new(embedding));
        }

        let sql = build_bulk_upsert_sql(
            table,
            primary_key,
            columns,
            embedding_column,
            batch.num_rows(),
        )?;
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        conn.execute(&sql, &param_refs)
            .await
            .map_err(|e| NexusError::Connector(format!("pgvector upsert failed: {e}")))?;
        Ok(())
    }

    async fn delete(
        conn: &impl GenericClient,
        table: &str,
        primary_key: &str,
        batch: &RecordBatch,
    ) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = project_column(batch, primary_key)?;
        let pk_column = [primary_key.to_string()];
        let mut params: Vec<Box<dyn ToSql + Sync + Send + '_>> =
            Vec::with_capacity(keys.num_rows());
        for row in 0..keys.num_rows() {
            params.extend(row_params(&keys, &pk_column, row)?);
        }

        let sql = build_bulk_delete_sql(table, primary_key, params.len())?;
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        conn.execute(&sql, &param_refs)
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
        // silently upserted. Both statements run inside one transaction (C12):
        // a batch that upserts some rows and fails to delete others must not
        // leave the table half-applied.
        let txn = self
            .client
            .transaction()
            .await
            .map_err(|e| NexusError::Connector(format!("pgvector begin failed: {e}")))?;
        match split_by_opcode(&batch)? {
            None => {
                Self::upsert(
                    &txn,
                    &self.table,
                    &self.primary_key,
                    &self.columns,
                    &self.embedding_column,
                    &batch,
                )
                .await?
            }
            Some(split) => {
                Self::upsert(
                    &txn,
                    &self.table,
                    &self.primary_key,
                    &self.columns,
                    &self.embedding_column,
                    &split.upserts,
                )
                .await?;
                Self::delete(&txn, &self.table, &self.primary_key, &split.deletes).await?;
            }
        }
        txn.commit()
            .await
            .map_err(|e| NexusError::Connector(format!("pgvector commit failed: {e}")))?;
        Ok(())
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        // Persisting the cursor is nexus-server's job — see ARCHITECTURE.md
        // §5. This connector's only idempotency obligation is the upsert
        // above.
        Ok(())
    }
}
