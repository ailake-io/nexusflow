use crate::config::{MySqlCdcFieldSpec, MySqlConnectorConfig};
use crate::rows::{
    batch_to_delete_params, batch_to_upsert_params, build_delete_sql, build_upsert_sql,
};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use nexus_core::{
    project_column, split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink,
};

/// Idempotent by construction: every row is an `INSERT ... ON DUPLICATE KEY
/// UPDATE` upsert keyed on `primary_key`, matching the `Sink` contract in
/// ARCHITECTURE.md §5 (at-least-once delivery, retry-safe writes). A
/// `__opcode = "D"` row (see `nexus_core::split_by_opcode`) is issued as a
/// real `DELETE` instead.
pub struct MySqlSink {
    conn: Conn,
    upsert_sql: String,
    delete_sql: String,
    primary_key: String,
    // Same order `upsert_sql`'s column list was built with (primary key
    // included, as an ordinary entry) — `write_batch`'s incoming
    // `RecordBatch` must be looked up by these names/types rather than
    // trusting its own column order, since MySQL binds `?` positionally.
    fields: Vec<MySqlCdcFieldSpec>,
    timeout_seconds: u64,
}

impl MySqlSink {
    pub async fn connect(config: &MySqlConnectorConfig) -> Result<Self, NexusError> {
        let conn = with_timeout(config.timeout_seconds, "mysql connect", async {
            Conn::from_url(config.connection_string())
                .await
                .map_err(|e| NexusError::Connector(format!("mysql connect failed: {e}")))
        })
        .await?;

        Ok(Self {
            conn,
            upsert_sql: build_upsert_sql(&config.table, &config.primary_key, &config.fields)?,
            delete_sql: build_delete_sql(&config.table, &config.primary_key)?,
            primary_key: config.primary_key.clone(),
            fields: config.fields.clone(),
            timeout_seconds: config.timeout_seconds,
        })
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // A plain (non-CDC) batch has no `__opcode` column — treat every row
        // as an upsert. A CDC-shaped batch (fed through a transform, or from
        // `mysql-cdc` directly) gets split so deletes are issued as real
        // `DELETE`s instead of being silently upserted.
        match split_by_opcode(&batch)? {
            None => self.upsert(&batch).await,
            Some(split) => {
                if split.upserts.num_rows() > 0 {
                    self.upsert(&split.upserts).await?;
                }
                if split.deletes.num_rows() > 0 {
                    let keys = project_column(&split.deletes, &self.primary_key)?;
                    self.delete(&keys).await?;
                }
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

impl MySqlSink {
    async fn upsert(&mut self, batch: &RecordBatch) -> Result<(), NexusError> {
        let params = batch_to_upsert_params(batch, &self.fields)?;
        let sql = self.upsert_sql.clone();
        with_timeout(self.timeout_seconds, "mysql upsert", async {
            self.conn
                .exec_batch(sql, params)
                .await
                .map_err(|e| NexusError::Connector(format!("mysql upsert failed: {e}")))
        })
        .await
    }

    async fn delete(&mut self, keys: &RecordBatch) -> Result<(), NexusError> {
        let params = batch_to_delete_params(keys, &self.primary_key)?;
        let sql = self.delete_sql.clone();
        with_timeout(self.timeout_seconds, "mysql delete", async {
            self.conn
                .exec_batch(sql, params)
                .await
                .map_err(|e| NexusError::Connector(format!("mysql delete failed: {e}")))
        })
        .await
    }
}
