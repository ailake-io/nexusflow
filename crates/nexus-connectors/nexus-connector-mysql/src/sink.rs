use crate::config::{MySqlCdcFieldSpec, MySqlConnectorConfig};
use crate::rows::{
    batch_to_delete_params, batch_to_multi_upsert_params, build_create_table_sql, build_delete_sql,
    build_multi_upsert_sql, build_upsert_sql, schema_to_fields,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
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
/// Rows per multi-row `INSERT` statement. MySQL's max_allowed_packet is the
/// usual ceiling; 1000 rows per statement keeps the packet small while still
/// amortizing the statement-parse cost across many rows.
const UPSERT_CHUNK_SIZE: usize = 1000;

pub struct MySqlSink {
    conn: Conn,
    table: String,
    upsert_sql: String,
    multi_upsert_sql: String,
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
    pub async fn connect(
        config: &MySqlConnectorConfig,
        schema: &SchemaRef,
    ) -> Result<Self, NexusError> {
        let mut conn = with_timeout(config.timeout_seconds, "mysql connect", async {
            Conn::from_url(config.connection_string())
                .await
                .map_err(|e| NexusError::Connector(format!("mysql connect failed: {e}")))
        })
        .await?;

        // Use the explicit `fields` when provided; otherwise derive the target
        // schema from the incoming Arrow schema so passthrough pipelines from
        // CSV/Parquet/Postgres work without manual column declarations.
        let fields = if config.fields.is_empty() {
            schema_to_fields(schema)
        } else {
            config.fields.clone()
        };

        // Auto-create the target table from the resolved fields if it
        // doesn't exist yet — see `rows::build_create_table_sql`'s doc
        // comment.
        let create_table_sql = build_create_table_sql(&config.table, &config.primary_key, &fields)?;
        with_timeout(config.timeout_seconds, "mysql create table", async {
            conn.query_drop(&create_table_sql)
                .await
                .map_err(|e| NexusError::Connector(format!("create table failed: {e}")))
        })
        .await?;

        Ok(Self {
            conn,
            table: config.table.clone(),
            upsert_sql: build_upsert_sql(&config.table, &config.primary_key, &fields)?,
            multi_upsert_sql: build_multi_upsert_sql(
                &config.table,
                &config.primary_key,
                &fields,
                UPSERT_CHUNK_SIZE,
            )?,
            delete_sql: build_delete_sql(&config.table, &config.primary_key)?,
            primary_key: config.primary_key.clone(),
            fields,
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
        // Multi-row upsert: flatten the batch into chunks and run one
        // INSERT ... VALUES (...), (...), ... ON DUPLICATE KEY UPDATE per
        // chunk. This is dramatically faster than exec_batch with one
        // statement per row.
        let chunks = batch_to_multi_upsert_params(batch, &self.fields, UPSERT_CHUNK_SIZE)?;
        if chunks.is_empty() {
            return Ok(());
        }
        let full_chunk_sql = self.multi_upsert_sql.clone();
        let last_chunk_sql = self.upsert_sql.clone();

        for chunk in chunks {
            let rows_in_chunk = chunk.len() / self.fields.len();
            let sql = if rows_in_chunk == UPSERT_CHUNK_SIZE {
                full_chunk_sql.clone()
            } else {
                // Last (partial) chunk needs a statement with the right number
                // of row groups, or fall back to single-row statements for
                // simplicity when the batch is tiny.
                if rows_in_chunk == 1 {
                    last_chunk_sql.clone()
                } else {
                    build_multi_upsert_sql(
                        &self.table,
                        &self.primary_key,
                        &self.fields,
                        rows_in_chunk,
                    )
                    .map_err(|e| {
                        NexusError::Connector(format!("mysql build chunk sql failed: {e}"))
                    })?
                }
            };
            with_timeout(self.timeout_seconds, "mysql upsert", async {
                self.conn
                    .exec_drop(sql, chunk)
                    .await
                    .map_err(|e| NexusError::Connector(format!("mysql upsert failed: {e}")))
            })
            .await?;
        }
        Ok(())
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
