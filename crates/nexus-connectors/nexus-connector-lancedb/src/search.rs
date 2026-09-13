use crate::config::LanceDbConnectorConfig;
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use nexus_core::{with_timeout, NexusError};

/// Vector similarity search against a LanceDB table
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L5) — separate from `LanceDbSink`
/// (write-only, pipeline sink) since this is a read-only, ad-hoc query path
/// used by `nexus-server`'s `POST /rag/query`, never by the batch pipeline
/// engine. First real read/query capability any vector connector in this
/// repo has (`LanceDbSink`/`QdrantSink`/`PgvectorSink` are all sink-only
/// today — confirmed by inspecting all three crates before writing this).
pub struct LanceDbSearchClient {
    connection: Connection,
    table: String,
    timeout_seconds: u64,
}

impl LanceDbSearchClient {
    pub async fn connect(cfg: &LanceDbConnectorConfig) -> Result<Self, NexusError> {
        let uri = cfg.connection_uri();
        let table = cfg.table_name();

        let connection = with_timeout(cfg.timeout_seconds, "lancedb connect", async {
            lancedb::connect(&uri)
                .execute()
                .await
                .map_err(|e| NexusError::Connector(format!("lancedb connect failed: {e}")))
        })
        .await?;

        Ok(Self {
            connection,
            table,
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    /// Returns the `limit` nearest rows to `query_vector` by distance on
    /// `embedding_column`. An empty `Vec` (not an error) when the table has
    /// no rows yet — same "nothing to find" semantics as a real search
    /// hitting zero matches.
    pub async fn search(
        &self,
        query_vector: Vec<f32>,
        embedding_column: &str,
        limit: usize,
    ) -> Result<Vec<RecordBatch>, NexusError> {
        let table = with_timeout(self.timeout_seconds, "lancedb open_table", async {
            self.connection
                .open_table(&self.table)
                .execute()
                .await
                .map_err(|e| NexusError::Connector(format!("lancedb open_table failed: {e}")))
        })
        .await?;

        let stream = with_timeout(self.timeout_seconds, "lancedb search", async {
            let vector_query = table
                .query()
                .nearest_to(query_vector)
                .map_err(|e| NexusError::Connector(format!("lancedb nearest_to failed: {e}")))?;
            vector_query
                .column(embedding_column)
                .limit(limit)
                .execute()
                .await
                .map_err(|e| NexusError::Connector(format!("lancedb search failed: {e}")))
        })
        .await?;

        stream
            .try_collect()
            .await
            .map_err(|e| NexusError::Connector(format!("lancedb search stream failed: {e}")))
    }
}
