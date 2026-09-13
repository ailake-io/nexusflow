use crate::config::PgVectorConnectorConfig;
use nexus_core::{quote_identifier, with_timeout, NexusError};
use pgvector::Vector;
use tokio_postgres::NoTls;

/// Vector similarity search against a pgvector-backed table
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up — RAG multi-vetor) —
/// read-only, ad-hoc query path used by `nexus-server`'s `POST /rag/query`,
/// separate from `PgVectorSink` (write-only, pipeline sink), same split
/// `LanceDbSearchClient` established for LanceDB (Marco L5). The simplest
/// of the non-LanceDB search clients — just SQL with pgvector's `<->`
/// distance operator, no client SDK of its own to learn.
pub struct PgVectorSearchClient {
    client: tokio_postgres::Client,
    table: String,
    primary_key: String,
    timeout_seconds: u64,
}

impl PgVectorSearchClient {
    pub async fn connect(cfg: &PgVectorConnectorConfig) -> Result<Self, NexusError> {
        let (client, connection) = with_timeout(cfg.timeout_seconds, "pgvector connect", async {
            tokio_postgres::connect(&cfg.connection_string(), NoTls)
                .await
                .map_err(|e| NexusError::Connector(format!("pgvector connect failed: {e}")))
        })
        .await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "pgvector search connection closed with an error");
            }
        });
        Ok(Self {
            client,
            table: cfg.table.clone(),
            primary_key: cfg.primary_key.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    /// Returns the `limit` nearest rows to `query_vector` by `<->` distance
    /// on `embedding_column`, each as `(primary key, text from
    /// `source_column`)`.
    pub async fn search(
        &self,
        query_vector: Vec<f32>,
        embedding_column: &str,
        source_column: &str,
        limit: i64,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let quoted_table = quote_identifier(&self.table)?;
        let quoted_primary_key = quote_identifier(&self.primary_key)?;
        let quoted_embedding_column = quote_identifier(embedding_column)?;
        let quoted_source_column = quote_identifier(source_column)?;
        let sql = format!(
            "SELECT {quoted_primary_key}, {quoted_source_column} FROM {quoted_table} \
             ORDER BY {quoted_embedding_column} <-> $1 LIMIT $2"
        );

        let rows = with_timeout(self.timeout_seconds, "pgvector search", async {
            self.client
                .query(&sql, &[&Vector::from(query_vector), &limit])
                .await
                .map_err(|e| NexusError::Connector(format!("pgvector search failed: {e}")))
        })
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let key: String = match row.try_get::<_, String>(0) {
                    Ok(s) => s,
                    Err(_) => row.get::<_, i64>(0).to_string(),
                };
                let text: String = row.get(1);
                (key, text)
            })
            .collect())
    }
}
