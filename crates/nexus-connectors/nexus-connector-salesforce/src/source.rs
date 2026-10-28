use crate::auth::{authenticate, SalesforceSession};
use crate::bulk::{create_job, fetch_query_results, poll_until_complete};
use crate::config::SalesforceConnectorConfig;
use crate::describe::describe_sobject;
use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, Source};
use std::io::Cursor;

/// Reads via a Bulk API 2.0 **query** job — SOQL has no `SELECT *`, so
/// `connect()` always runs `describe_sobject` first: it supplies both
/// the Arrow schema and, when `cfg.soql` is unset, the field list used
/// to build a default `SELECT <fields> FROM <sobject>`.
///
/// Whole-result read, no true streaming — Bulk API 2.0 query results
/// come back as paginated CSV, all pages are fetched and concatenated
/// before parsing (see `bulk::fetch_query_results`), same v1
/// simplification the other connectors in this workspace make.
pub struct SalesforceSource {
    client: reqwest::Client,
    session: SalesforceSession,
    cfg: SalesforceConnectorConfig,
    soql: String,
    schema: SchemaRef,
}

impl SalesforceSource {
    pub async fn connect(cfg: &SalesforceConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::new();
        let (session, (schema, field_names)) =
            with_timeout(cfg.timeout_seconds, "salesforce connect", async {
                let session = authenticate(&client, cfg).await?;
                let described = describe_sobject(&client, &session, cfg).await?;
                Ok::<_, NexusError>((session, described))
            })
            .await?;

        let soql = cfg
            .soql
            .clone()
            .unwrap_or_else(|| format!("SELECT {} FROM {}", field_names.join(", "), cfg.sobject));

        Ok(Self {
            client,
            session,
            cfg: cfg.clone(),
            soql,
            schema,
        })
    }
}

#[async_trait]
impl Source for SalesforceSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let body = serde_json::json!({ "operation": "query", "query": self.soql });
        let job = with_timeout(
            self.cfg.timeout_seconds,
            "salesforce create query job",
            async { create_job(&self.client, &self.session, &self.cfg, "query", &body).await },
        )
        .await?;

        poll_until_complete(&self.client, &self.session, &self.cfg, "query", &job.id).await?;

        let csv = with_timeout(
            self.cfg.timeout_seconds,
            "salesforce fetch query results",
            async { fetch_query_results(&self.client, &self.session, &self.cfg, &job.id).await },
        )
        .await?;

        let schema = self.schema.clone();
        let batches =
            tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
                let reader = ReaderBuilder::new(schema)
                    .with_header(true)
                    .build(Cursor::new(csv))
                    .map_err(|e| {
                        NexusError::Connector(format!("salesforce CSV reader build failed: {e}"))
                    })?;
                reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| NexusError::Connector(format!("salesforce CSV parse failed: {e}")))
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
