use crate::auth::{authenticate, SalesforceSession};
use crate::bulk::{
    create_job, fetch_failed_results, mark_upload_complete, poll_until_complete, upload_csv,
};
use crate::config::SalesforceConnectorConfig;
use arrow_array::RecordBatch;
use arrow_csv::WriterBuilder;
use async_trait::async_trait;
use nexus_core::{split_by_opcode, with_timeout, CheckpointCursor, NexusError, Sink};

/// Writes via a Bulk API 2.0 **ingest** job with `operation: "upsert"` —
/// v1 only supports upsert (requires `external_id_field`), same
/// decision every other sink in this workspace makes by requiring a
/// `primary_key`. Columns are derived from each incoming `RecordBatch`'s
/// own schema, not passed at `connect()` time — the plugin `SinkBuilder`
/// only carries the config JSON, same standard pattern as
/// Snowflake/BigQuery/Redshift.
pub struct SalesforceSink {
    client: reqwest::Client,
    session: SalesforceSession,
    cfg: SalesforceConnectorConfig,
}

impl SalesforceSink {
    pub async fn connect(cfg: &SalesforceConnectorConfig) -> Result<Self, NexusError> {
        cfg.external_id_field_or_err()?;
        let client = reqwest::Client::new();
        let session = with_timeout(cfg.timeout_seconds, "salesforce connect", async {
            authenticate(&client, cfg).await
        })
        .await?;

        Ok(Self {
            client,
            session,
            cfg: cfg.clone(),
        })
    }

    async fn upsert(&self, batch: &RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut csv_bytes = Vec::new();
        {
            let mut writer = WriterBuilder::new().with_header(true).build(&mut csv_bytes);
            writer
                .write(batch)
                .map_err(|e| NexusError::Schema(format!("salesforce CSV encode failed: {e}")))?;
        }

        let external_id_field = self.cfg.external_id_field_or_err()?;
        let body = serde_json::json!({
            "object": self.cfg.sobject,
            "operation": "upsert",
            "externalIdFieldName": external_id_field,
            "lineEnding": "LF",
            "contentType": "CSV",
        });
        let job = with_timeout(
            self.cfg.timeout_seconds,
            "salesforce create ingest job",
            async { create_job(&self.client, &self.session, &self.cfg, "ingest", &body).await },
        )
        .await?;
        let content_url = job.content_url.clone().ok_or_else(|| {
            NexusError::Connector("salesforce ingest job response had no contentUrl".into())
        })?;

        with_timeout(self.cfg.timeout_seconds, "salesforce upload CSV", async {
            upload_csv(&self.client, &self.session, &content_url, csv_bytes).await
        })
        .await?;
        with_timeout(
            self.cfg.timeout_seconds,
            "salesforce mark upload complete",
            async { mark_upload_complete(&self.client, &self.session, &self.cfg, &job.id).await },
        )
        .await?;

        poll_until_complete(&self.client, &self.session, &self.cfg, "ingest", &job.id).await?;

        let failed_csv = with_timeout(
            self.cfg.timeout_seconds,
            "salesforce fetch failedResults",
            async { fetch_failed_results(&self.client, &self.session, &self.cfg, &job.id).await },
        )
        .await?;
        if has_data_rows(&failed_csv) {
            return Err(NexusError::Connector(format!(
                "salesforce ingest job {} had row failures: {failed_csv}",
                job.id
            )));
        }

        Ok(())
    }
}

/// A CSV response with only a header line (or nothing) means no rows —
/// `failedResults` always returns at least the header even when empty.
fn has_data_rows(csv: &str) -> bool {
    csv.lines().count() > 1
}

#[async_trait]
impl Sink for SalesforceSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        // v1 scope: CDC delete rows are rejected outright rather than
        // silently dropped or half-applied — Bulk API 2.0's delete
        // operation needs the record's real Salesforce `Id`, not an
        // arbitrary external ID, and nothing guarantees a CDC batch
        // carries that column. Documented fast-follow in README.
        match split_by_opcode(&batch)? {
            None => self.upsert(&batch).await,
            Some(split) => {
                if split.deletes.num_rows() > 0 {
                    return Err(NexusError::Connector(
                        "salesforce sink does not support delete opcodes yet (Bulk API 2.0 delete requires the record's real Id, not an external id) — see README".into(),
                    ));
                }
                self.upsert(&split.upserts).await
            }
        }
    }

    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_only_csv_has_no_data_rows() {
        assert!(!has_data_rows("sf__Id,sf__Error\n"));
        assert!(!has_data_rows(""));
    }

    #[test]
    fn csv_with_rows_has_data_rows() {
        assert!(has_data_rows("sf__Id,sf__Error\n001xx,DUPLICATE_VALUE\n"));
    }
}
