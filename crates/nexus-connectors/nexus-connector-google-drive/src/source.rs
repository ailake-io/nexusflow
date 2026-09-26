use crate::client::{download_file, list_files};
use crate::config::GoogleDriveConnectorConfig;
use crate::schema::{ascii_byte, build_schema, infer_schema};
use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, Source};
use std::io::Cursor;

/// Reads one or more delimited text files from a Google Drive folder
/// back as `RecordBatch`es — same "concatenate every file in the
/// folder" semantics as `nexus-connector-csv`'s local-directory mode
/// and `nexus-connector-dropbox`'s folder mode, just fetched via
/// `client::download_file` instead of the filesystem or Dropbox API.
pub struct GoogleDriveSource {
    client: reqwest::Client,
    cfg: GoogleDriveConnectorConfig,
    files: Vec<(String, String)>,
    schema: SchemaRef,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
}

impl GoogleDriveSource {
    pub async fn connect(cfg: &GoogleDriveConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let client = reqwest::Client::new();
        let files = list_files(&client, cfg).await?;
        let delimiter = ascii_byte(cfg.delimiter, "delimiter")?;
        let quote = ascii_byte(cfg.quote, "quote")?;
        let escape = cfg.escape.map(|c| ascii_byte(c, "escape")).transpose()?;

        let schema = if cfg.fields.is_empty() {
            let (first_id, _) = files.first().ok_or_else(|| {
                NexusError::Connector(
                    "google-drive source: no files found to infer schema from".into(),
                )
            })?;
            let sample = download_file(&client, cfg, first_id).await?;
            infer_schema(
                &sample,
                delimiter,
                quote,
                escape,
                cfg.has_header,
                cfg.schema_sample_rows,
            )?
        } else {
            build_schema(&cfg.fields)
        };

        Ok(Self {
            client,
            cfg: cfg.clone(),
            files,
            schema,
            delimiter,
            quote,
            escape,
        })
    }

    async fn read_one(&self, file_id: &str) -> Result<Vec<RecordBatch>, NexusError> {
        let bytes = download_file(&self.client, &self.cfg, file_id).await?;
        let schema = self.schema.clone();
        let delimiter = self.delimiter;
        let quote = self.quote;
        let escape = self.escape;
        let has_header = self.cfg.has_header;
        let batch_size = self.cfg.batch_size;
        let file_id_owned = file_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
            let mut builder = ReaderBuilder::new(schema)
                .with_delimiter(delimiter)
                .with_header(has_header)
                .with_batch_size(batch_size)
                .with_quote(quote);
            if let Some(escape) = escape {
                builder = builder.with_escape(escape);
            }
            let reader = builder.build(Cursor::new(bytes)).map_err(|e| {
                NexusError::Connector(format!(
                    "google-drive reader build '{file_id_owned}' failed: {e}"
                ))
            })?;
            reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
                NexusError::Connector(format!("google-drive parse '{file_id_owned}' failed: {e}"))
            })
        })
        .await
        .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
    }
}

#[async_trait]
impl Source for GoogleDriveSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let mut all_batches = Vec::new();
        for (file_id, _name) in &self.files {
            all_batches.extend(self.read_one(file_id).await?);
        }
        Ok(Box::pin(stream::iter(all_batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
