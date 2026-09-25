//! Amazon Kinesis Data Streams connector — twenty-third crate. First
//! connector in either repo to depend on a real `aws-sdk-*` crate (S3
//! support elsewhere goes through the `object_store` crate's own
//! embedded client instead — see `Cargo.toml`'s comment). `Bridged`.
//!
//! Source + sink: the sink reuses the source's `build_client` (async
//! SDK, `Send`-friendly, no ODBC-style dedicated thread needed) and
//! batches via `PutRecords` — see `sink.rs` doc comment for the partial-
//! failure retry contract.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::{KinesisConnectorConfig, KinesisFieldSpec, StartingPosition};
pub use sink::KinesisSink;
pub use source::KinesisSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "kinesis",
    ConnectorCapability::Bridged,
    KinesisConnectorConfig
);

fn validate_kinesis_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    let parsed: KinesisConnectorConfig = serde_json::from_value(cfg.clone())
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()
}

nexus_core::submit_source_builder!(
    "kinesis",
    validate_kinesis_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: KinesisConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = KinesisSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "kinesis",
    validate_kinesis_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: KinesisConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = KinesisSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
