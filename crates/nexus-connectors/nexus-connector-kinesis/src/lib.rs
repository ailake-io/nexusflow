//! Amazon Kinesis Data Streams connector — twenty-third crate. First
//! connector in either repo to depend on a real `aws-sdk-*` crate (S3
//! support elsewhere goes through the `object_store` crate's own
//! embedded client instead — see `Cargo.toml`'s comment). `Bridged`.
//! Source only.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod source;

pub use config::{KinesisConnectorConfig, KinesisFieldSpec, StartingPosition};
pub use source::KinesisSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_enterprise_connector!(
    "kinesis",
    ConnectorCapability::Bridged,
    KinesisConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<KinesisConnectorConfig, NexusError> {
    let parsed: KinesisConnectorConfig = serde_json::from_value(cfg)
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_kinesis_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "kinesis",
    validate_kinesis_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = KinesisSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
