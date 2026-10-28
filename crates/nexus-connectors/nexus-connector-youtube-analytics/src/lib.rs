//! YouTube Analytics connector — seventeenth crate. `Bridged` (REST +
//! JSON, no ADBC/ODBC driver exists for this API). Source only — the
//! Analytics API this connector uses is read-only.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod auth;
mod config;
mod source;

pub use config::YoutubeAnalyticsConnectorConfig;
pub use source::YoutubeAnalyticsSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "youtube-analytics",
    ConnectorCapability::Bridged,
    YoutubeAnalyticsConnectorConfig
);

fn validate_youtube_analytics_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<YoutubeAnalyticsConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "youtube-analytics",
    validate_youtube_analytics_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: YoutubeAnalyticsConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = YoutubeAnalyticsSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
