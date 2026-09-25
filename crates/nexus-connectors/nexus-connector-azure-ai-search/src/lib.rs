//! Azure AI Search connector — twenty-first crate, fifth and last
//! vector/search item of the tier-6 group. `Bridged` (REST + JSON, no
//! ADBC/ODBC driver exists for this). Sink only — mirrors
//! `nexus-connector-weaviate`/`elasticsearch` (this repo): the index
//! must already exist, this connector only writes documents.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod rows;
mod sink;

pub use config::AzureAiSearchConnectorConfig;
pub use sink::AzureAiSearchSink;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink};

nexus_core::submit_connector!(
    "azure-ai-search",
    ConnectorCapability::Bridged,
    AzureAiSearchConnectorConfig
);

fn validate_azure_ai_search_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<AzureAiSearchConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_sink_builder!(
    "azure-ai-search",
    validate_azure_ai_search_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: AzureAiSearchConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = AzureAiSearchSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
