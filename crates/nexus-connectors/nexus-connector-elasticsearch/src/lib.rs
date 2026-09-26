//! Elasticsearch/OpenSearch connector — eighteenth crate, first of the
//! tier-6 vector/search group. `Bridged` (REST + JSON, no ADBC/ODBC
//! driver exists for this). Sink only — mirrors
//! `nexus-connector-chromadb`/`pinecone`/`qdrant` (public repo): the
//! index must already exist, this connector only writes rows.
//!
//! Registers **two** catalog names (`"elasticsearch"` and
//! `"opensearch"`) from the same config/sink types — the Bulk API's
//! NDJSON wire format and response shape are confirmed identical
//! between the two (OpenSearch forked from Elasticsearch 7.10 and
//! kept it), same technique `nexus-connector-mssql`'s `"synapse"`
//! registration already uses in this repo.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod rows;
mod sink;

pub use config::ElasticsearchConnectorConfig;
pub use sink::ElasticsearchSink;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink};

fn validate_elasticsearch_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<ElasticsearchConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

fn build_sink(cfg: serde_json::Value) -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
    Box::pin(async move {
        let parsed: ElasticsearchConnectorConfig =
            serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
        let sink = ElasticsearchSink::connect(&parsed).await?;
        Ok(Box::new(sink) as Box<dyn Sink>)
    })
}

nexus_core::submit_connector!(
    "elasticsearch",
    ConnectorCapability::Bridged,
    ElasticsearchConnectorConfig
);
nexus_core::submit_sink_builder!("elasticsearch", validate_elasticsearch_config, build_sink);

nexus_core::submit_connector!(
    "opensearch",
    ConnectorCapability::Bridged,
    ElasticsearchConnectorConfig
);
nexus_core::submit_sink_builder!("opensearch", validate_elasticsearch_config, build_sink);
