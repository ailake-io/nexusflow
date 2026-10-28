//! Shopify connector — fourteenth crate, fifth and last of the
//! tier-5 marketing/ads/payments group. `Bridged` (GraphQL + JSON, no
//! ADBC/ODBC driver exists for this API). Source **and** sink — the
//! only tier-5 connector with a write path, via Shopify's real bulk
//! mutation mechanism.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod bulk;
mod config;
mod sink;
mod source;

pub use config::ShopifyConnectorConfig;
pub use sink::ShopifySink;
pub use source::ShopifySource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "shopify",
    ConnectorCapability::Bridged,
    ShopifyConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<ShopifyConnectorConfig, NexusError> {
    let parsed: ShopifyConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_shopify_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "shopify",
    validate_shopify_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = ShopifySource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "shopify",
    validate_shopify_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = ShopifySink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
