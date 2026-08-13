//! Bridging connector for generic REST/SaaS APIs. Converts paginated JSON
//! responses into `RecordBatch` via `RecordBatchBuilder` — no ADBC/Flight
//! fast-path exists for arbitrary REST APIs, so this is always `Bridged`.
//! See ARCHITECTURE.md §2/§4.1 and IMPLEMENTATION_PLAN.md Marco 3.

mod config;
mod json_path;
mod rows;
mod sink;
mod source;

pub use config::{
    RestConnectorConfig, RestDataType, RestFieldSpec, RestMethod, RestPagination, WebhookBodyMode,
    WebhookMethod, WebhookSinkConfig,
};
pub use sink::WebhookSink;
pub use source::RestSource;

nexus_core::submit_connector!(
    "rest",
    nexus_core::ConnectorCapability::Bridged,
    RestConnectorConfig
);

// Distinct connector name from "rest" (the paginated-GET source above) —
// see `WebhookSinkConfig`'s doc comment for why they can't share a config
// shape, and `nexus_core::registry`'s `ConnectorDescriptor` for why they
// therefore can't share a catalog entry either (one name -> one schema).
nexus_core::submit_connector!(
    "webhook",
    nexus_core::ConnectorCapability::Bridged,
    WebhookSinkConfig
);
