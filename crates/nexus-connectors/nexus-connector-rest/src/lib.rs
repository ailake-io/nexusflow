//! Bridging connector for generic REST/SaaS APIs. Converts paginated JSON
//! responses into `RecordBatch` via `RecordBatchBuilder` — no ADBC/Flight
//! fast-path exists for arbitrary REST APIs, so this is always `Bridged`.
//! See ARCHITECTURE.md §2/§4.1 and IMPLEMENTATION_PLAN.md Marco 3.

mod config;
mod json_path;
mod source;

pub use config::{RestConnectorConfig, RestDataType, RestFieldSpec, RestPagination};
pub use source::RestSource;

nexus_core::submit_connector!("rest", nexus_core::ConnectorCapability::Bridged);
