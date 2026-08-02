//! Bridging connector for MongoDB. Converts `bson::Document` into
//! `RecordBatch` via `RecordBatchBuilder` — MongoDB has no ADBC/Flight
//! fast-path, so this is always `Bridged`. See ARCHITECTURE.md §2/§4.1 and
//! IMPLEMENTATION_PLAN.md Marco 3.

mod config;
mod rows;
mod sink;
mod source;

pub use config::{MongoConnectorConfig, MongoDataType, MongoFieldSpec};
pub use sink::MongoSink;
pub use source::MongoSource;

nexus_core::submit_connector!(
    "mongodb",
    nexus_core::ConnectorCapability::Bridged,
    MongoConnectorConfig
);
