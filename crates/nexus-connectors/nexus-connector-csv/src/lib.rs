//! Delimited text file connector (CSV, TSV, or any single-character
//! separator) — source and sink, local disk or cloud object storage
//! (S3/GCS/Azure Blob via `object_store`). See `config.rs` for the field
//! contract; unlike Parquet, plain delimited text carries no type
//! information of its own so `fields` always has to say what each column is.

mod config;
mod rows;
mod schema;
mod sink;
mod source;
mod store;

pub use config::{CsvConnectorConfig, CsvDataType, CsvFieldSpec};
pub use sink::CsvSink;
pub use source::CsvSource;

nexus_core::submit_connector!(
    "csv",
    nexus_core::ConnectorCapability::Bridged,
    CsvConnectorConfig
);
