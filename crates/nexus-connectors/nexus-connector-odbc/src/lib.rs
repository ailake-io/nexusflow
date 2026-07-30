//! Bridging connector for legacy databases via ODBC. Row-wise cursor access
//! converted to `RecordBatch` via `RecordBatchBuilder` — see
//! ARCHITECTURE.md §2/§4.1 and IMPLEMENTATION_PLAN.md Marco 3.
//!
//! `odbc-api` links a driver manager (unixODBC, vendored), a native C
//! dependency, so it's behind the `legacy` Cargo feature (CLAUDE.md §8.5) —
//! building this crate with no features enabled compiles SQL-building and
//! row-mapping logic only, no native linkage.

mod config;
mod row_mapping;
#[cfg(feature = "legacy")]
mod sink;
#[cfg(feature = "legacy")]
mod source;
mod sql;

pub use config::{OdbcConnectorConfig, OdbcDataType, OdbcFieldSpec};
pub use row_mapping::batch_to_json_rows;
#[cfg(feature = "legacy")]
pub use sink::OdbcSink;
#[cfg(feature = "legacy")]
pub use source::OdbcSource;
pub use sql::{build_insert_sql, build_select_sql, build_update_sql, update_param_order};

#[cfg(feature = "legacy")]
nexus_core::submit_connector!("odbc", nexus_core::ConnectorCapability::Bridged);
