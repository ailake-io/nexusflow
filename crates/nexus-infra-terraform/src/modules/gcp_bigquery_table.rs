//! Plain `google_bigquery_table`. `schema` is a raw BigQuery column-JSON
//! string, same passthrough approach as this crate's other raw-JSON
//! fields (`aws-stepfunctions-state-machine`'s ASL, etc).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    table_id: String,
    /// Name (not wired output — BigQuery has none) of the parent
    /// dataset. Pass the same `dataset_id` literal used on a
    /// `gcp-bigquery-dataset` node's config.
    dataset_id: String,
    project: String,
    /// Raw BigQuery column-definition JSON, e.g.
    /// `[{"name":"id","type":"INTEGER","mode":"REQUIRED"}]`.
    schema: String,
}

submit_infra_module!(
    "gcp-bigquery-table",
    "BigQuery Table",
    "data",
    "gcp",
    Config,
    &["table_id", "self_link"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let table_id = get("table_id", &format!("\"{id}\""));
    let dataset_id = get("dataset_id", "\"\"");
    let project = get("project", "\"\"");
    let schema = get("schema", "\"[]\"");

    format!(
        "resource \"google_bigquery_table\" \"{id}\" {{\n  table_id   = {table_id}\n  dataset_id = {dataset_id}\n  project    = {project}\n  schema     = {schema}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "table_id" => Some(format!("google_bigquery_table.{id}.table_id")),
        "self_link" => Some(format!("google_bigquery_table.{id}.self_link")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
