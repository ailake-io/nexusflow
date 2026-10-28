//! Plain `google_bigquery_dataset` — the GCP analog of `aws-glue-catalog`
//! (a namespace tables live in). Pairs with `gcp-bigquery-table` for the
//! actual table schema.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    dataset_id: String,
    project: String,
    #[serde(default = "default_location")]
    location: String,
    #[serde(default)]
    friendly_name: Option<String>,
}

fn default_location() -> String {
    "US".to_string()
}

submit_infra_module!(
    "gcp-bigquery-dataset",
    "BigQuery Dataset",
    "data",
    "gcp",
    Config,
    &["dataset_id", "self_link"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let dataset_id = get("dataset_id", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"US\"");

    let friendly_name_line = args
        .get("friendly_name")
        .map(|f| format!("\n  friendly_name = {f}"))
        .unwrap_or_default();

    format!(
        "resource \"google_bigquery_dataset\" \"{id}\" {{\n  dataset_id = {dataset_id}\n  project    = {project}\n  location   = {location}{friendly_name_line}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "dataset_id" => Some(format!("google_bigquery_dataset.{id}.dataset_id")),
        "self_link" => Some(format!("google_bigquery_dataset.{id}.self_link")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
