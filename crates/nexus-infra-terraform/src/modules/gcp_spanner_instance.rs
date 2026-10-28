//! 2-resource custom emit: `google_spanner_instance` +
//! `google_spanner_database` — globally-distributed relational DB,
//! alongside `gcp-cloud-sql-postgres` (regional relational),
//! `gcp-bigquery-dataset` (warehouse), `gcp-bigtable-instance`
//! (wide-column), and `gcp-firestore-database` (document).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project: String,
    #[serde(default = "default_config")]
    config: String,
    #[serde(default = "default_num_nodes")]
    num_nodes: u32,
    database_name: String,
}

fn default_config() -> String {
    "regional-us-central1".to_string()
}
fn default_num_nodes() -> u32 {
    1
}

submit_infra_module!(
    "gcp-spanner-instance",
    "Spanner Instance",
    "data",
    "gcp",
    Config,
    &["instance_id", "database_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let config = get("config", "\"regional-us-central1\"");
    let num_nodes = get("num_nodes", "1");
    let database_name = get("database_name", &format!("\"{id}-db\""));

    let instance_id = format!("{id}_instance");

    format!(
        "resource \"google_spanner_instance\" \"{instance_id}\" {{\n  name         = \"{id}-instance\"\n  project      = {project}\n  config       = {config}\n  display_name = {name}\n  num_nodes    = {num_nodes}\n}}\n\nresource \"google_spanner_database\" \"{id}\" {{\n  instance = google_spanner_instance.{instance_id}.name\n  project  = {project}\n  name     = {database_name}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "instance_id" => Some(format!("google_spanner_instance.{id}_instance.id")),
        "database_id" => Some(format!("google_spanner_database.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
