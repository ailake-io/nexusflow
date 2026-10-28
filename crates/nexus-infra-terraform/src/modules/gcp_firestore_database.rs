//! Plain `google_firestore_database` — GCP's app-facing NoSQL document
//! database, the piece missing from the catalog alongside Cloud SQL
//! (relational), BigQuery (warehouse), and Bigtable (wide-column).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    project: String,
    #[serde(default = "default_database_id")]
    database_id: String,
    #[serde(default = "default_location_id")]
    location_id: String,
    /// "FIRESTORE_NATIVE" or "DATASTORE_MODE".
    #[serde(default = "default_type")]
    #[serde(rename = "type")]
    type_: String,
}

fn default_database_id() -> String {
    "(default)".to_string()
}
fn default_location_id() -> String {
    "nam5".to_string()
}
fn default_type() -> String {
    "FIRESTORE_NATIVE".to_string()
}

submit_infra_module!(
    "gcp-firestore-database",
    "Firestore Database",
    "data",
    "gcp",
    Config,
    &["id", "name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let project = get("project", "\"\"");
    let database_id = get("database_id", "\"(default)\"");
    let location_id = get("location_id", "\"nam5\"");
    let type_ = get("type", "\"FIRESTORE_NATIVE\"");

    format!(
        "resource \"google_firestore_database\" \"{id}\" {{\n  project     = {project}\n  name        = {database_id}\n  location_id = {location_id}\n  type        = {type_}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_firestore_database.{id}.id")),
        "name" => Some(format!("google_firestore_database.{id}.name")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
