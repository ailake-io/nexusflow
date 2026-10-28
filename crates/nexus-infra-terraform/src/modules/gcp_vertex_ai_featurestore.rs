//! Plain `google_vertex_ai_featurestore` — GCP analog of
//! `aws-sagemaker-feature-group`. Entity types/features (the nested
//! schema) aren't modeled here — create them separately via
//! `google_vertex_ai_featurestore_entitytype`/`...feature` resources,
//! this module only provisions the top-level featurestore.

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
    #[serde(default = "default_location")]
    location: String,
    #[serde(default = "default_fixed_node_count")]
    fixed_node_count: u32,
}

fn default_location() -> String {
    "us-central1".to_string()
}
fn default_fixed_node_count() -> u32 {
    1
}

submit_infra_module!(
    "gcp-vertex-ai-featurestore",
    "Vertex AI Feature Store",
    "ai",
    "gcp",
    Config,
    &["id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1\"");
    let fixed_node_count = get("fixed_node_count", "1");

    format!(
        "resource \"google_vertex_ai_featurestore\" \"{id}\" {{\n  name     = {name}\n  project  = {project}\n  region   = {location}\n\n  online_serving_config {{\n    fixed_node_count = {fixed_node_count}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("google_vertex_ai_featurestore.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
