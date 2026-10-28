//! Plain `google_artifact_registry_repository`, Docker format — GCP
//! analog of `aws-ecr-repo`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    repository_id: String,
    project: String,
    #[serde(default = "default_location")]
    location: String,
}

fn default_location() -> String {
    "us-central1".to_string()
}

submit_infra_module!(
    "gcp-artifact-registry-repo",
    "Artifact Registry (Docker)",
    "cicd",
    "gcp",
    Config,
    &["id", "name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let repository_id = get("repository_id", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let location = get("location", "\"us-central1\"");

    format!(
        "resource \"google_artifact_registry_repository\" \"{id}\" {{\n  repository_id = {repository_id}\n  project       = {project}\n  location      = {location}\n  format        = \"DOCKER\"\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "name" => Some(format!("google_artifact_registry_repository.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
