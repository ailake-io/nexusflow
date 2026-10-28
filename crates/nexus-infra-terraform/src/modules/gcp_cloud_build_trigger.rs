//! Plain `google_cloudbuild_trigger`, GitHub-connected — GCP analog of
//! `aws-codebuild-project`. `filename` points at a `cloudbuild.yaml`
//! committed to the repo root, same pattern as CodeBuild's optional
//! `buildspec.yml`.

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
    /// GitHub repo owner.
    owner: String,
    /// GitHub repo name.
    repo_name: String,
    #[serde(default = "default_branch_pattern")]
    branch_pattern: String,
    #[serde(default = "default_filename")]
    filename: String,
}

fn default_branch_pattern() -> String {
    "^main$".to_string()
}
fn default_filename() -> String {
    "cloudbuild.yaml".to_string()
}

submit_infra_module!(
    "gcp-cloud-build-trigger",
    "Cloud Build Trigger",
    "cicd",
    "gcp",
    Config,
    &["trigger_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let owner = get("owner", "\"\"");
    let repo_name = get("repo_name", "\"\"");
    let branch_pattern = get("branch_pattern", "\"^main$\"");
    let filename = get("filename", "\"cloudbuild.yaml\"");

    format!(
        "resource \"google_cloudbuild_trigger\" \"{id}\" {{\n  name    = {name}\n  project = {project}\n\n  github {{\n    owner = {owner}\n    name  = {repo_name}\n\n    push {{\n      branch = {branch_pattern}\n    }}\n  }}\n\n  filename = {filename}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "trigger_id" => Some(format!("google_cloudbuild_trigger.{id}.trigger_id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
