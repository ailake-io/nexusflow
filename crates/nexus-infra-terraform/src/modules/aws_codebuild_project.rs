//! Plain `aws_codebuild_project` resource. Flattens the real resource's
//! nested `artifacts`/`environment`/`source` blocks into top-level config
//! fields (assuming the common case: no artifacts beyond what CodePipeline
//! itself manages, a CodeBuild-managed Linux image, a Git source) — `emit`
//! reconstructs the real nested shape.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

const RESOURCE_TYPE: &str = "aws_codebuild_project";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Project name.
    name: String,
    /// ARN of the IAM role CodeBuild assumes to run — wire this to an
    /// `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["codebuild.amazonaws.com"]`).
    service_role: String,
    /// CodeBuild-managed build image, e.g. "aws/codebuild/standard:7.0".
    #[serde(default = "default_image")]
    image: String,
    /// Build compute size.
    #[serde(default = "default_compute_type")]
    compute_type: String,
    /// "GITHUB", "CODECOMMIT", "S3", ... — see the `source` block's `type`
    /// argument in the AWS provider docs for the full list.
    #[serde(default = "default_source_type")]
    source_type: String,
    /// Repo URL (for GITHUB/CODECOMMIT) or S3 path (for S3).
    source_location: String,
    /// Inline buildspec YAML — omit to use a `buildspec.yml` committed to
    /// the repo root instead.
    #[serde(default)]
    buildspec: Option<String>,
}

fn default_image() -> String {
    "aws/codebuild/standard:7.0".to_string()
}
fn default_compute_type() -> String {
    "BUILD_GENERAL1_SMALL".to_string()
}
fn default_source_type() -> String {
    "GITHUB".to_string()
}

submit_infra_module!(
    "aws-codebuild-project",
    "CodeBuild Project",
    "cicd",
    "aws",
    Config,
    &["arn", "id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let service_role = get("service_role", "\"\"");
    let image = get("image", "\"aws/codebuild/standard:7.0\"");
    let compute_type = get("compute_type", "\"BUILD_GENERAL1_SMALL\"");
    let source_type = get("source_type", "\"GITHUB\"");
    let source_location = get("source_location", "\"\"");
    let buildspec_line = args
        .get("buildspec")
        .map(|b| format!("\n    buildspec = {b}"))
        .unwrap_or_default();

    format!(
        "resource \"{RESOURCE_TYPE}\" \"{id}\" {{\n  name         = {name}\n  service_role = {service_role}\n\n  artifacts {{\n    type = \"NO_ARTIFACTS\"\n  }}\n\n  environment {{\n    compute_type = {compute_type}\n    image        = {image}\n    type         = \"LINUX_CONTAINER\"\n  }}\n\n  source {{\n    type     = {source_type}\n    location = {source_location}{buildspec_line}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" | "id" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
