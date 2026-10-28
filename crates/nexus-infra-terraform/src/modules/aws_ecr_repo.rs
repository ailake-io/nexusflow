//! Wraps `terraform-aws-modules/ecr/aws` — image scanning and lifecycle
//! policy defaults the community module already gets right.

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "terraform-aws-modules/ecr/aws";
const VERSION: &str = "~> 2.3";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Repository name, e.g. "my-app/api".
    repository_name: String,
    /// "MUTABLE" (default, tags like "latest" can be overwritten) or
    /// "IMMUTABLE" (a pushed tag can never be replaced — recommended once
    /// a CI/CD pipeline tags images by commit SHA).
    #[serde(default = "default_mutability")]
    repository_image_tag_mutability: String,
}

fn default_mutability() -> String {
    "MUTABLE".to_string()
}

submit_infra_module!(
    "aws-ecr-repo",
    "ECR Repository",
    "cicd",
    "aws",
    Config,
    &["repository_url", "repository_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_module_block(id, SOURCE, VERSION, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "repository_url" | "repository_arn" => Some(format!("module.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
