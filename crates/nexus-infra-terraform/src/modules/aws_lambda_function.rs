//! Plain `aws_lambda_function` resource, container-image packaging (pairs
//! naturally with `aws-ecr-repo` in a CI/CD chain — build an image, push
//! it, point a Lambda at it — instead of the zip-file packaging path,
//! which needs a build artifact this canvas has no notion of).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_lambda_function";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Function name.
    function_name: String,
    /// ARN of the execution role — wire from an `aws-iam-role` node's
    /// `iam_role_arn` output (`trusted_role_services:
    /// ["lambda.amazonaws.com"]`).
    role: String,
    /// Full image URI including tag — wire from an `aws-ecr-repo` node's
    /// `repository_url` output (append your own `:tag`).
    image_uri: String,
    /// Seconds before a timed-out invocation is aborted.
    #[serde(default = "default_timeout")]
    timeout: u32,
    /// Memory (and proportional CPU) allocated, in MB.
    #[serde(default = "default_memory")]
    memory_size: u32,
}

fn default_timeout() -> u32 {
    30
}
fn default_memory() -> u32 {
    512
}

submit_infra_module!(
    "aws-lambda-function",
    "Lambda Function",
    "ai",
    "aws",
    Config,
    &["arn", "function_name", "invoke_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let mut args = args.clone();
    args.insert("package_type".to_string(), "\"Image\"".to_string());
    render_resource_block(RESOURCE_TYPE, id, &args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" | "function_name" | "invoke_arn" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
