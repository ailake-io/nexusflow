//! Plain `aws_sagemaker_notebook_instance` — the exploration/dev
//! counterpart to `aws-sagemaker-endpoint`'s production serving.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_sagemaker_notebook_instance";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Notebook instance name.
    name: String,
    /// ARN of the role the notebook assumes — wire from an `aws-iam-role`
    /// node's `iam_role_arn` output (`trusted_role_services:
    /// ["sagemaker.amazonaws.com"]`).
    role_arn: String,
    /// Instance size, e.g. "ml.t3.medium".
    #[serde(default = "default_instance_type")]
    instance_type: String,
}

fn default_instance_type() -> String {
    "ml.t3.medium".to_string()
}

submit_infra_module!(
    "aws-sagemaker-notebook",
    "SageMaker Notebook Instance",
    "ai",
    "aws",
    Config,
    &["arn", "url"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" | "url" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
