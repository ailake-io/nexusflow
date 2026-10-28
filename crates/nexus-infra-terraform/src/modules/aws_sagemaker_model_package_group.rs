//! Plain `aws_sagemaker_model_package_group` — the Model Registry
//! grouping a model's versions, referenced by a `aws-sagemaker-pipeline`
//! node's training/registration step.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_sagemaker_model_package_group";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    model_package_group_name: String,
    #[serde(default)]
    model_package_group_description: Option<String>,
}

submit_infra_module!(
    "aws-sagemaker-model-package-group",
    "SageMaker Model Registry Group",
    "ai",
    "aws",
    Config,
    &["arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("{RESOURCE_TYPE}.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
