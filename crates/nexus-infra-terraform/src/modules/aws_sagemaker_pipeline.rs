//! Plain `aws_sagemaker_pipeline`. `pipeline_definition` is a raw
//! SageMaker Pipelines JSON DSL string — this crate doesn't model the
//! pipeline step graph, only passes the text through.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_sagemaker_pipeline";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    pipeline_name: String,
    #[serde(default)]
    pipeline_display_name: Option<String>,
    /// ARN of the role the pipeline assumes to run its steps — wire from
    /// an `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["sagemaker.amazonaws.com"]`).
    role_arn: String,
    /// Raw SageMaker Pipelines JSON DSL.
    pipeline_definition: String,
}

submit_infra_module!(
    "aws-sagemaker-pipeline",
    "SageMaker Pipeline",
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
