//! Plain `aws_sfn_state_machine`. `definition` is a raw ASL JSON string —
//! this crate doesn't model Step Functions' state graph, only passes the
//! text through.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_sfn_state_machine";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// ARN of the role the state machine assumes — wire from an
    /// `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["states.amazonaws.com"]`).
    role_arn: String,
    /// Raw Amazon States Language JSON.
    definition: String,
    #[serde(default = "default_type")]
    #[serde(rename = "type")]
    type_: String,
}

fn default_type() -> String {
    "STANDARD".to_string()
}

submit_infra_module!(
    "aws-stepfunctions-state-machine",
    "Step Functions State Machine",
    "cicd",
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
