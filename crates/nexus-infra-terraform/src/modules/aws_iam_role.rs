//! Wraps `terraform-aws-modules/iam//modules/iam-assumable-role` (the
//! standard community module for "a role any of these AWS services can
//! assume, with these policies attached") — the connective glue almost
//! every other module needs (ECS task execution role, CodeBuild service
//! role, SageMaker execution role, ...).

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "terraform-aws-modules/iam/aws//modules/iam-assumable-role";
const VERSION: &str = "~> 5.48";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Name of the IAM role.
    role_name: String,
    /// AWS services allowed to assume this role, e.g.
    /// ["ecs-tasks.amazonaws.com", "sagemaker.amazonaws.com"].
    trusted_role_services: Vec<String>,
    /// ARNs of managed policies to attach — AWS-managed (e.g.
    /// "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess") or your own.
    #[serde(default)]
    custom_role_policy_arns: Vec<String>,
}

submit_infra_module!(
    "aws-iam-role",
    "IAM Role",
    "iam",
    "aws",
    Config,
    &["iam_role_arn", "iam_role_name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_module_block(id, SOURCE, VERSION, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "iam_role_arn" | "iam_role_name" => Some(format!("module.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
