//! 2-resource custom emit: SageMaker Studio domain + one default user
//! profile — a bare domain with zero user profiles isn't usable, so this
//! module always creates the first one alongside it. Add further
//! `aws_sagemaker_user_profile` resources by hand for additional users;
//! not modeled as a separate catalog module (nothing to wire between
//! nodes for it).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    domain_name: String,
    vpc_id: String,
    /// Subnet ids Studio's ENIs land in — wire from an `aws-vpc` node's
    /// `private_subnets` output.
    subnet_ids: Vec<String>,
    /// ARN of the role Studio apps assume — wire from an `aws-iam-role`
    /// node's `iam_role_arn` output (`trusted_role_services:
    /// ["sagemaker.amazonaws.com"]`).
    execution_role_arn: String,
    #[serde(default = "default_auth_mode")]
    auth_mode: String,
    #[serde(default = "default_network_access")]
    app_network_access_type: String,
    #[serde(default = "default_user_profile_name")]
    default_user_profile_name: String,
}

fn default_auth_mode() -> String {
    "IAM".to_string()
}
fn default_network_access() -> String {
    "PublicInternetOnly".to_string()
}
fn default_user_profile_name() -> String {
    "default-user".to_string()
}

submit_infra_module!(
    "aws-sagemaker-studio-domain",
    "SageMaker Studio Domain",
    "ai",
    "aws",
    Config,
    &["domain_id", "domain_arn", "home_efs_file_system_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let domain_name = get("domain_name", &format!("\"{id}\""));
    let vpc_id = get("vpc_id", "\"\"");
    let subnet_ids = get("subnet_ids", "[]");
    let execution_role_arn = get("execution_role_arn", "\"\"");
    let auth_mode = get("auth_mode", "\"IAM\"");
    let app_network_access_type = get("app_network_access_type", "\"PublicInternetOnly\"");
    let default_user_profile_name = get("default_user_profile_name", "\"default-user\"");

    let domain_id = format!("{id}_domain");

    format!(
        "resource \"aws_sagemaker_domain\" \"{domain_id}\" {{\n  domain_name             = {domain_name}\n  auth_mode               = {auth_mode}\n  vpc_id                  = {vpc_id}\n  subnet_ids              = {subnet_ids}\n  app_network_access_type = {app_network_access_type}\n\n  default_user_settings {{\n    execution_role = {execution_role_arn}\n  }}\n}}\n\nresource \"aws_sagemaker_user_profile\" \"{id}\" {{\n  domain_id         = aws_sagemaker_domain.{domain_id}.id\n  user_profile_name = {default_user_profile_name}\n\n  user_settings {{\n    execution_role = {execution_role_arn}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "domain_id" => Some(format!("aws_sagemaker_domain.{id}_domain.id")),
        "domain_arn" => Some(format!("aws_sagemaker_domain.{id}_domain.arn")),
        "home_efs_file_system_id" => Some(format!(
            "aws_sagemaker_domain.{id}_domain.home_efs_file_system_id"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
