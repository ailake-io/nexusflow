//! 2-resource custom emit (namespace + workgroup) — Redshift Serverless
//! has no established community module yet (recent service), so both
//! resources are emitted directly.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    namespace_name: String,
    workgroup_name: String,
    /// Initial database created in the namespace.
    #[serde(default = "default_db_name")]
    db_name: String,
    #[serde(default = "default_admin_username")]
    admin_username: String,
    /// Redshift Processing Units for the workgroup.
    #[serde(default = "default_base_capacity")]
    base_capacity: u32,
    /// Subnet ids the workgroup's ENIs land in — wire from an `aws-vpc`
    /// node's `private_subnets` output.
    subnet_ids: Vec<String>,
    security_group_ids: Vec<String>,
}

fn default_db_name() -> String {
    "dev".to_string()
}
fn default_admin_username() -> String {
    "admin".to_string()
}
fn default_base_capacity() -> u32 {
    8
}

submit_infra_module!(
    "aws-redshift-serverless",
    "Redshift Serverless",
    "data",
    "aws",
    Config,
    &["namespace_arn", "workgroup_arn", "workgroup_endpoint"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let namespace_name = get("namespace_name", &format!("\"{id}-ns\""));
    let workgroup_name = get("workgroup_name", &format!("\"{id}-wg\""));
    let db_name = get("db_name", "\"dev\"");
    let admin_username = get("admin_username", "\"admin\"");
    let base_capacity = get("base_capacity", "8");
    let subnet_ids = get("subnet_ids", "[]");
    let security_group_ids = get("security_group_ids", "[]");

    let namespace_id = format!("{id}_namespace");
    let workgroup_id = format!("{id}_workgroup");

    format!(
        "resource \"aws_redshiftserverless_namespace\" \"{namespace_id}\" {{\n  namespace_name      = {namespace_name}\n  db_name             = {db_name}\n  admin_username      = {admin_username}\n  manage_admin_password = true\n}}\n\nresource \"aws_redshiftserverless_workgroup\" \"{workgroup_id}\" {{\n  namespace_name = aws_redshiftserverless_namespace.{namespace_id}.namespace_name\n  workgroup_name = {workgroup_name}\n  base_capacity  = {base_capacity}\n  subnet_ids         = {subnet_ids}\n  security_group_ids = {security_group_ids}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "namespace_arn" => Some(format!(
            "aws_redshiftserverless_namespace.{id}_namespace.arn"
        )),
        "workgroup_arn" => Some(format!(
            "aws_redshiftserverless_workgroup.{id}_workgroup.arn"
        )),
        "workgroup_endpoint" => Some(format!(
            "aws_redshiftserverless_workgroup.{id}_workgroup.endpoint"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
