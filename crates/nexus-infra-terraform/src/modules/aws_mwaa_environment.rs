//! Plain `aws_mwaa_environment` (Managed Airflow). The source S3 bucket
//! must have versioning enabled (an MWAA requirement) — wire
//! `source_bucket_arn` from an `aws-s3-data-lake` node and enable
//! versioning on it separately (not modeled as a config toggle here,
//! since `aws-s3-data-lake` doesn't expose one yet).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// ARN of the versioned S3 bucket holding `dags/` (and optionally
    /// `plugins.zip`/`requirements.txt`) — wire from an
    /// `aws-s3-data-lake` node's `bucket_arn` output.
    source_bucket_arn: String,
    #[serde(default = "default_dag_s3_path")]
    dag_s3_path: String,
    /// ARN of the role Airflow tasks assume — wire from an
    /// `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["airflow.amazonaws.com",
    /// "airflow-env.amazonaws.com"]`).
    execution_role_arn: String,
    /// At least 2 private subnets in different AZs — wire from an
    /// `aws-vpc` node's `private_subnets` output.
    subnet_ids: Vec<String>,
    security_group_ids: Vec<String>,
    #[serde(default = "default_environment_class")]
    environment_class: String,
    #[serde(default = "default_webserver_access_mode")]
    webserver_access_mode: String,
}

fn default_dag_s3_path() -> String {
    "dags".to_string()
}
fn default_environment_class() -> String {
    "mw1.small".to_string()
}
fn default_webserver_access_mode() -> String {
    "PUBLIC_ONLY".to_string()
}

submit_infra_module!(
    "aws-mwaa-environment",
    "Managed Airflow (MWAA)",
    "data",
    "aws",
    Config,
    &["arn", "webserver_url"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let source_bucket_arn = get("source_bucket_arn", "\"\"");
    let dag_s3_path = get("dag_s3_path", "\"dags\"");
    let execution_role_arn = get("execution_role_arn", "\"\"");
    let subnet_ids = get("subnet_ids", "[]");
    let security_group_ids = get("security_group_ids", "[]");
    let environment_class = get("environment_class", "\"mw1.small\"");
    let webserver_access_mode = get("webserver_access_mode", "\"PUBLIC_ONLY\"");

    format!(
        "resource \"aws_mwaa_environment\" \"{id}\" {{\n  name                  = {name}\n  source_bucket_arn     = {source_bucket_arn}\n  dag_s3_path           = {dag_s3_path}\n  execution_role_arn    = {execution_role_arn}\n  environment_class     = {environment_class}\n  webserver_access_mode = {webserver_access_mode}\n\n  network_configuration {{\n    subnet_ids         = {subnet_ids}\n    security_group_ids = {security_group_ids}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("aws_mwaa_environment.{id}.arn")),
        "webserver_url" => Some(format!("aws_mwaa_environment.{id}.webserver_url")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
