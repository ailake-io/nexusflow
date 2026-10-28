//! Plain `aws_glue_job` — a Spark/PySpark ETL job. `script_location` must
//! already exist in S3 (this crate doesn't upload script files) — wire
//! it to a path inside an `aws-s3-data-lake` node's bucket.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// ARN of the role the job assumes — wire from an `aws-iam-role`
    /// node's `iam_role_arn` output (`trusted_role_services:
    /// ["glue.amazonaws.com"]`).
    role_arn: String,
    /// s3:// path to the job's Python/Scala script.
    script_location: String,
    #[serde(default = "default_glue_version")]
    glue_version: String,
    /// "glueetl" (Spark) or "pythonshell" or "gluestreaming".
    #[serde(default = "default_command_name")]
    command_name: String,
    #[serde(default = "default_worker_type")]
    worker_type: String,
    #[serde(default = "default_number_of_workers")]
    number_of_workers: u32,
}

fn default_glue_version() -> String {
    "4.0".to_string()
}
fn default_command_name() -> String {
    "glueetl".to_string()
}
fn default_worker_type() -> String {
    "G.1X".to_string()
}
fn default_number_of_workers() -> u32 {
    2
}

submit_infra_module!(
    "aws-glue-job",
    "Glue ETL Job",
    "data",
    "aws",
    Config,
    &["name", "arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let role_arn = get("role_arn", "\"\"");
    let script_location = get("script_location", "\"\"");
    let glue_version = get("glue_version", "\"4.0\"");
    let command_name = get("command_name", "\"glueetl\"");
    let worker_type = get("worker_type", "\"G.1X\"");
    let number_of_workers = get("number_of_workers", "2");

    format!(
        "resource \"aws_glue_job\" \"{id}\" {{\n  name              = {name}\n  role_arn          = {role_arn}\n  glue_version      = {glue_version}\n  worker_type       = {worker_type}\n  number_of_workers = {number_of_workers}\n\n  command {{\n    name            = {command_name}\n    script_location = {script_location}\n    python_version  = \"3\"\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "name" => Some(format!("aws_glue_job.{id}.name")),
        "arn" => Some(format!("aws_glue_job.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
