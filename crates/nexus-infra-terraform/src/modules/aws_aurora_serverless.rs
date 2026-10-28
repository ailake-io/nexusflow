//! Wraps `terraform-aws-modules/rds-aurora/aws` in Aurora Serverless v2
//! mode. The module's `instances` map is fixed to a single `db.serverless`
//! reader/writer instance here (v1 doesn't expose multi-instance
//! topology as config) — `manage_master_user_password = true` like
//! `aws-rds-postgres`, no plaintext password field.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_module_block, ModuleImpl};

const SOURCE: &str = "terraform-aws-modules/rds-aurora/aws";
const VERSION: &str = "~> 9.10";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    #[serde(default = "default_engine")]
    engine: String,
    #[serde(default = "default_engine_version")]
    engine_version: String,
    #[serde(default = "default_username")]
    master_username: String,
    vpc_id: String,
    /// Subnet ids for the DB subnet group — wire from an `aws-vpc` node's
    /// `private_subnets` output.
    subnets: Vec<String>,
    vpc_security_group_ids: Vec<String>,
    #[serde(default = "default_min_capacity")]
    min_capacity: f64,
    #[serde(default = "default_max_capacity")]
    max_capacity: f64,
}

fn default_engine() -> String {
    "aurora-postgresql".to_string()
}
fn default_engine_version() -> String {
    "15.4".to_string()
}
fn default_username() -> String {
    "app_admin".to_string()
}
fn default_min_capacity() -> f64 {
    0.5
}
fn default_max_capacity() -> f64 {
    4.0
}

submit_infra_module!(
    "aws-aurora-serverless",
    "Aurora Serverless v2",
    "data",
    "aws",
    Config,
    &["cluster_arn", "cluster_endpoint", "cluster_reader_endpoint"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let min_capacity = args
        .get("min_capacity")
        .cloned()
        .unwrap_or_else(|| "0.5".to_string());
    let max_capacity = args
        .get("max_capacity")
        .cloned()
        .unwrap_or_else(|| "4".to_string());

    let mut args = args.clone();
    args.remove("min_capacity");
    args.remove("max_capacity");
    args.insert(
        "serverlessv2_scaling_configuration".to_string(),
        format!("{{\n    min_capacity = {min_capacity}\n    max_capacity = {max_capacity}\n  }}"),
    );
    args.insert(
        "instances".to_string(),
        "{\n    one = {\n      instance_class = \"db.serverless\"\n    }\n  }".to_string(),
    );
    args.entry("manage_master_user_password".to_string())
        .or_insert_with(|| "true".to_string());

    render_module_block(id, SOURCE, VERSION, &args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "cluster_arn" | "cluster_endpoint" | "cluster_reader_endpoint" => {
            Some(format!("module.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
