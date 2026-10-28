//! Wraps `terraform-aws-modules/rds/aws` (widely-adopted community
//! module) for a single-instance Postgres RDS database. Uses
//! `manage_master_user_password = true` (IAM-managed Secrets Manager
//! password) by default so no plaintext password field exists in this
//! module's config at all.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_module_block, ModuleImpl};

const SOURCE: &str = "terraform-aws-modules/rds/aws";
const VERSION: &str = "~> 6.5";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// DB instance identifier.
    identifier: String,
    #[serde(default = "default_engine")]
    engine: String,
    #[serde(default = "default_engine_version")]
    engine_version: String,
    #[serde(default = "default_instance_class")]
    instance_class: String,
    #[serde(default = "default_allocated_storage")]
    allocated_storage: u32,
    /// Initial database name created in the instance.
    db_name: String,
    /// Master username (password is IAM/Secrets-Manager-managed, see
    /// module doc comment above — never set as plaintext here).
    #[serde(default = "default_username")]
    username: String,
    /// Subnet ids for the DB subnet group — wire from an `aws-vpc` node's
    /// `private_subnets` output.
    subnet_ids: Vec<String>,
    /// Security group ids attached to the instance.
    vpc_security_group_ids: Vec<String>,
}

fn default_engine() -> String {
    "postgres".to_string()
}
fn default_engine_version() -> String {
    "16".to_string()
}
fn default_instance_class() -> String {
    "db.t4g.micro".to_string()
}
fn default_allocated_storage() -> u32 {
    20
}
fn default_username() -> String {
    "app_admin".to_string()
}

submit_infra_module!(
    "aws-rds-postgres",
    "RDS for PostgreSQL",
    "data",
    "aws",
    Config,
    &["db_instance_endpoint", "db_instance_arn", "db_instance_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let mut args = args.clone();
    args.entry("manage_master_user_password".to_string())
        .or_insert_with(|| "true".to_string());
    args.entry("family".to_string())
        .or_insert_with(|| "\"postgres16\"".to_string());
    args.entry("create_db_subnet_group".to_string())
        .or_insert_with(|| "true".to_string());
    render_module_block(id, SOURCE, VERSION, &args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "db_instance_endpoint" | "db_instance_arn" | "db_instance_id" => {
            Some(format!("module.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
