//! Wraps `terraform-google-modules/sql-db/google//modules/postgresql`
//! (the standard community module for Cloud SQL) — GCP analog of
//! `aws-rds-postgres`. Uses `random_instance_name = true` by default
//! (module's own recommendation, avoids naming collisions on
//! recreate) rather than a plaintext password field.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_module_block, ModuleImpl};

const SOURCE: &str = "terraform-google-modules/sql-db/google//modules/postgresql";
const VERSION: &str = "~> 25.0";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project_id: String,
    region: String,
    #[serde(default = "default_database_version")]
    database_version: String,
    #[serde(default = "default_tier")]
    tier: String,
    /// VPC self-link for private IP — wire from a `gcp-vpc-network`
    /// node's `network_id` output.
    #[serde(default)]
    private_network: Option<String>,
}

fn default_database_version() -> String {
    "POSTGRES_16".to_string()
}
fn default_tier() -> String {
    "db-custom-1-3840".to_string()
}

submit_infra_module!(
    "gcp-cloud-sql-postgres",
    "Cloud SQL for PostgreSQL",
    "data",
    "gcp",
    Config,
    &[
        "instance_name",
        "instance_connection_name",
        "private_ip_address"
    ]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let mut args = args.clone();
    args.entry("random_instance_name".to_string())
        .or_insert_with(|| "true".to_string());
    render_module_block(id, SOURCE, VERSION, &args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "instance_name" | "instance_connection_name" | "private_ip_address" => {
            Some(format!("module.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
