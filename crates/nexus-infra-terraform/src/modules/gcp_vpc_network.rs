//! Wraps `terraform-google-modules/network/google` (the de facto
//! standard community VPC module for GCP, mirrors
//! `terraform-aws-modules/vpc/aws`'s role on the AWS side).

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "terraform-google-modules/network/google";
const VERSION: &str = "~> 9.0";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Subnet {
    subnet_name: String,
    subnet_ip: String,
    subnet_region: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Name applied to the VPC network.
    network_name: String,
    project_id: String,
    subnets: Vec<Subnet>,
}

submit_infra_module!(
    "gcp-vpc-network",
    "VPC Network",
    "network",
    "gcp",
    Config,
    &["network_id", "network_name", "subnets_ids"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_module_block(id, SOURCE, VERSION, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "network_id" | "network_name" | "subnets_ids" => Some(format!("module.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
