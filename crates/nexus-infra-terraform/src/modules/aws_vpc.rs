//! Wraps `terraform-aws-modules/vpc/aws` (the de facto standard community
//! VPC module — thousands of production users, actively maintained) rather
//! than vendoring our own VPC HCL. Field names below match that module's
//! real input variables exactly, so a node's config serializes straight
//! into valid `module` arguments.

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "terraform-aws-modules/vpc/aws";
const VERSION: &str = "~> 5.8";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Name tag applied to the VPC and every child resource.
    name: String,
    /// VPC CIDR block, e.g. "10.0.0.0/16".
    cidr: String,
    /// Availability zones to spread subnets across, e.g.
    /// ["us-east-1a", "us-east-1b"].
    azs: Vec<String>,
    /// Private subnet CIDRs, one per AZ.
    private_subnets: Vec<String>,
    /// Public subnet CIDRs, one per AZ.
    public_subnets: Vec<String>,
    /// Whether to provision a NAT gateway for private-subnet egress —
    /// real money (an hourly + per-GB charge), off by default here even
    /// though the wrapped module defaults it to `false` too, so a config
    /// generated without touching this field never surprises anyone with
    /// a NAT bill.
    #[serde(default)]
    enable_nat_gateway: bool,
}

submit_infra_module!(
    "aws-vpc",
    "VPC",
    "network",
    "aws",
    Config,
    &[
        "vpc_id",
        "private_subnets",
        "public_subnets",
        "public_subnets_cidr_blocks"
    ]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_module_block(id, SOURCE, VERSION, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "vpc_id" | "private_subnets" | "public_subnets" | "public_subnets_cidr_blocks" => {
            Some(format!("module.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
