//! Wraps `Azure/vnet/azurerm` — the de facto standard community VNet
//! module, mirrors `terraform-aws-modules/vpc/aws`'s role.

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "Azure/vnet/azurerm";
const VERSION: &str = "~> 4.1";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    vnet_name: String,
    resource_group_name: String,
    vnet_location: String,
    /// CIDR blocks for the VNet, e.g. ["10.0.0.0/16"].
    address_space: Vec<String>,
    subnet_names: Vec<String>,
    /// Subnet CIDRs, one per `subnet_names` entry, same order.
    subnet_prefixes: Vec<String>,
}

submit_infra_module!(
    "azure-vnet",
    "Virtual Network",
    "network",
    "azure",
    Config,
    &["vnet_id", "vnet_name", "vnet_subnets", "vnet_address_space"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_module_block(id, SOURCE, VERSION, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "vnet_id" | "vnet_name" | "vnet_subnets" | "vnet_address_space" => {
            Some(format!("module.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
