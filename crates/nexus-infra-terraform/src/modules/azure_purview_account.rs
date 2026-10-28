//! Plain `azurerm_purview_account` — Azure analog of
//! `aws-lakeformation-registration`'s data-governance/catalog role.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "azurerm_purview_account";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_sku_name")]
    sku_name: String,
    #[serde(default = "default_public_network_enabled")]
    public_network_enabled: bool,
}

fn default_sku_name() -> String {
    "Standard_4".to_string()
}
fn default_public_network_enabled() -> bool {
    true
}

submit_infra_module!(
    "azure-purview-account",
    "Purview Account",
    "data",
    "azure",
    Config,
    &["id", "catalog_endpoint"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    render_resource_block(RESOURCE_TYPE, id, args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "catalog_endpoint" => Some(format!("{RESOURCE_TYPE}.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
