//! 2-resource custom emit: `azurerm_batch_account` +
//! `azurerm_batch_pool` — Azure analog of `aws-emr-cluster`/
//! `gcp-dataproc-cluster`'s parallel-batch-compute role.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    account_name: String,
    pool_name: String,
    resource_group_name: String,
    location: String,
    #[serde(default = "default_vm_size")]
    vm_size: String,
    #[serde(default = "default_node_agent_sku_id")]
    node_agent_sku_id: String,
    #[serde(default = "default_publisher")]
    publisher: String,
    #[serde(default = "default_offer")]
    offer: String,
    #[serde(default = "default_sku")]
    sku: String,
    #[serde(default = "default_target_nodes")]
    target_dedicated_nodes: u32,
}

fn default_vm_size() -> String {
    "Standard_D2s_v3".to_string()
}
fn default_node_agent_sku_id() -> String {
    "batch.node.ubuntu 22.04".to_string()
}
fn default_publisher() -> String {
    "canonical".to_string()
}
fn default_offer() -> String {
    "0001-com-ubuntu-server-jammy".to_string()
}
fn default_sku() -> String {
    "22_04-lts".to_string()
}
fn default_target_nodes() -> u32 {
    1
}

submit_infra_module!(
    "azure-batch-account",
    "Batch Account",
    "data",
    "azure",
    Config,
    &["account_id", "pool_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let account_name = get("account_name", &format!("\"{id}\""));
    let pool_name = get("pool_name", &format!("\"{id}-pool\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let vm_size = get("vm_size", "\"Standard_D2s_v3\"");
    let node_agent_sku_id = get("node_agent_sku_id", "\"batch.node.ubuntu 22.04\"");
    let publisher = get("publisher", "\"canonical\"");
    let offer = get("offer", "\"0001-com-ubuntu-server-jammy\"");
    let sku = get("sku", "\"22_04-lts\"");
    let target_dedicated_nodes = get("target_dedicated_nodes", "1");

    let account_id = format!("{id}_account");

    format!(
        "resource \"azurerm_batch_account\" \"{account_id}\" {{\n  name                = {account_name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  pool_allocation_mode = \"BatchService\"\n}}\n\nresource \"azurerm_batch_pool\" \"{id}\" {{\n  name                = {pool_name}\n  resource_group_name = {resource_group_name}\n  account_name        = azurerm_batch_account.{account_id}.name\n  display_name        = {pool_name}\n  vm_size             = {vm_size}\n  node_agent_sku_id   = {node_agent_sku_id}\n\n  fixed_scale {{\n    target_dedicated_nodes = {target_dedicated_nodes}\n  }}\n\n  storage_image_reference {{\n    publisher = {publisher}\n    offer     = {offer}\n    sku       = {sku}\n    version   = \"latest\"\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "account_id" => Some(format!("azurerm_batch_account.{id}_account.id")),
        "pool_id" => Some(format!("azurerm_batch_pool.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
