//! 3-resource custom emit: `azurerm_key_vault` +
//! `azurerm_application_insights` + `azurerm_machine_learning_workspace`
//! — Azure analog of `aws-sagemaker-studio-domain`/
//! `gcp-vertex-ai-featurestore`'s "provision the whole platform, not
//! just one piece" role. `azurerm_machine_learning_workspace` requires
//! both a Key Vault and Application Insights, so this module bundles all
//! three rather than making the user wire two extra nodes for every ML
//! workspace.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    tenant_id: String,
    /// ARM id of the storage account backing the workspace — wire from
    /// an `azure-storage-account` node's `id` output.
    storage_account_id: String,
    #[serde(default = "default_key_vault_sku")]
    key_vault_sku: String,
}

fn default_key_vault_sku() -> String {
    "standard".to_string()
}

submit_infra_module!(
    "azure-ml-workspace",
    "Azure ML Workspace",
    "ai",
    "azure",
    Config,
    &["id", "discovery_url"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let tenant_id = get("tenant_id", "\"\"");
    let storage_account_id = get("storage_account_id", "\"\"");
    let key_vault_sku = get("key_vault_sku", "\"standard\"");

    let kv_id = format!("{id}_kv");
    let ai_id = format!("{id}_ai");

    format!(
        "resource \"azurerm_key_vault\" \"{kv_id}\" {{\n  name                = \"{id}-kv\"\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  tenant_id           = {tenant_id}\n  sku_name            = {key_vault_sku}\n}}\n\nresource \"azurerm_application_insights\" \"{ai_id}\" {{\n  name                = \"{id}-ai\"\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  application_type    = \"web\"\n}}\n\nresource \"azurerm_machine_learning_workspace\" \"{id}\" {{\n  name                    = {name}\n  resource_group_name     = {resource_group_name}\n  location                = {location}\n  key_vault_id            = azurerm_key_vault.{kv_id}.id\n  application_insights_id = azurerm_application_insights.{ai_id}.id\n  storage_account_id      = {storage_account_id}\n\n  identity {{\n    type = \"SystemAssigned\"\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("azurerm_machine_learning_workspace.{id}.id")),
        "discovery_url" => Some(format!(
            "azurerm_machine_learning_workspace.{id}.discovery_url"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
