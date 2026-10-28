//! 2-resource custom emit: `azurerm_storage_data_lake_gen2_filesystem` +
//! `azurerm_synapse_workspace` — Azure analog of `aws-redshift-serverless`/
//! `gcp-bigquery-dataset`'s data-warehouse role. Entra ID-only admin
//! (`aad_admin` block), matching `azure-sql-database`'s "no plaintext
//! password" choice.

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
    /// ARM id of the backing Data Lake Gen2 storage account — wire from
    /// an `azure-storage-account` node's `id` output (must have
    /// `is_hns_enabled = true`, this module's default).
    storage_account_id: String,
    aad_admin_login_username: String,
    aad_admin_object_id: String,
    aad_admin_tenant_id: String,
}

submit_infra_module!(
    "azure-synapse-workspace",
    "Synapse Workspace",
    "data",
    "azure",
    Config,
    &["id", "connectivity_endpoints"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let storage_account_id = get("storage_account_id", "\"\"");
    let aad_admin_login_username = get("aad_admin_login_username", "\"\"");
    let aad_admin_object_id = get("aad_admin_object_id", "\"\"");
    let aad_admin_tenant_id = get("aad_admin_tenant_id", "\"\"");

    let fs_id = format!("{id}_fs");

    format!(
        "resource \"azurerm_storage_data_lake_gen2_filesystem\" \"{fs_id}\" {{\n  name               = \"synapsefs\"\n  storage_account_id = {storage_account_id}\n}}\n\nresource \"azurerm_synapse_workspace\" \"{id}\" {{\n  name                                 = {name}\n  resource_group_name                  = {resource_group_name}\n  location                             = {location}\n  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.{fs_id}.id\n\n  identity {{\n    type = \"SystemAssigned\"\n  }}\n\n  aad_admin {{\n    login     = {aad_admin_login_username}\n    object_id = {aad_admin_object_id}\n    tenant_id = {aad_admin_tenant_id}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("azurerm_synapse_workspace.{id}.id")),
        "connectivity_endpoints" => Some(format!(
            "azurerm_synapse_workspace.{id}.connectivity_endpoints"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
