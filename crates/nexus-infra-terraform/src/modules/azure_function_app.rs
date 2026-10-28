//! 2-resource custom emit: `azurerm_service_plan` (Consumption by
//! default) + `azurerm_linux_function_app`, container-image packaging —
//! Azure analog of `aws-lambda-function`.
//!
//! **Credentials note**: `storage_account_access_key` is a plain config
//! field for v1 simplicity, same pragmatic call as
//! `aws-dms-migration`'s endpoint passwords — wire it from a Key Vault
//! reference instead for anything beyond a disposable test.

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
    /// Backing storage account name — wire from an `azure-storage-account`
    /// node's `name` output.
    storage_account_name: String,
    storage_account_access_key: String,
    /// Full container image URI including tag — wire from an
    /// `azure-container-registry` node's `login_server` output.
    image: String,
    #[serde(default = "default_sku")]
    sku_name: String,
}

fn default_sku() -> String {
    "Y1".to_string()
}

submit_infra_module!(
    "azure-function-app",
    "Function App",
    "cicd",
    "azure",
    Config,
    &["id", "default_hostname"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let storage_account_name = get("storage_account_name", "\"\"");
    let storage_account_access_key = get("storage_account_access_key", "\"\"");
    let image = get("image", "\"\"");
    let sku_name = get("sku_name", "\"Y1\"");

    let plan_id = format!("{id}_plan");

    format!(
        "resource \"azurerm_service_plan\" \"{plan_id}\" {{\n  name                = \"{id}-plan\"\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  os_type             = \"Linux\"\n  sku_name            = {sku_name}\n}}\n\nresource \"azurerm_linux_function_app\" \"{id}\" {{\n  name                       = {name}\n  resource_group_name        = {resource_group_name}\n  location                   = {location}\n  service_plan_id            = azurerm_service_plan.{plan_id}.id\n  storage_account_name       = {storage_account_name}\n  storage_account_access_key = {storage_account_access_key}\n\n  site_config {{\n    application_stack {{\n      docker_image_name = {image}\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "default_hostname" => Some(format!("azurerm_linux_function_app.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
