//! Plain `azurerm_data_factory` — Azure analog of `aws-mwaa-environment`/
//! `gcp-composer-environment`'s ETL-orchestration role (pipelines are
//! authored inside the service itself, not modeled here).

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
}

submit_infra_module!(
    "azure-data-factory",
    "Data Factory",
    "data",
    "azure",
    Config,
    &["id", "identity_principal_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");

    format!(
        "resource \"azurerm_data_factory\" \"{id}\" {{\n  name                = {name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n\n  identity {{\n    type = \"SystemAssigned\"\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("azurerm_data_factory.{id}.id")),
        "identity_principal_id" => Some(format!(
            "azurerm_data_factory.{id}.identity[0].principal_id"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
