//! `azurerm_user_assigned_identity` + one `azurerm_role_assignment`
//! resource with `for_each` over `roles` — Azure analog of
//! `aws-iam-role`/`gcp-service-account`'s "principal + attached
//! permissions" pattern.

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
    /// Resource id the roles below are granted on, e.g. a resource
    /// group id or a specific storage account id.
    scope: String,
    /// Built-in or custom role definition names, e.g.
    /// ["Storage Blob Data Contributor"].
    #[serde(default)]
    roles: Vec<String>,
}

submit_infra_module!(
    "azure-managed-identity",
    "Managed Identity",
    "iam",
    "azure",
    Config,
    &["principal_id", "client_id", "id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let scope = get("scope", "\"\"");

    let identity_id = format!("{id}_identity");
    let mut block = format!(
        "resource \"azurerm_user_assigned_identity\" \"{identity_id}\" {{\n  name                = {name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n}}\n"
    );

    if let Some(roles) = args.get("roles") {
        block.push_str(&format!(
            "\nresource \"azurerm_role_assignment\" \"{id}_roles\" {{\n  for_each             = toset({roles})\n  scope                = {scope}\n  role_definition_name = each.value\n  principal_id         = azurerm_user_assigned_identity.{identity_id}.principal_id\n}}\n"
        ));
    }

    block
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "principal_id" | "client_id" | "id" => Some(format!(
            "azurerm_user_assigned_identity.{id}_identity.{output}"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
