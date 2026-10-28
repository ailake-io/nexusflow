//! 2-resource custom emit: `azurerm_mssql_server` + `azurerm_mssql_database`
//! — Azure analog of `aws-rds-postgres`/`gcp-cloud-sql-postgres`.
//! Entra ID (Azure AD)-only authentication by default
//! (`azuread_authentication_only = true`), matching those two modules'
//! "no plaintext password field" choice.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    server_name: String,
    database_name: String,
    resource_group_name: String,
    location: String,
    /// Entra ID user/group/service-principal display name granted SQL
    /// admin.
    aad_admin_login_username: String,
    /// Object id of that same Entra ID principal.
    aad_admin_object_id: String,
    #[serde(default = "default_sku")]
    sku_name: String,
}

fn default_sku() -> String {
    "S0".to_string()
}

submit_infra_module!(
    "azure-sql-database",
    "Azure SQL Database",
    "data",
    "azure",
    Config,
    &["server_fqdn", "database_id"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let server_name = get("server_name", &format!("\"{id}-server\""));
    let database_name = get("database_name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let aad_admin_login_username = get("aad_admin_login_username", "\"\"");
    let aad_admin_object_id = get("aad_admin_object_id", "\"\"");
    let sku_name = get("sku_name", "\"S0\"");

    let server_id = format!("{id}_server");

    format!(
        "resource \"azurerm_mssql_server\" \"{server_id}\" {{\n  name                = {server_name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  version             = \"12.0\"\n\n  azuread_administrator {{\n    login_username              = {aad_admin_login_username}\n    object_id                   = {aad_admin_object_id}\n    azuread_authentication_only = true\n  }}\n}}\n\nresource \"azurerm_mssql_database\" \"{id}\" {{\n  name      = {database_name}\n  server_id = azurerm_mssql_server.{server_id}.id\n  sku_name  = {sku_name}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "server_fqdn" => Some(format!(
            "azurerm_mssql_server.{id}_server.fully_qualified_domain_name"
        )),
        "database_id" => Some(format!("azurerm_mssql_database.{id}.id")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
