//! Plain `azurerm_cosmosdb_account`, SQL (document) API — Azure analog
//! of `aws-dynamodb-table`/`gcp-firestore-database`.

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
    #[serde(default = "default_consistency_level")]
    consistency_level: String,
    /// Failover region — a `geo_location` block is required even for a
    /// single-region account.
    failover_location: String,
}

fn default_consistency_level() -> String {
    "Session".to_string()
}

submit_infra_module!(
    "azure-cosmosdb-account",
    "Cosmos DB Account",
    "data",
    "azure",
    Config,
    &["id", "endpoint"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let consistency_level = get("consistency_level", "\"Session\"");
    let failover_location = get("failover_location", "\"\"");

    format!(
        "resource \"azurerm_cosmosdb_account\" \"{id}\" {{\n  name                = {name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  offer_type          = \"Standard\"\n  kind                = \"GlobalDocumentDB\"\n\n  consistency_policy {{\n    consistency_level = {consistency_level}\n  }}\n\n  geo_location {{\n    location          = {failover_location}\n    failover_priority = 0\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" => Some(format!("azurerm_cosmosdb_account.{id}.id")),
        "endpoint" => Some(format!("azurerm_cosmosdb_account.{id}.endpoint")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
