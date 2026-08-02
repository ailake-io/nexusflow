use crate::traits::ConnectorCapability;
use serde::Serialize;

// Re-exported so downstream connector crates don't need their own direct
// `inventory`/`schemars`/`serde_json` dependency just to call
// `submit_connector!` — several connector crates only depend on `serde`
// (for Deserialize), not `serde_json` directly.
pub use inventory;
pub use schemars;
pub use serde_json;

/// Metadata a connector crate publishes about itself. Each connector crate calls
/// `nexus_core::registry::submit_connector!(...)` in its own lib.rs — nexus-server
/// never hardcodes a connector list, it just iterates the registry.
/// See ARCHITECTURE.md §3. `Serialize` so `nexus-server` can expose this
/// directly as the frontend's connector catalog (Marco 8) with no separate DTO —
/// except `config_schema`, a fn pointer (not itself `Serialize`), skipped here
/// and computed on demand by nexus-server's own DTO instead (see
/// list_connectors_handler) so the canvas can render a real form instead of a
/// raw JSON textarea.
#[derive(Serialize)]
pub struct ConnectorDescriptor {
    pub name: &'static str,
    pub capability: ConnectorCapability,
    #[serde(skip)]
    pub config_schema: fn() -> serde_json::Value,
}

inventory::collect!(ConnectorDescriptor);

#[macro_export]
macro_rules! submit_connector {
    ($name:expr, $capability:expr, $config:ty) => {
        $crate::registry::inventory::submit! {
            $crate::registry::ConnectorDescriptor {
                name: $name,
                capability: $capability,
                config_schema: || {
                    let schema = $crate::registry::schemars::schema_for!($config);
                    $crate::registry::serde_json::to_value(&schema)
                        .expect("JSON schema always serializes")
                },
            }
        }
    };
}

pub struct ConnectorRegistry;

impl ConnectorRegistry {
    pub fn all() -> impl Iterator<Item = &'static ConnectorDescriptor> {
        inventory::iter::<ConnectorDescriptor>()
    }

    pub fn find(name: &str) -> Option<&'static ConnectorDescriptor> {
        Self::all().find(|d| d.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(schemars::JsonSchema)]
    struct TestConnectorConfig {
        #[allow(dead_code)]
        uri: String,
    }

    submit_connector!(
        "test-connector",
        ConnectorCapability::Bridged,
        TestConnectorConfig
    );

    #[test]
    fn registers_and_finds_connector() {
        let found = ConnectorRegistry::find("test-connector").expect("connector registered");
        assert_eq!(found.capability, ConnectorCapability::Bridged);
        let schema = (found.config_schema)();
        assert_eq!(schema["properties"]["uri"]["type"], "string");
    }

    #[test]
    fn missing_connector_is_none() {
        assert!(ConnectorRegistry::find("does-not-exist").is_none());
    }
}
