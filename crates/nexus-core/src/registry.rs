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
    /// `None` for every OSS connector. `Some(slug)` marks a connector as
    /// enterprise-gated — a paid feature per `LICENSING.md §2` /
    /// `docs/ENTERPRISE_LICENSING.md`, unlocked only by an installed license
    /// whose `connectors` list contains `slug`. Set via
    /// `submit_enterprise_connector!` instead of `submit_connector!`.
    pub requires_license: Option<&'static str>,
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
                requires_license: None,
            }
        }
    };
}

/// Same as `submit_connector!`, but marks the connector as enterprise-gated
/// under its own name (see `ConnectorDescriptor::requires_license`). No
/// crate calls this yet — enterprise connectors live in a private repo that
/// doesn't exist in this workspace (`LICENSING.md §2`); this macro is the
/// registration primitive that repo will use.
#[macro_export]
macro_rules! submit_enterprise_connector {
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
                requires_license: Some($name),
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

    submit_enterprise_connector!(
        "test-enterprise-connector",
        ConnectorCapability::Bridged,
        TestConnectorConfig
    );

    #[test]
    fn enterprise_connector_is_tagged_with_its_own_slug() {
        let found = ConnectorRegistry::find("test-enterprise-connector").expect("registered");
        assert_eq!(found.requires_license, Some("test-enterprise-connector"));
    }

    #[test]
    fn oss_connector_requires_no_license() {
        let found = ConnectorRegistry::find("test-connector").expect("registered");
        assert_eq!(found.requires_license, None);
    }
}
