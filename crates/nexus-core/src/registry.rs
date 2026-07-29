use crate::traits::ConnectorCapability;

// Re-exported so downstream connector crates don't need their own direct
// `inventory` dependency just to call `submit_connector!`.
pub use inventory;

/// Metadata a connector crate publishes about itself. Each connector crate calls
/// `nexus_core::registry::submit_connector!(...)` in its own lib.rs — nexus-server
/// never hardcodes a connector list, it just iterates the registry.
/// See ARCHITECTURE.md §3.
pub struct ConnectorDescriptor {
    pub name: &'static str,
    pub capability: ConnectorCapability,
}

inventory::collect!(ConnectorDescriptor);

#[macro_export]
macro_rules! submit_connector {
    ($name:expr, $capability:expr) => {
        $crate::registry::inventory::submit! {
            $crate::registry::ConnectorDescriptor {
                name: $name,
                capability: $capability,
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

    submit_connector!("test-connector", ConnectorCapability::Bridged);

    #[test]
    fn registers_and_finds_connector() {
        let found = ConnectorRegistry::find("test-connector").expect("connector registered");
        assert_eq!(found.capability, ConnectorCapability::Bridged);
    }

    #[test]
    fn missing_connector_is_none() {
        assert!(ConnectorRegistry::find("does-not-exist").is_none());
    }
}
