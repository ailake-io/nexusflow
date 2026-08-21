use crate::error::NexusError;
use crate::traits::{ConnectorCapability, Sink, Source};
use futures::future::BoxFuture;
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

/// Plugin extension point for connectors that live outside this workspace
/// (`docs/ENTERPRISE_CONNECTORS.md`, `ROADMAP.md` Fase 12 Bloco 3) — an
/// enterprise crate in a private repo can't add a match arm to
/// `nexus-server`'s `build_source`/`build_sink` (it doesn't own that code),
/// so it registers a builder here instead, via `submit_source_builder!`.
/// `nexus-server` falls back to this registry only when its own hardcoded
/// match on `node.connector` doesn't recognize the name (see
/// `connectors.rs`'s doc comment on `build_source`) — every OSS connector
/// keeps going through its own match arm unchanged, this is additive.
/// `fn` pointer type of [`SourceBuilder::build`] — factored out because
/// clippy's `type_complexity` lint (denied via `-D warnings` in CI) flags
/// the inline form.
pub type SourceBuildFn =
    fn(serde_json::Value) -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>>;

pub struct SourceBuilder {
    pub name: &'static str,
    /// Cheap check that the raw JSON config deserializes into this
    /// connector's config struct — no I/O, mirrors what
    /// `validate_source_config` already does per OSS connector today.
    pub validate: fn(&serde_json::Value) -> Result<(), NexusError>,
    /// Deserializes the config and connects for real. Boxed future because
    /// connecting is inherently async and a `fn` pointer can't itself be
    /// `async fn` — same shape `async_trait` already generates for
    /// `Source`/`Sink` themselves (`traits.rs`).
    pub build: SourceBuildFn,
}

inventory::collect!(SourceBuilder);

/// `fn` pointer type of [`SinkBuilder::build`] — same reason as
/// [`SourceBuildFn`].
pub type SinkBuildFn =
    fn(serde_json::Value) -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>>;

/// Sink counterpart of [`SourceBuilder`] — same rationale, same fallback
/// contract in `nexus-server`'s `build_sink`.
pub struct SinkBuilder {
    pub name: &'static str,
    pub validate: fn(&serde_json::Value) -> Result<(), NexusError>,
    pub build: SinkBuildFn,
}

inventory::collect!(SinkBuilder);

#[macro_export]
macro_rules! submit_source_builder {
    ($name:expr, $validate:expr, $build:expr) => {
        $crate::registry::inventory::submit! {
            $crate::registry::SourceBuilder {
                name: $name,
                validate: $validate,
                build: $build,
            }
        }
    };
}

#[macro_export]
macro_rules! submit_sink_builder {
    ($name:expr, $validate:expr, $build:expr) => {
        $crate::registry::inventory::submit! {
            $crate::registry::SinkBuilder {
                name: $name,
                validate: $validate,
                build: $build,
            }
        }
    };
}

/// Marks a connector's config as local-filesystem-based by design — its
/// `path`/`uri`/`table_uri` field (whatever it's called) is documented as a
/// plain local path, not a URL a remote caller could redirect, so `dag.rs`'s
/// absolute-path SSRF-style guard should let it through. Separate from
/// `ConnectorDescriptor` (rather than a field added there) specifically so
/// opting in never requires touching every existing `submit_connector!`/
/// `submit_enterprise_connector!` call site — a connector just adds one more
/// registration line.
///
/// An enterprise connector living in a private repo opts in the same way an
/// OSS one does: call `nexus_core::submit_local_path_connector!("its-name")`
/// from its own crate. The public repo's `is_local_path_connector` (dag.rs)
/// never needs to know that connector's name — `LICENSING.md §3`'s "no
/// enterprise connector name in the public repo" rule stays intact.
pub struct LocalPathConnector {
    pub name: &'static str,
}

inventory::collect!(LocalPathConnector);

#[macro_export]
macro_rules! submit_local_path_connector {
    ($name:expr) => {
        $crate::registry::inventory::submit! {
            $crate::registry::LocalPathConnector { name: $name }
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

    pub fn find_source_builder(name: &str) -> Option<&'static SourceBuilder> {
        inventory::iter::<SourceBuilder>().find(|b| b.name == name)
    }

    pub fn find_sink_builder(name: &str) -> Option<&'static SinkBuilder> {
        inventory::iter::<SinkBuilder>().find(|b| b.name == name)
    }

    /// True if `name` registered itself via `submit_local_path_connector!`.
    pub fn is_local_path_connector(name: &str) -> bool {
        inventory::iter::<LocalPathConnector>().any(|c| c.name == name)
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

    // --- SourceBuilder / SinkBuilder plugin extension point ---
    // Stands in for a connector defined in an out-of-tree private crate
    // (`nexus-connectors-enterprise`) that can't add a match arm to
    // nexus-server's build_source/build_sink — see the doc comment above
    // `SourceBuilder`.

    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};

    struct DummySource;

    #[async_trait]
    impl Source for DummySource {
        async fn read_batches(
            &mut self,
        ) -> Result<BoxStream<'_, Result<arrow_array::RecordBatch, NexusError>>, NexusError>
        {
            Ok(Box::pin(stream::empty()))
        }

        fn schema(&self) -> arrow_schema::SchemaRef {
            std::sync::Arc::new(arrow_schema::Schema::empty())
        }
    }

    struct DummySink;

    #[async_trait]
    impl Sink for DummySink {
        async fn write_batch(
            &mut self,
            _batch: arrow_array::RecordBatch,
        ) -> Result<(), NexusError> {
            Ok(())
        }

        async fn commit_checkpoint(
            &mut self,
            _cursor: crate::CheckpointCursor,
        ) -> Result<(), NexusError> {
            Ok(())
        }
    }

    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct TestPluginConfig {
        #[allow(dead_code)]
        value: String,
    }

    fn validate_test_plugin_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
        serde_json::from_value::<TestPluginConfig>(cfg.clone())
            .map(|_| ())
            .map_err(|e| NexusError::Serialization(e.to_string()))
    }

    submit_source_builder!(
        "test-plugin-source",
        validate_test_plugin_config,
        |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
            Box::pin(async move {
                let _parsed: TestPluginConfig = serde_json::from_value(cfg)
                    .map_err(|e| NexusError::Serialization(e.to_string()))?;
                Ok(Box::new(DummySource) as Box<dyn Source>)
            })
        }
    );

    submit_sink_builder!(
        "test-plugin-sink",
        validate_test_plugin_config,
        |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
            Box::pin(async move {
                let _parsed: TestPluginConfig = serde_json::from_value(cfg)
                    .map_err(|e| NexusError::Serialization(e.to_string()))?;
                Ok(Box::new(DummySink) as Box<dyn Sink>)
            })
        }
    );

    #[test]
    fn finds_registered_source_builder() {
        let found =
            ConnectorRegistry::find_source_builder("test-plugin-source").expect("registered");
        assert!((found.validate)(&serde_json::json!({"value": "x"})).is_ok());
        assert!((found.validate)(&serde_json::json!({})).is_err());
    }

    #[tokio::test]
    async fn builds_source_via_registered_builder() {
        let found =
            ConnectorRegistry::find_source_builder("test-plugin-source").expect("registered");
        let source = (found.build)(serde_json::json!({"value": "x"}))
            .await
            .expect("builds");
        assert_eq!(source.schema().fields().len(), 0);
    }

    #[test]
    fn missing_source_builder_is_none() {
        assert!(ConnectorRegistry::find_source_builder("does-not-exist").is_none());
    }

    #[tokio::test]
    async fn builds_sink_via_registered_builder() {
        let found = ConnectorRegistry::find_sink_builder("test-plugin-sink").expect("registered");
        let mut sink = (found.build)(serde_json::json!({"value": "x"}))
            .await
            .expect("builds");
        sink.commit_checkpoint(crate::CheckpointCursor::new("p0"))
            .await
            .expect("commits");
    }

    #[test]
    fn missing_sink_builder_is_none() {
        assert!(ConnectorRegistry::find_sink_builder("does-not-exist").is_none());
    }
}
