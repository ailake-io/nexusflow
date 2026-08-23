use nexus_core::{NodeSpec, PipelineSpec};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// What kind of resource a lineage node represents — purely cosmetic
/// (which icon the frontend picks), no behavior depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Table,
    Collection,
    Topic,
    File,
}

/// One node in the lineage graph — a saved pipeline, or a resource one or
/// more pipelines touch. Resource nodes are the join points: two pipelines
/// pointing at the same `(connector, identifier)` share one resource node,
/// which is what makes cross-pipeline lineage fall out of the graph shape
/// itself instead of needing separate matching logic.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum LineageNode {
    Pipeline {
        id: String,
        label: String,
        has_schedule: bool,
    },
    Resource {
        id: String,
        label: String,
        connector: String,
        resource_kind: ResourceKind,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct LineageEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LineageGraph {
    pub nodes: Vec<LineageNode>,
    pub edges: Vec<LineageEdge>,
}

/// Reads only a fixed allowlist of known-safe fields per connector — never
/// `uri`/`url`/`password`, or anything else that can carry a credential.
/// The legacy `uri` form several connectors accept embeds `user:pass@host`
/// inline (e.g. `nexus-connector-postgres`'s `PostgresConnectorConfig::uri`)
/// — `PipelineSummary`/`NodeSummary` already never expose raw `config` for
/// exactly this reason (`pipeline_store.rs`), and this function keeps that
/// same bar for the lineage graph. Every field name below was confirmed
/// against the connector's own `config.rs` in this repo, not guessed.
///
/// An unrecognized connector returns `None` — the node still appears in the
/// graph (via `build_graph`), just without a resource to cross-link on.
/// Expanding this list is incremental, low-risk work: a wrong or missing
/// field just means a node stays unlinked, never a panic or a leak.
pub fn resource_identifier(connector: &str, config: &Value) -> Option<(ResourceKind, String)> {
    let field = |name: &str| -> Option<&str> {
        config
            .get(name)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    };
    // Prefixes `name_field` with `ns_field` when present, so e.g. two
    // Postgres databases with a same-named `events` table don't collide
    // into one lineage node.
    let qualified = |ns_field: &str, name_field: &str| -> Option<String> {
        let name = field(name_field)?;
        Some(match field(ns_field) {
            Some(ns) => format!("{ns}.{name}"),
            None => name.to_string(),
        })
    };

    match connector {
        "postgres" | "postgres-cdc" | "sqlite" | "mysql" | "clickhouse" | "pgvector" => {
            qualified("database", "table").map(|id| (ResourceKind::Table, id))
        }
        "ailake" | "iceberg" => qualified("namespace", "table").map(|id| (ResourceKind::Table, id)),
        "lancedb" => field("table").map(|t| (ResourceKind::Table, t.to_string())),
        "mongodb" | "chromadb" => {
            qualified("database", "collection").map(|id| (ResourceKind::Collection, id))
        }
        "qdrant" | "milvus" => {
            field("collection").map(|c| (ResourceKind::Collection, c.to_string()))
        }
        "pinecone" => qualified("namespace", "index_name").map(|id| (ResourceKind::Collection, id)),
        "kafka" => field("topic").map(|t| (ResourceKind::Topic, t.to_string())),
        "mqtt" => field("topic_filter").map(|t| (ResourceKind::Topic, t.to_string())),
        "csv" | "parquet" | "deltalake" => {
            field("path").map(|p| (ResourceKind::File, p.to_string()))
        }
        _ => None,
    }
}

fn add_resource_node(nodes: &mut BTreeMap<String, LineageNode>, node: &NodeSpec) -> Option<String> {
    let (resource_kind, identifier) = resource_identifier(&node.connector, &node.config)?;
    let id = format!("resource::{}::{identifier}", node.connector);
    nodes
        .entry(id.clone())
        .or_insert_with(|| LineageNode::Resource {
            id: id.clone(),
            label: identifier,
            connector: node.connector.clone(),
            resource_kind,
        });
    Some(id)
}

/// Builds the whole-catalog lineage graph from every saved `PipelineSpec` —
/// pure and DB-free (specs are passed in already decrypted/decoded), same
/// testability rationale as the SQL builders in the connector crates (e.g.
/// `nexus-connector-postgres`'s `build_select_query`). Transform/dbt/python/
/// embedding stages stay implicit inside the pipeline node — this is
/// pipeline-level lineage, not column-level (see `docs/` proposal, Fase 5).
pub fn build_graph(specs: &[PipelineSpec]) -> LineageGraph {
    let mut nodes: BTreeMap<String, LineageNode> = BTreeMap::new();
    let mut edges = Vec::new();

    for spec in specs {
        let pipeline_id = format!("pipeline::{}", spec.pipeline_id);
        nodes.insert(
            pipeline_id.clone(),
            LineageNode::Pipeline {
                id: pipeline_id.clone(),
                label: spec.pipeline_id.clone(),
                has_schedule: spec.schedule.is_some(),
            },
        );

        for source in &spec.sources {
            if let Some(resource_id) = add_resource_node(&mut nodes, source) {
                edges.push(LineageEdge {
                    from: resource_id,
                    to: pipeline_id.clone(),
                });
            }
        }
        // `post_dbt_sinks` (true-ETL final destination, DbtConfig::output)
        // is a write, same direction as `sinks` — chained here for free,
        // zero new capture.
        for sink in spec.sinks.iter().chain(spec.post_dbt_sinks.iter()) {
            if let Some(resource_id) = add_resource_node(&mut nodes, sink) {
                edges.push(LineageEdge {
                    from: pipeline_id.clone(),
                    to: resource_id,
                });
            }
        }
    }

    LineageGraph {
        nodes: nodes.into_values().collect(),
        edges,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(connector: &str, config: Value) -> NodeSpec {
        NodeSpec {
            name: None,
            connector: connector.to_string(),
            config,
        }
    }

    fn spec(id: &str, sources: Vec<NodeSpec>, sinks: Vec<NodeSpec>) -> PipelineSpec {
        PipelineSpec {
            pipeline_id: id.to_string(),
            sources,
            transform: None,
            sinks,
            embedding: None,
            python: None,
            channel_capacity: 100,
            partitions: 1,
            dbt: None,
            post_dbt_sinks: Vec::new(),
            schedule: None,
            draft: false,
        }
    }

    #[test]
    fn resource_identifier_reads_table_for_relational_connectors() {
        let cfg = serde_json::json!({"table": "events", "database": "analytics"});
        assert_eq!(
            resource_identifier("postgres", &cfg),
            Some((ResourceKind::Table, "analytics.events".to_string()))
        );
    }

    #[test]
    fn resource_identifier_never_reads_credential_bearing_fields() {
        // Same shape a real legacy config might have: `uri` embeds a
        // password right alongside the safe `table`/`database` fields.
        let cfg = serde_json::json!({
            "uri": "postgres://admin:s3cr3t@db.internal/prod",
            "password": "s3cr3t",
            "table": "events",
            "database": "analytics",
        });
        let (_, identifier) = resource_identifier("postgres", &cfg).unwrap();
        assert_eq!(identifier, "analytics.events");
        assert!(!identifier.contains("s3cr3t"));
        assert!(!identifier.contains("admin"));
    }

    #[test]
    fn resource_identifier_falls_back_to_bare_name_without_namespace() {
        let cfg = serde_json::json!({"table": "events"});
        assert_eq!(
            resource_identifier("sqlite", &cfg),
            Some((ResourceKind::Table, "events".to_string()))
        );
    }

    #[test]
    fn resource_identifier_covers_every_documented_connector_family() {
        let cases: &[(&str, Value, ResourceKind)] = &[
            (
                "kafka",
                serde_json::json!({"topic": "orders"}),
                ResourceKind::Topic,
            ),
            (
                "mqtt",
                serde_json::json!({"topic_filter": "sensors/#"}),
                ResourceKind::Topic,
            ),
            (
                "mongodb",
                serde_json::json!({"collection": "docs", "database": "app"}),
                ResourceKind::Collection,
            ),
            (
                "qdrant",
                serde_json::json!({"collection": "docs"}),
                ResourceKind::Collection,
            ),
            (
                "pinecone",
                serde_json::json!({"index_name": "docs"}),
                ResourceKind::Collection,
            ),
            (
                "csv",
                serde_json::json!({"path": "/data/events.csv"}),
                ResourceKind::File,
            ),
        ];
        for (connector, cfg, expected_kind) in cases {
            let (kind, _) = resource_identifier(connector, cfg)
                .unwrap_or_else(|| panic!("expected a resource for {connector:?}"));
            assert_eq!(kind, *expected_kind, "connector {connector:?}");
        }
    }

    #[test]
    fn resource_identifier_returns_none_for_unknown_connector() {
        let cfg = serde_json::json!({"url": "https://api.example.com/webhook"});
        assert_eq!(resource_identifier("webhook", &cfg), None);
    }

    #[test]
    fn resource_identifier_returns_none_when_the_allowlisted_field_is_missing() {
        let cfg = serde_json::json!({"database": "analytics"});
        assert_eq!(resource_identifier("postgres", &cfg), None);
    }

    #[test]
    fn build_graph_links_two_pipelines_sharing_a_resource() {
        let table = serde_json::json!({"table": "events", "database": "analytics"});
        let specs = vec![
            spec(
                "ingest",
                vec![node("csv", serde_json::json!({"path": "/data/raw.csv"}))],
                vec![node("postgres", table.clone())],
            ),
            spec(
                "export",
                vec![node("postgres", table)],
                vec![node("csv", serde_json::json!({"path": "/data/out.csv"}))],
            ),
        ];

        let graph = build_graph(&specs);

        // 2 pipeline nodes + 3 resource nodes (raw.csv, analytics.events, out.csv).
        assert_eq!(graph.nodes.len(), 5);
        assert_eq!(graph.edges.len(), 4);

        let shared_resource_id = "resource::postgres::analytics.events";
        let touches_shared = graph
            .edges
            .iter()
            .filter(|e| e.from == shared_resource_id || e.to == shared_resource_id)
            .count();
        assert_eq!(
            touches_shared, 2,
            "the shared table must connect both pipelines, one edge each"
        );
    }

    #[test]
    fn build_graph_keeps_an_isolated_pipeline_as_its_own_component() {
        let specs = vec![spec(
            "solo",
            vec![node("csv", serde_json::json!({"path": "/data/a.csv"}))],
            vec![node("csv", serde_json::json!({"path": "/data/b.csv"}))],
        )];

        let graph = build_graph(&specs);
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.edges.len(), 2);
    }

    #[test]
    fn build_graph_keeps_the_pipeline_node_when_its_connector_has_no_resource() {
        let specs = vec![spec(
            "webhook-only",
            vec![node(
                "rest",
                serde_json::json!({"url": "https://api.example.com"}),
            )],
            vec![node(
                "webhook",
                serde_json::json!({"url": "https://hooks.example.com"}),
            )],
        )];

        let graph = build_graph(&specs);
        // Only the pipeline node — neither `rest` nor `webhook` is in the
        // allowlist, so no resource nodes or edges are produced.
        assert_eq!(graph.nodes.len(), 1);
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn build_graph_includes_post_dbt_sinks_as_writes() {
        let mut s = spec(
            "elt",
            vec![node("csv", serde_json::json!({"path": "/data/raw.csv"}))],
            vec![node("postgres", serde_json::json!({"table": "staging"}))],
        );
        s.post_dbt_sinks = vec![node("postgres", serde_json::json!({"table": "final"}))];

        let graph = build_graph(&[s]);
        let pipeline_id = "pipeline::elt";
        let writes_to_final = graph
            .edges
            .iter()
            .any(|e| e.from == pipeline_id && e.to == "resource::postgres::final");
        assert!(writes_to_final);
    }
}
