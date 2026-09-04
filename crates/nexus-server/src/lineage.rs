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
    /// One node from a pipeline's dbt project (`model`/`source`/`seed`/
    /// `snapshot` — see `dbt_resource_type_and_label`). `id` is prefixed by
    /// the owning pipeline (`dbt::{pipeline_id}::{unique_id}`) so two
    /// different dbt projects reusing a model name never collide into one
    /// node — same caution as `Resource` never merging across connectors.
    DbtNode {
        id: String,
        label: String,
        resource_type: String,
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

    // Resolves whichever of a legacy/new field-name pair is set — several
    // connectors (`qdrant`/`milvus`'s `collection`→`collection_name`,
    // `deltalake`/`iceberg`/`ailake`'s `table`→`table_name`, etc.) kept the
    // old field for backward compatibility but prefer the new one for new
    // canvas nodes, per each connector's own config.rs doc comment.
    let either = |legacy: &str, preferred: &str| field(legacy).or_else(|| field(preferred));

    match connector {
        "postgres" | "postgres-cdc" | "sqlite" | "mysql" | "mysql-cdc" | "clickhouse"
        | "pgvector" | "odbc" => qualified("database", "table").map(|id| (ResourceKind::Table, id)),
        "duckdb" => field("table").map(|t| (ResourceKind::Table, t.to_string())),
        "ailake" | "ailake-cdc" | "iceberg" | "iceberg-cdc" => {
            let ns = either("namespace", "namespace_name");
            let name = either("table", "table_name");
            name.map(|n| {
                let id = match ns {
                    Some(ns) => format!("{ns}.{n}"),
                    None => n.to_string(),
                };
                (ResourceKind::Table, id)
            })
        }
        "deltalake" | "deltalake-cdc" => field("table_name")
            .or_else(|| field("path"))
            .map(|t| (ResourceKind::Table, t.to_string())),
        "lancedb" => either("table", "table_name").map(|t| (ResourceKind::Table, t.to_string())),
        "mongodb" | "mongodb-cdc" | "chromadb" => {
            qualified("database", "collection").map(|id| (ResourceKind::Collection, id))
        }
        "qdrant" | "milvus" => {
            either("collection", "collection_name").map(|c| (ResourceKind::Collection, c.to_string()))
        }
        "pinecone" => qualified("namespace", "index_name").map(|id| (ResourceKind::Collection, id)),
        "kafka" => field("topic").map(|t| (ResourceKind::Topic, t.to_string())),
        "mqtt" => field("topic_filter").map(|t| (ResourceKind::Topic, t.to_string())),
        "nats" => field("subject").map(|s| (ResourceKind::Topic, s.to_string())),
        "redis" => field("stream_key").map(|s| (ResourceKind::Topic, s.to_string())),
        "rabbitmq" => field("queue").map(|q| (ResourceKind::Topic, q.to_string())),
        "csv" | "parquet" => field("path").map(|p| (ResourceKind::File, p.to_string())),
        // Enterprise connectors — ODBC batch (database+table, no CDC
        // variant for any of these yet):
        "teradata" | "vertica" => qualified("database", "table").map(|id| (ResourceKind::Table, id)),
        // Enterprise REST/SaaS — one stable identifier per connector, no
        // shared namespace concept:
        "hubspot" => field("object_type").map(|o| (ResourceKind::Collection, o.to_string())),
        "zendesk" => field("resource").map(|r| (ResourceKind::Collection, r.to_string())),
        "google-sheets" => field("spreadsheet_id").map(|s| (ResourceKind::Table, s.to_string())),
        "dropbox" => field("folder_path").map(|p| (ResourceKind::File, p.to_string())),
        "google-drive" => field("folder_id").map(|f| (ResourceKind::File, f.to_string())),
        "servicenow" => field("table").map(|t| (ResourceKind::Table, t.to_string())),
        "dynamics365" => field("entity_set").map(|e| (ResourceKind::Table, e.to_string())),
        "sharepoint" => qualified("site_id", "list_id").map(|id| (ResourceKind::Table, id)),
        "netsuite" => field("record_type").map(|r| (ResourceKind::Table, r.to_string())),
        "workday" => field("report_name").map(|r| (ResourceKind::Table, r.to_string())),
        // Enterprise ODBC/ADBC batch (database+table, no CDC-specific
        // fields needed beyond what the batch variant already has):
        "hana" | "mssql" | "mssql-cdc" | "oracle" | "oracle-cdc" | "redshift" | "synapse" => {
            qualified("database", "table").map(|id| (ResourceKind::Table, id))
        }
        // Three-part warehouse addressing — `qualified()` only joins two
        // fields, so these are built inline instead of stretching that
        // closure's signature for 3 call sites.
        "snowflake" => {
            let db = field("database")?;
            let schema = field("schema")?;
            let table = field("table")?;
            Some((ResourceKind::Table, format!("{db}.{schema}.{table}")))
        }
        "bigquery" => {
            let project = field("project_id")?;
            let dataset = field("dataset_id")?;
            let table = field("table")?;
            Some((ResourceKind::Table, format!("{project}.{dataset}.{table}")))
        }
        "databricks" => {
            let catalog = field("catalog")?;
            let schema = field("schema")?;
            let table = field("table")?;
            Some((ResourceKind::Table, format!("{catalog}.{schema}.{table}")))
        }
        "starburst" => {
            let catalog = field("catalog")?;
            let schema = field("schema_name")?;
            let table = field("table_name")?;
            Some((ResourceKind::Table, format!("{catalog}.{schema}.{table}")))
        }
        // Enterprise vector/search sinks — same shape as qdrant/milvus
        // above, one collection-like identifier field each:
        "elasticsearch" | "opensearch" => {
            field("index").map(|i| (ResourceKind::Collection, i.to_string()))
        }
        "azure-ai-search" => field("index_name").map(|i| (ResourceKind::Collection, i.to_string())),
        "weaviate" => field("class_name").map(|c| (ResourceKind::Collection, c.to_string())),
        "vertex-vector-search" => field("index_id").map(|i| (ResourceKind::Collection, i.to_string())),
        // Enterprise streaming:
        "kinesis" => field("stream_name").map(|s| (ResourceKind::Topic, s.to_string())),
        "pulsar" => field("topic").map(|t| (ResourceKind::Topic, t.to_string())),
        // Enterprise SaaS/CRM — one stable identifier per connector:
        "salesforce" => field("sobject").map(|s| (ResourceKind::Table, s.to_string())),
        "shopify" => field("shop_domain").map(|s| (ResourceKind::Table, s.to_string())),
        "stripe" => field("resource").map(|r| (ResourceKind::Table, r.to_string())),
        "excel" => field("path").map(|p| (ResourceKind::File, p.to_string())),
        // Enterprise ads/analytics — the queried account/property is the
        // closest thing to a stable "resource" (there's no destination
        // table on this side, these are source-only reporting APIs):
        "ga4" => field("property_id").map(|p| (ResourceKind::Table, p.to_string())),
        "google-ads" => field("customer_id").map(|c| (ResourceKind::Table, c.to_string())),
        "meta-ads" => field("ad_account_id").map(|a| (ResourceKind::Table, a.to_string())),
        "tiktok-ads" => field("advertiser_id").map(|a| (ResourceKind::Table, a.to_string())),
        "x-ads" => field("account_id").map(|a| (ResourceKind::Table, a.to_string())),
        "youtube-analytics" => field("ids").map(|i| (ResourceKind::Table, i.to_string())),
        "linkedin-ads" => config
            .get("account_urns")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(|urn| (ResourceKind::Table, urn.to_string())),
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

/// Splits a dbt `unique_id` (`"<resource_type>.<package>.<name...>"`) into
/// its resource type and a display label (everything after the package
/// name — `"model.proj.stg_orders"` → `("model", "stg_orders")`,
/// `"source.proj.raw.orders"` → `("source", "raw.orders")`). Same parsing
/// dbt itself uses internally; duplicated here (not imported from `dbt.rs`)
/// because that module's types are feature-gated and this one isn't.
fn dbt_resource_type_and_label(unique_id: &str) -> (&str, &str) {
    let resource_type = unique_id.split('.').next().unwrap_or(unique_id);
    let label = unique_id
        .split_once('.')
        .and_then(|(_, rest)| rest.split_once('.'))
        .map(|(_, name)| name)
        .unwrap_or(unique_id);
    (resource_type, label)
}

fn dbt_node_id(pipeline_spec_id: &str, unique_id: &str) -> String {
    format!("dbt::{pipeline_spec_id}::{unique_id}")
}

/// Merges one pipeline's dbt `parent_map` (`unique_id` -> its upstream
/// `unique_id`s, dbt's own direction) into the graph, anchored on the
/// pipeline node via every dbt node with no parent *within this map* (its
/// entry points — normally `source.*` nodes reading the table the pipeline
/// itself just loaded). Test nodes are excluded: they're validations, not
/// part of the data-transformation flow this tab is showing (Fase 3's
/// Quality tab is where test results belong).
fn merge_dbt_lineage(
    nodes: &mut BTreeMap<String, LineageNode>,
    edges: &mut Vec<LineageEdge>,
    pipeline_node_id: &str,
    pipeline_spec_id: &str,
    parent_map: &std::collections::HashMap<String, Vec<String>>,
) {
    let is_test = |id: &str| dbt_resource_type_and_label(id).0 == "test";

    // Every real (non-test) node that appears anywhere in the map — as a
    // key, or only inside some other node's parent list (dbt doesn't
    // always give a leaf source its own key with an empty value).
    let mut all_ids: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for (child, parents) in parent_map {
        if !is_test(child) {
            all_ids.insert(child.as_str());
        }
        all_ids.extend(parents.iter().map(String::as_str).filter(|p| !is_test(p)));
    }

    for unique_id in all_ids {
        let node_id = dbt_node_id(pipeline_spec_id, unique_id);
        nodes.entry(node_id.clone()).or_insert_with(|| {
            let (resource_type, label) = dbt_resource_type_and_label(unique_id);
            LineageNode::DbtNode {
                id: node_id.clone(),
                label: label.to_string(),
                resource_type: resource_type.to_string(),
            }
        });

        let real_parents: Vec<&str> = parent_map
            .get(unique_id)
            .into_iter()
            .flatten()
            .map(String::as_str)
            .filter(|p| !is_test(p))
            .collect();

        if real_parents.is_empty() {
            // No upstream dependency within this pipeline's dbt project —
            // an entry point, anchored on the pipeline that ran it.
            edges.push(LineageEdge {
                from: pipeline_node_id.to_string(),
                to: node_id.clone(),
            });
        } else {
            for parent in real_parents {
                edges.push(LineageEdge {
                    from: dbt_node_id(pipeline_spec_id, parent),
                    to: node_id.clone(),
                });
            }
        }
    }
}

/// Builds the whole-catalog lineage graph from every saved `PipelineSpec`,
/// optionally deepened with each pipeline's latest dbt model graph
/// (`dbt_lineages`, keyed by `pipeline_id` — from `DbtLineageStore::
/// get_all`, empty map if no pipeline has run dbt yet). Pure and DB-free
/// otherwise (specs/lineages passed in already loaded), same testability
/// rationale as the SQL builders in the connector crates (e.g.
/// `nexus-connector-postgres`'s `build_select_query`). Transform/python/
/// embedding stages stay implicit inside the pipeline node — dbt is the one
/// stage that gets its own sub-graph because dbt already computes one.
pub fn build_graph(
    specs: &[PipelineSpec],
    dbt_lineages: &std::collections::HashMap<
        String,
        std::collections::HashMap<String, Vec<String>>,
    >,
) -> LineageGraph {
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

        if let Some(parent_map) = dbt_lineages.get(&spec.pipeline_id) {
            merge_dbt_lineage(
                &mut nodes,
                &mut edges,
                &pipeline_id,
                &spec.pipeline_id,
                parent_map,
            );
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
            alerts: None,
            quality_checks: Vec::new(),
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
            (
                "duckdb",
                serde_json::json!({"table": "events"}),
                ResourceKind::Table,
            ),
            (
                "redis",
                serde_json::json!({"stream_key": "events"}),
                ResourceKind::Topic,
            ),
            (
                "nats",
                serde_json::json!({"subject": "events"}),
                ResourceKind::Topic,
            ),
            (
                "rabbitmq",
                serde_json::json!({"queue": "events"}),
                ResourceKind::Topic,
            ),
            (
                "odbc",
                serde_json::json!({"table": "events", "database": "warehouse"}),
                ResourceKind::Table,
            ),
            (
                "mysql-cdc",
                serde_json::json!({"table": "events", "database": "app"}),
                ResourceKind::Table,
            ),
            (
                "mongodb-cdc",
                serde_json::json!({"collection": "docs", "database": "app"}),
                ResourceKind::Collection,
            ),
            (
                "hubspot",
                serde_json::json!({"object_type": "contacts"}),
                ResourceKind::Collection,
            ),
            (
                "zendesk",
                serde_json::json!({"resource": "tickets"}),
                ResourceKind::Collection,
            ),
            (
                "google-sheets",
                serde_json::json!({"spreadsheet_id": "abc123"}),
                ResourceKind::Table,
            ),
            (
                "dropbox",
                serde_json::json!({"folder_path": "/data"}),
                ResourceKind::File,
            ),
            (
                "google-drive",
                serde_json::json!({"folder_id": "folder123"}),
                ResourceKind::File,
            ),
            (
                "servicenow",
                serde_json::json!({"table": "incident"}),
                ResourceKind::Table,
            ),
            (
                "dynamics365",
                serde_json::json!({"entity_set": "accounts"}),
                ResourceKind::Table,
            ),
            (
                "sharepoint",
                serde_json::json!({"site_id": "site1", "list_id": "list1"}),
                ResourceKind::Table,
            ),
            (
                "netsuite",
                serde_json::json!({"record_type": "customer"}),
                ResourceKind::Table,
            ),
            (
                "workday",
                serde_json::json!({"report_name": "Custom_Worker_Report"}),
                ResourceKind::Table,
            ),
            (
                "teradata",
                serde_json::json!({"table": "events", "database": "warehouse"}),
                ResourceKind::Table,
            ),
            (
                "vertica",
                serde_json::json!({"table": "events", "database": "vmart"}),
                ResourceKind::Table,
            ),
            (
                "hana",
                serde_json::json!({"table": "events", "database": "warehouse"}),
                ResourceKind::Table,
            ),
            (
                "mssql",
                serde_json::json!({"table": "events", "database": "app"}),
                ResourceKind::Table,
            ),
            (
                "oracle-cdc",
                serde_json::json!({"table": "events", "database": "app"}),
                ResourceKind::Table,
            ),
            (
                "snowflake",
                serde_json::json!({"database": "db", "schema": "public", "table": "events"}),
                ResourceKind::Table,
            ),
            (
                "bigquery",
                serde_json::json!({"project_id": "proj", "dataset_id": "ds", "table": "events"}),
                ResourceKind::Table,
            ),
            (
                "databricks",
                serde_json::json!({"catalog": "main", "schema": "default", "table": "events"}),
                ResourceKind::Table,
            ),
            (
                "starburst",
                serde_json::json!({"catalog": "hive", "schema_name": "default", "table_name": "events"}),
                ResourceKind::Table,
            ),
            (
                "elasticsearch",
                serde_json::json!({"index": "events"}),
                ResourceKind::Collection,
            ),
            (
                "weaviate",
                serde_json::json!({"class_name": "Events"}),
                ResourceKind::Collection,
            ),
            (
                "kinesis",
                serde_json::json!({"stream_name": "events"}),
                ResourceKind::Topic,
            ),
            (
                "pulsar",
                serde_json::json!({"topic": "events"}),
                ResourceKind::Topic,
            ),
            (
                "salesforce",
                serde_json::json!({"sobject": "Account"}),
                ResourceKind::Table,
            ),
            (
                "stripe",
                serde_json::json!({"resource": "charges"}),
                ResourceKind::Table,
            ),
            (
                "ga4",
                serde_json::json!({"property_id": "123456"}),
                ResourceKind::Table,
            ),
            (
                "linkedin-ads",
                serde_json::json!({"account_urns": ["urn:li:sponsoredAccount:123"]}),
                ResourceKind::Table,
            ),
        ];
        for (connector, cfg, expected_kind) in cases {
            let (kind, _) = resource_identifier(connector, cfg)
                .unwrap_or_else(|| panic!("expected a resource for {connector:?}"));
            assert_eq!(kind, *expected_kind, "connector {connector:?}");
        }
    }

    #[test]
    fn resource_identifier_returns_none_for_connectors_with_no_stable_identifier() {
        // REST-style connectors with only arbitrary identifiers — not a bug.
        assert_eq!(
            resource_identifier("rest", &serde_json::json!({"base_url": "https://x"})),
            None
        );
    }

    #[test]
    fn resource_identifier_builds_triple_qualified_warehouse_ids() {
        let cfg = serde_json::json!({
            "table": "events",
            "project_id": "proj",
            "dataset_id": "ds",
        });
        assert_eq!(
            resource_identifier("bigquery", &cfg),
            Some((ResourceKind::Table, "proj.ds.events".to_string()))
        );
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

        let graph = build_graph(&specs, &Default::default());

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

        let graph = build_graph(&specs, &Default::default());
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

        let graph = build_graph(&specs, &Default::default());
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

        let graph = build_graph(&[s], &Default::default());
        let pipeline_id = "pipeline::elt";
        let writes_to_final = graph
            .edges
            .iter()
            .any(|e| e.from == pipeline_id && e.to == "resource::postgres::final");
        assert!(writes_to_final);
    }

    #[test]
    fn dbt_resource_type_and_label_parses_model_and_source_ids() {
        assert_eq!(
            dbt_resource_type_and_label("model.proj.stg_orders"),
            ("model", "stg_orders")
        );
        assert_eq!(
            dbt_resource_type_and_label("source.proj.raw.orders"),
            ("source", "raw.orders")
        );
    }

    fn parent_map(pairs: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn build_graph_merges_dbt_chain_anchored_on_the_pipeline() {
        let s = spec(
            "elt",
            vec![node("csv", serde_json::json!({"path": "/data/raw.csv"}))],
            vec![node("postgres", serde_json::json!({"table": "staging"}))],
        );
        let mut lineages = std::collections::HashMap::new();
        lineages.insert(
            "elt".to_string(),
            parent_map(&[
                ("model.proj.stg_orders", &["source.proj.raw.orders"]),
                ("model.proj.final_orders", &["model.proj.stg_orders"]),
            ]),
        );

        let graph = build_graph(&[s], &lineages);

        let entry = "dbt::elt::source.proj.raw.orders";
        let mid = "dbt::elt::model.proj.stg_orders";
        let end = "dbt::elt::model.proj.final_orders";
        assert!(graph
            .edges
            .iter()
            .any(|e| e.from == "pipeline::elt" && e.to == entry));
        assert!(graph.edges.iter().any(|e| e.from == entry && e.to == mid));
        assert!(graph.edges.iter().any(|e| e.from == mid && e.to == end));
    }

    #[test]
    fn build_graph_excludes_dbt_test_nodes_from_the_lineage_flow() {
        let s = spec("elt", vec![], vec![]);
        let mut lineages = std::collections::HashMap::new();
        lineages.insert(
            "elt".to_string(),
            parent_map(&[("test.proj.not_null_orders_id", &["model.proj.stg_orders"])]),
        );

        let graph = build_graph(&[s], &lineages);

        assert!(!graph
            .nodes
            .iter()
            .any(|n| matches!(n, LineageNode::DbtNode { id, .. } if id.contains("test."))));
        // The model itself is still a real dbt node, and since its only
        // "child" was a test (excluded), it has no non-test dependents —
        // it must still show up as an entry point off the pipeline.
        assert!(graph
            .edges
            .iter()
            .any(|e| e.from == "pipeline::elt" && e.to == "dbt::elt::model.proj.stg_orders"));
    }

    #[test]
    fn build_graph_scopes_dbt_nodes_per_pipeline_even_with_the_same_model_name() {
        let s1 = spec("elt-a", vec![], vec![]);
        let s2 = spec("elt-b", vec![], vec![]);
        let mut lineages = std::collections::HashMap::new();
        lineages.insert(
            "elt-a".to_string(),
            parent_map(&[("model.proj.orders", &[])]),
        );
        lineages.insert(
            "elt-b".to_string(),
            parent_map(&[("model.proj.orders", &[])]),
        );

        let graph = build_graph(&[s1, s2], &lineages);

        let dbt_node_count = graph
            .nodes
            .iter()
            .filter(|n| matches!(n, LineageNode::DbtNode { .. }))
            .count();
        assert_eq!(
            dbt_node_count, 2,
            "same model name in two pipelines must not collide"
        );
    }
}
