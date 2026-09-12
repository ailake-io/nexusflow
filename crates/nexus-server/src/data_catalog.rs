use crate::db::{rewrite_placeholders, MetadataPool};
use crate::lineage::{resource_identifier, ResourceKind};
use crate::pipeline_schema_store::{ColumnInfo, PipelineSchema};
use nexus_core::{NodeSpec, PipelineSpec};
use serde::{Deserialize, Serialize};

/// Same `"resource::{connector}::{identifier}"` shape as
/// `lineage::resource_node_id` — deliberately recomputed here (not called
/// directly) so this module only depends on the public `resource_identifier`
/// function, not `lineage`'s `pub(crate)` id helper. Any dataset registered
/// here shares its key with the matching `Resource` node in the lineage
/// graph, so the frontend can cross-link the two without a lookup table.
fn dataset_key(connector: &str, identifier: &str) -> String {
    format!("resource::{connector}::{identifier}")
}

fn resource_kind_to_str(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Table => "table",
        ResourceKind::Collection => "collection",
        ResourceKind::Topic => "topic",
        ResourceKind::File => "file",
    }
}

fn resource_kind_from_str(s: &str) -> ResourceKind {
    match s {
        "collection" => ResourceKind::Collection,
        "topic" => ResourceKind::Topic,
        "file" => ResourceKind::File,
        // "table" and any unrecognized legacy value both fall back to
        // Table — this is cosmetic only (see `ResourceKind`'s doc comment
        // in lineage.rs), never worth a hard error.
        _ => ResourceKind::Table,
    }
}

/// One column of a cataloged dataset — `data_type` is filled in from the
/// most recent `PipelineSchema` capture that touched this dataset (`None`
/// if a column was only ever manually annotated, never actually observed
/// on a run — shouldn't normally happen, but isn't treated as corruption).
/// `description`/`pii_flag` are user-edited metadata (Fase 25 decision:
/// manual only, no automatic PII heuristic) and are never overwritten by
/// discovery — only `data_type` is refreshed on every run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CatalogColumn {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub pii_flag: bool,
}

/// One dataset in the catalog — a resource (table/collection/topic/file)
/// one or more pipelines read from or write to, identified the same way
/// `lineage.rs` identifies a `Resource` node. `columns` is the union of
/// every column ever observed for this dataset across runs (a column is
/// never removed automatically when a pipeline stops touching it — the
/// catalog is a "what have we ever seen here" index, not a live schema
/// mirror; `pipeline_schema_store` already owns "current schema").
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CatalogDataset {
    pub dataset_key: String,
    pub connector: String,
    pub resource_kind: ResourceKind,
    pub identifier: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub tags: Vec<String>,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub columns: Vec<CatalogColumn>,
}

impl CatalogDataset {
    pub fn has_pii(&self) -> bool {
        self.columns.iter().any(|c| c.pii_flag)
    }
}

/// Search/filter parameters for `GET /catalog/datasets` — every field is
/// optional and AND-combined. Filtering happens in Rust after loading every
/// dataset (see `CatalogStore::list`'s doc comment for why), so this struct
/// is plain data, not a SQL fragment builder.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CatalogFilter {
    pub q: Option<String>,
    pub tag: Option<String>,
    pub connector: Option<String>,
    pub owner: Option<String>,
    pub has_pii: Option<bool>,
}

impl CatalogFilter {
    fn matches(&self, dataset: &CatalogDataset) -> bool {
        if let Some(q) = self.q.as_deref().filter(|s| !s.is_empty()) {
            let q = q.to_lowercase();
            let haystack = format!(
                "{} {} {}",
                dataset.dataset_key,
                dataset.identifier,
                dataset.description.as_deref().unwrap_or("")
            )
            .to_lowercase();
            if !haystack.contains(&q) {
                return false;
            }
        }
        if let Some(tag) = self.tag.as_deref().filter(|s| !s.is_empty()) {
            if !dataset.tags.iter().any(|t| t == tag) {
                return false;
            }
        }
        if let Some(connector) = self.connector.as_deref().filter(|s| !s.is_empty()) {
            if dataset.connector != connector {
                return false;
            }
        }
        if let Some(owner) = self.owner.as_deref().filter(|s| !s.is_empty()) {
            if dataset.owner.as_deref() != Some(owner) {
                return false;
            }
        }
        if let Some(has_pii) = self.has_pii {
            if dataset.has_pii() != has_pii {
                return false;
            }
        }
        true
    }
}

/// Searchable index of datasets a pipeline reads from or writes to —
/// populated automatically from `execute_pipeline_run`'s success hook
/// (same event `pipeline_schema_store`/`quality_check_store` already key
/// off of), edited manually via `PUT /catalog/datasets/{key}` and
/// `PUT /catalog/datasets/{key}/columns/{column}`. Unlike `lineage.rs`'s
/// whole-catalog graph (recomputed on every `GET /lineage` request), this
/// is a materialized index — it has to be, to support search/filter without
/// re-scanning every saved pipeline spec on every request.
#[derive(Clone)]
pub struct CatalogStore {
    pool: MetadataPool,
}

impl CatalogStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        let datasets = r#"
            CREATE TABLE IF NOT EXISTS catalog_datasets (
                dataset_key TEXT NOT NULL PRIMARY KEY,
                connector TEXT NOT NULL,
                resource_kind TEXT NOT NULL,
                identifier TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
        "#;
        let dataset_metadata = r#"
            CREATE TABLE IF NOT EXISTS catalog_dataset_metadata (
                dataset_key TEXT NOT NULL PRIMARY KEY,
                description TEXT,
                owner TEXT,
                tags_json TEXT NOT NULL
            )
        "#;
        let column_metadata = r#"
            CREATE TABLE IF NOT EXISTS catalog_column_metadata (
                dataset_key TEXT NOT NULL,
                column_name TEXT NOT NULL,
                data_type TEXT,
                description TEXT,
                pii_flag BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (dataset_key, column_name)
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(datasets).execute(p).await?;
                sqlx::query(dataset_metadata).execute(p).await?;
                sqlx::query(column_metadata).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(datasets).execute(p).await?;
                sqlx::query(dataset_metadata).execute(p).await?;
                sqlx::query(column_metadata).execute(p).await?;
            }
        }

        Ok(Self { pool })
    }

    /// Discovers/refreshes every dataset a just-succeeded pipeline touches
    /// — called from `execute_pipeline_run`'s success branch, fire-and-
    /// forget from the caller's perspective (same posture as
    /// `pipeline_schema_store::record`/`quality_check_store::record_all`).
    /// `schema` is the pipeline's freshly captured `PipelineSchema` (`None`
    /// if capture failed or hasn't happened yet — datasets still get
    /// registered, just without columns this round).
    ///
    /// Source columns are only attributed when the pipeline has exactly
    /// one source: `PipelineSchema.source_columns` is a single flattened
    /// list, not split per source node, so attaching it to more than one
    /// source dataset would risk mislabeling one source's columns as
    /// another's. Output columns don't have this ambiguity — every sink in
    /// a run receives the same final batch stream — so they're always
    /// attached to every sink dataset.
    pub async fn record_from_pipeline(
        &self,
        spec: &PipelineSpec,
        schema: Option<&PipelineSchema>,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        let source_columns = schema
            .filter(|_| spec.sources.len() == 1)
            .map(|s| s.source_columns.as_slice());
        for node in &spec.sources {
            self.upsert_dataset(node, source_columns, &now).await?;
        }

        let output_columns = schema.map(|s| s.output_columns.as_slice());
        for node in &spec.sinks {
            self.upsert_dataset(node, output_columns, &now).await?;
        }

        Ok(())
    }

    async fn upsert_dataset(
        &self,
        node: &NodeSpec,
        columns: Option<&[ColumnInfo]>,
        now: &str,
    ) -> anyhow::Result<()> {
        let Some((kind, identifier)) = resource_identifier(&node.connector, &node.config) else {
            // Unrecognized connector or missing identifying field — same
            // "stays unlinked, never a panic" posture as
            // `lineage::add_resource_node`.
            return Ok(());
        };
        let key = dataset_key(&node.connector, &identifier);

        let sql = self.q(
            "INSERT INTO catalog_datasets \
                (dataset_key, connector, resource_kind, identifier, first_seen_at, last_seen_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (dataset_key) DO UPDATE SET last_seen_at = excluded.last_seen_at",
        );
        let resource_kind = resource_kind_to_str(kind);
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(&key)
                    .bind(&node.connector)
                    .bind(resource_kind)
                    .bind(&identifier)
                    .bind(now)
                    .bind(now)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(&key)
                    .bind(&node.connector)
                    .bind(resource_kind)
                    .bind(&identifier)
                    .bind(now)
                    .bind(now)
                    .execute(p)
                    .await?;
            }
        }

        let Some(columns) = columns else {
            return Ok(());
        };
        // Only `data_type` is refreshed by discovery — `description`/
        // `pii_flag` are user-edited and must survive re-discovery
        // untouched (the whole point of splitting them into their own
        // table instead of storing everything on `catalog_datasets`).
        let col_sql = self.q(
            "INSERT INTO catalog_column_metadata (dataset_key, column_name, data_type, pii_flag) \
             VALUES (?, ?, ?, FALSE) \
             ON CONFLICT (dataset_key, column_name) DO UPDATE SET data_type = excluded.data_type",
        );
        for col in columns {
            match &self.pool {
                MetadataPool::Sqlite(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(col_sql.clone()))
                        .bind(&key)
                        .bind(&col.name)
                        .bind(&col.data_type)
                        .execute(p)
                        .await?;
                }
                MetadataPool::Postgres(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(col_sql.clone()))
                        .bind(&key)
                        .bind(&col.name)
                        .bind(&col.data_type)
                        .execute(p)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Every dataset the catalog knows about, matching `filter`. Filtering
    /// happens in Rust, not SQL: the catalog is expected to hold at most a
    /// few hundred datasets (one per distinct resource a pipeline touches,
    /// not per row of data), so loading everything and filtering in memory
    /// is simpler and safer than hand-building a dynamic, dual-dialect
    /// `WHERE` clause for 5 independent optional filters — revisit only if
    /// a real deployment's dataset count makes this measurably slow.
    pub async fn list(&self, filter: &CatalogFilter) -> anyhow::Result<Vec<CatalogDataset>> {
        let all = self.load_all().await?;
        Ok(all.into_iter().filter(|d| filter.matches(d)).collect())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<CatalogDataset>> {
        Ok(self.load_all().await?.into_iter().find(|d| d.dataset_key == key))
    }

    /// Distinct tags across every dataset, sorted — powers a tag-filter
    /// dropdown in the frontend without it needing to derive the list from
    /// a full dataset listing itself.
    pub async fn list_tags(&self) -> anyhow::Result<Vec<String>> {
        let all = self.load_all().await?;
        let mut tags: Vec<String> = all.into_iter().flat_map(|d| d.tags).collect();
        tags.sort_unstable();
        tags.dedup();
        Ok(tags)
    }

    async fn load_all(&self) -> anyhow::Result<Vec<CatalogDataset>> {
        let sql = self.q(
            "SELECT d.dataset_key, d.connector, d.resource_kind, d.identifier, \
                    d.first_seen_at, d.last_seen_at, m.description, m.owner, m.tags_json \
             FROM catalog_datasets d \
             LEFT JOIN catalog_dataset_metadata m ON m.dataset_key = d.dataset_key",
        );
        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query_as(sqlx::AssertSqlSafe(sql)).fetch_all(p).await?,
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql)).fetch_all(p).await?
            }
        };

        let col_sql = self.q(
            "SELECT dataset_key, column_name, data_type, description, pii_flag \
             FROM catalog_column_metadata",
        );
        let col_rows: Vec<(String, String, Option<String>, Option<String>, bool)> =
            match &self.pool {
                MetadataPool::Sqlite(p) => {
                    sqlx::query_as(sqlx::AssertSqlSafe(col_sql)).fetch_all(p).await?
                }
                MetadataPool::Postgres(p) => {
                    sqlx::query_as(sqlx::AssertSqlSafe(col_sql)).fetch_all(p).await?
                }
            };

        let mut datasets: Vec<CatalogDataset> = rows
            .into_iter()
            .map(
                |(
                    dataset_key,
                    connector,
                    resource_kind,
                    identifier,
                    first_seen_at,
                    last_seen_at,
                    description,
                    owner,
                    tags_json,
                )| {
                    let tags = tags_json
                        .as_deref()
                        .and_then(|j| serde_json::from_str::<Vec<String>>(j).ok())
                        .unwrap_or_default();
                    CatalogDataset {
                        dataset_key,
                        connector,
                        resource_kind: resource_kind_from_str(&resource_kind),
                        identifier,
                        description,
                        owner,
                        tags,
                        first_seen_at,
                        last_seen_at,
                        columns: Vec::new(),
                    }
                },
            )
            .collect();

        for (dataset_key, column_name, data_type, description, pii_flag) in col_rows {
            if let Some(dataset) = datasets.iter_mut().find(|d| d.dataset_key == dataset_key) {
                dataset.columns.push(CatalogColumn {
                    name: column_name,
                    data_type,
                    description,
                    pii_flag,
                });
            }
        }
        for dataset in &mut datasets {
            dataset.columns.sort_by(|a, b| a.name.cmp(&b.name));
        }

        Ok(datasets)
    }

    /// `Ok(false)` when `dataset_key` isn't a known dataset yet — the
    /// handler turns that into a 404 rather than silently creating
    /// metadata for a resource no pipeline has ever actually touched.
    pub async fn update_dataset_metadata(
        &self,
        dataset_key: &str,
        description: Option<String>,
        owner: Option<String>,
        tags: Vec<String>,
    ) -> anyhow::Result<bool> {
        if self.get(dataset_key).await?.is_none() {
            return Ok(false);
        }
        let tags_json = serde_json::to_string(&tags)?;
        let sql = self.q(
            "INSERT INTO catalog_dataset_metadata (dataset_key, description, owner, tags_json) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT (dataset_key) DO UPDATE SET \
                 description = excluded.description, \
                 owner = excluded.owner, \
                 tags_json = excluded.tags_json",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(dataset_key)
                    .bind(&description)
                    .bind(&owner)
                    .bind(&tags_json)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(dataset_key)
                    .bind(&description)
                    .bind(&owner)
                    .bind(&tags_json)
                    .execute(p)
                    .await?;
            }
        }
        Ok(true)
    }

    /// `Ok(false)` when `dataset_key` isn't known yet — same reasoning as
    /// `update_dataset_metadata`. Unlike the dataset itself, a column is
    /// allowed to be annotated (e.g. flagged PII) before it's ever been
    /// observed by a run, so this never checks `catalog_column_metadata`
    /// for a pre-existing row, only that the parent dataset exists.
    pub async fn update_column_metadata(
        &self,
        dataset_key: &str,
        column_name: &str,
        description: Option<String>,
        pii_flag: bool,
    ) -> anyhow::Result<bool> {
        if self.get(dataset_key).await?.is_none() {
            return Ok(false);
        }
        let sql = self.q(
            "INSERT INTO catalog_column_metadata \
                (dataset_key, column_name, data_type, description, pii_flag) \
             VALUES (?, ?, NULL, ?, ?) \
             ON CONFLICT (dataset_key, column_name) DO UPDATE SET \
                 description = excluded.description, \
                 pii_flag = excluded.pii_flag",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(dataset_key)
                    .bind(column_name)
                    .bind(&description)
                    .bind(pii_flag)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(dataset_key)
                    .bind(column_name)
                    .bind(&description)
                    .bind(pii_flag)
                    .execute(p)
                    .await?;
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_schema_store::ColumnInfo as SchemaColumnInfo;

    fn node(connector: &str, config: serde_json::Value) -> NodeSpec {
        NodeSpec {
            name: None,
            connector: connector.to_string(),
            config,
        }
    }

    fn spec(sources: Vec<NodeSpec>, sinks: Vec<NodeSpec>) -> PipelineSpec {
        PipelineSpec {
            pipeline_id: "pipe-1".to_string(),
            sources,
            transform: None,
            sinks,
            embedding: None,
            llm: None,
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

    fn cols(pairs: &[(&str, &str)]) -> Vec<SchemaColumnInfo> {
        pairs
            .iter()
            .map(|(n, t)| SchemaColumnInfo {
                name: n.to_string(),
                data_type: t.to_string(),
            })
            .collect()
    }

    #[tokio::test]
    async fn record_from_pipeline_registers_source_and_sink_datasets() {
        let store = CatalogStore::connect("sqlite::memory:").await.unwrap();
        let s = spec(
            vec![node(
                "postgres",
                serde_json::json!({"database": "app", "table": "events"}),
            )],
            vec![node(
                "postgres",
                serde_json::json!({"database": "app", "table": "events_copy"}),
            )],
        );
        let schema = PipelineSchema {
            pipeline_id: "pipe-1".to_string(),
            source_columns: cols(&[("id", "Int64")]),
            output_columns: cols(&[("id", "Int64")]),
            column_lineage: None,
            captured_at: "now".to_string(),
            last_drift: None,
        };
        store.record_from_pipeline(&s, Some(&schema)).await.unwrap();

        let datasets = store.list(&CatalogFilter::default()).await.unwrap();
        assert_eq!(datasets.len(), 2);
        let source_ds = datasets
            .iter()
            .find(|d| d.identifier == "app.events")
            .expect("source dataset registered");
        assert_eq!(source_ds.columns.len(), 1);
        assert_eq!(source_ds.columns[0].name, "id");
        assert!(datasets.iter().any(|d| d.identifier == "app.events_copy"));
    }

    #[tokio::test]
    async fn record_from_pipeline_skips_unrecognized_connector_without_error() {
        let store = CatalogStore::connect("sqlite::memory:").await.unwrap();
        let s = spec(
            vec![node("totally-unknown-connector", serde_json::json!({}))],
            vec![node(
                "csv",
                serde_json::json!({"path": "/data/out.csv"}),
            )],
        );
        store.record_from_pipeline(&s, None).await.unwrap();

        let datasets = store.list(&CatalogFilter::default()).await.unwrap();
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].identifier, "/data/out.csv");
    }

    #[tokio::test]
    async fn record_from_pipeline_does_not_attribute_columns_with_multiple_sources() {
        let store = CatalogStore::connect("sqlite::memory:").await.unwrap();
        let s = spec(
            vec![
                node("csv", serde_json::json!({"path": "/a.csv"})),
                node("csv", serde_json::json!({"path": "/b.csv"})),
            ],
            vec![node("csv", serde_json::json!({"path": "/out.csv"}))],
        );
        let schema = PipelineSchema {
            pipeline_id: "pipe-1".to_string(),
            source_columns: cols(&[("id", "Int64")]),
            output_columns: cols(&[("id", "Int64")]),
            column_lineage: None,
            captured_at: "now".to_string(),
            last_drift: None,
        };
        store.record_from_pipeline(&s, Some(&schema)).await.unwrap();

        let datasets = store.list(&CatalogFilter::default()).await.unwrap();
        let a = datasets.iter().find(|d| d.identifier == "/a.csv").unwrap();
        let b = datasets.iter().find(|d| d.identifier == "/b.csv").unwrap();
        assert!(a.columns.is_empty(), "ambiguous source, must not guess");
        assert!(b.columns.is_empty(), "ambiguous source, must not guess");
        let out = datasets.iter().find(|d| d.identifier == "/out.csv").unwrap();
        assert_eq!(out.columns.len(), 1, "sink columns are never ambiguous");
    }

    #[tokio::test]
    async fn rediscovery_refreshes_data_type_but_preserves_manual_metadata() {
        let store = CatalogStore::connect("sqlite::memory:").await.unwrap();
        let s = spec(
            vec![node("csv", serde_json::json!({"path": "/a.csv"}))],
            vec![node("csv", serde_json::json!({"path": "/out.csv"}))],
        );
        let schema = PipelineSchema {
            pipeline_id: "pipe-1".to_string(),
            source_columns: cols(&[("email", "Utf8")]),
            output_columns: cols(&[("email", "Utf8")]),
            column_lineage: None,
            captured_at: "now".to_string(),
            last_drift: None,
        };
        store.record_from_pipeline(&s, Some(&schema)).await.unwrap();
        let key = "resource::csv::/a.csv";
        store
            .update_column_metadata(key, "email", Some("customer email".to_string()), true)
            .await
            .unwrap();

        // Second run, column type changes.
        let schema2 = PipelineSchema {
            source_columns: cols(&[("email", "LargeUtf8")]),
            ..schema
        };
        store.record_from_pipeline(&s, Some(&schema2)).await.unwrap();

        let dataset = store.get(key).await.unwrap().unwrap();
        let col = dataset.columns.iter().find(|c| c.name == "email").unwrap();
        assert_eq!(col.data_type.as_deref(), Some("LargeUtf8"));
        assert_eq!(col.description.as_deref(), Some("customer email"));
        assert!(col.pii_flag, "manual PII flag must survive rediscovery");
    }

    #[tokio::test]
    async fn update_dataset_metadata_rejects_unknown_dataset() {
        let store = CatalogStore::connect("sqlite::memory:").await.unwrap();
        let updated = store
            .update_dataset_metadata("resource::csv::/nope.csv", None, None, vec![])
            .await
            .unwrap();
        assert!(!updated);
    }

    #[tokio::test]
    async fn filter_by_has_pii_and_tag_and_connector() {
        let store = CatalogStore::connect("sqlite::memory:").await.unwrap();
        let s = spec(
            vec![node("csv", serde_json::json!({"path": "/a.csv"}))],
            vec![node("csv", serde_json::json!({"path": "/out.csv"}))],
        );
        store.record_from_pipeline(&s, None).await.unwrap();
        store
            .update_dataset_metadata(
                "resource::csv::/out.csv",
                Some("final output".to_string()),
                Some("data-eng".to_string()),
                vec!["gold".to_string()],
            )
            .await
            .unwrap();
        store
            .update_column_metadata("resource::csv::/a.csv", "ssn", None, true)
            .await
            .unwrap();

        let pii_only = store
            .list(&CatalogFilter {
                has_pii: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(pii_only.len(), 1);
        assert_eq!(pii_only[0].identifier, "/a.csv");

        let tagged = store
            .list(&CatalogFilter {
                tag: Some("gold".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(tagged.len(), 1);
        assert_eq!(tagged[0].owner.as_deref(), Some("data-eng"));

        let by_connector = store
            .list(&CatalogFilter {
                connector: Some("csv".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_connector.len(), 2);
    }

    #[tokio::test]
    async fn postgres_backend_records_and_lists() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = CatalogStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        let s = spec(
            vec![node("csv", serde_json::json!({"path": "/a.csv"}))],
            vec![node("csv", serde_json::json!({"path": "/out.csv"}))],
        );
        store.record_from_pipeline(&s, None).await.unwrap();
        let datasets = store.list(&CatalogFilter::default()).await.unwrap();
        assert_eq!(datasets.len(), 2);
    }
}
