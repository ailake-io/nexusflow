use crate::db::{rewrite_placeholders, MetadataPool};
use std::collections::HashMap;

/// Latest dbt model-to-model dependency graph per pipeline — one row per
/// `pipeline_id`, upserted on every dbt run (not a history: the Lineage tab
/// shows current state, not a trend over time — that's `DbtTestResultStore`'s
/// job instead). Stores the plain `HashMap<String, Vec<String>>` shape
/// (dbt's own "node -> its upstream nodes" `parent_map`), not
/// `dbt::DbtManifestSummary` directly — that type only exists behind the
/// `dbt` Cargo feature, and this store (like the Lineage tab's `GET
/// /lineage` endpoint) needs to always compile and exist regardless of
/// which features are on.
#[derive(Clone)]
pub struct DbtLineageStore {
    pool: MetadataPool,
}

impl DbtLineageStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        let create = r#"
            CREATE TABLE IF NOT EXISTS dbt_lineage (
                pipeline_id TEXT NOT NULL PRIMARY KEY,
                parent_map_json TEXT NOT NULL,
                captured_at TEXT NOT NULL
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
            }
        }

        Ok(Self { pool })
    }

    // Only called from `lib.rs`'s `#[cfg(feature = "dbt")]` block — a build
    // without that feature never has a `DbtOutcome` to record, but this
    // store still needs to always exist (`AppState` is feature-independent).
    #[allow(dead_code)]
    pub async fn record(
        &self,
        pipeline_id: &str,
        parent_map: &HashMap<String, Vec<String>>,
    ) -> anyhow::Result<()> {
        let parent_map_json = serde_json::to_string(parent_map)?;
        let sql = self.q(
            "INSERT INTO dbt_lineage (pipeline_id, parent_map_json, captured_at) \
             VALUES (?, ?, ?) \
             ON CONFLICT (pipeline_id) DO UPDATE SET \
                 parent_map_json = excluded.parent_map_json, \
                 captured_at = excluded.captured_at",
        );
        let captured_at = chrono::Utc::now().to_rfc3339();
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(&parent_map_json)
                    .bind(&captured_at)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(&parent_map_json)
                    .bind(&captured_at)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// Every pipeline's latest dbt lineage, keyed by `pipeline_id` — what
    /// `lineage::build_graph` merges into the pipeline/resource graph.
    /// Corrupt rows (shouldn't happen — this store is the only writer) are
    /// skipped rather than failing the whole lookup, same "don't let one
    /// bad row break the tab" posture as `resource_stats.rs`'s `range`.
    pub async fn get_all(&self) -> anyhow::Result<HashMap<String, HashMap<String, Vec<String>>>> {
        let sql = self.q("SELECT pipeline_id, parent_map_json FROM dbt_lineage");
        let rows: Vec<(String, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .filter_map(|(pipeline_id, json)| {
                serde_json::from_str(&json)
                    .ok()
                    .map(|parent_map| (pipeline_id, parent_map))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_and_get_all_round_trips_the_parent_map() {
        let store = DbtLineageStore::connect("sqlite::memory:").await.unwrap();
        let mut parent_map = HashMap::new();
        parent_map.insert(
            "model.proj.final".to_string(),
            vec!["model.proj.staging".to_string()],
        );
        store.record("pipe-1", &parent_map).await.unwrap();

        let all = store.get_all().await.unwrap();
        assert_eq!(all.get("pipe-1"), Some(&parent_map));
    }

    #[tokio::test]
    async fn record_is_an_upsert_not_a_history() {
        let store = DbtLineageStore::connect("sqlite::memory:").await.unwrap();
        let mut first = HashMap::new();
        first.insert("model.proj.a".to_string(), vec![]);
        store.record("pipe-1", &first).await.unwrap();

        let mut second = HashMap::new();
        second.insert("model.proj.b".to_string(), vec![]);
        store.record("pipe-1", &second).await.unwrap();

        let all = store.get_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all.get("pipe-1"), Some(&second));
    }

    #[tokio::test]
    async fn get_all_is_scoped_per_pipeline() {
        let store = DbtLineageStore::connect("sqlite::memory:").await.unwrap();
        store.record("pipe-1", &HashMap::new()).await.unwrap();
        store.record("pipe-2", &HashMap::new()).await.unwrap();

        let all = store.get_all().await.unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("pipe-1"));
        assert!(all.contains_key("pipe-2"));
    }

    #[tokio::test]
    async fn postgres_backend_records_and_gets_all() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = DbtLineageStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        let mut parent_map = HashMap::new();
        parent_map.insert(
            "model.proj.a".to_string(),
            vec!["source.proj.raw".to_string()],
        );
        store.record("pipe-1", &parent_map).await.unwrap();

        let all = store.get_all().await.unwrap();
        assert_eq!(all.get("pipe-1"), Some(&parent_map));
    }
}
