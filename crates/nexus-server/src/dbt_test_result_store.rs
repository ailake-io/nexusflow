use crate::db::{rewrite_placeholders, MetadataPool};

/// One dbt test's outcome, decoupled from `dbt::DbtRunResult` — that type
/// only exists behind the `dbt` Cargo feature, and this store (like
/// `DbtLineageStore`) needs to always compile regardless of which features
/// are on. The `dbt` feature's own persistence call site maps `DbtRunResult`
/// into this shape before calling `record_all`.
#[allow(dead_code)] // constructed only from lib.rs's `#[cfg(feature = "dbt")]` block
#[derive(Debug, Clone, PartialEq)]
pub struct DbtTestOutcome {
    pub unique_id: String,
    /// "pass"/"fail"/"warn" — dbt's own vocabulary, passed through as-is.
    pub status: String,
    pub message: Option<String>,
    pub execution_time: f64,
}

/// Append-only history of individual dbt test results — not the aggregate
/// pass/fail counts `DbtRunSummary` already persists in `pipeline_runs`
/// (`pipeline_store.rs`), but which test, in which run, with what message.
/// dbt's own CLI doesn't retain this across runs (that's dbt Cloud's paid
/// artifact storage, or a third-party tool) — nexus-server already parses
/// `run_results.json` for every dbt run anyway (`dbt.rs`), so persisting the
/// per-test detail instead of discarding it after logging is the only new
/// work. No read endpoint yet — that's the Quality tab, a later phase; this
/// only stops throwing away data that's already computed.
#[derive(Clone)]
pub struct DbtTestResultStore {
    pool: MetadataPool,
}

impl DbtTestResultStore {
    #[allow(dead_code)] // only reached via `record_all`, see its own note
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        let create = r#"
            CREATE TABLE IF NOT EXISTS dbt_test_results (
                pipeline_id TEXT NOT NULL,
                run_id BIGINT NOT NULL,
                unique_id TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                execution_time DOUBLE PRECISION NOT NULL,
                recorded_at TEXT NOT NULL
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_dbt_test_results_pipeline_id \
                     ON dbt_test_results(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_dbt_test_results_pipeline_id \
                     ON dbt_test_results(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    // Only called from lib.rs's `#[cfg(feature = "dbt")]` block — see
    // `DbtLineageStore::record`'s identical note.
    #[allow(dead_code)]
    pub async fn record_all(
        &self,
        pipeline_id: &str,
        run_id: i64,
        results: &[DbtTestOutcome],
    ) -> anyhow::Result<()> {
        let recorded_at = chrono::Utc::now().to_rfc3339();
        let sql = self.q("INSERT INTO dbt_test_results \
             (pipeline_id, run_id, unique_id, status, message, execution_time, recorded_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)");
        for r in results {
            match &self.pool {
                MetadataPool::Sqlite(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(pipeline_id)
                        .bind(run_id)
                        .bind(&r.unique_id)
                        .bind(&r.status)
                        .bind(&r.message)
                        .bind(r.execution_time)
                        .bind(&recorded_at)
                        .execute(p)
                        .await?;
                }
                MetadataPool::Postgres(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(pipeline_id)
                        .bind(run_id)
                        .bind(&r.unique_id)
                        .bind(&r.status)
                        .bind(&r.message)
                        .bind(r.execution_time)
                        .bind(&recorded_at)
                        .execute(p)
                        .await?;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn list_for_pipeline(&self, pipeline_id: &str) -> anyhow::Result<Vec<DbtTestOutcome>> {
        let sql = self.q(
            "SELECT unique_id, status, message, execution_time FROM dbt_test_results \
             WHERE pipeline_id = ? ORDER BY unique_id",
        );
        let rows: Vec<(String, String, Option<String>, f64)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(
                |(unique_id, status, message, execution_time)| DbtTestOutcome {
                    unique_id,
                    status,
                    message,
                    execution_time,
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(unique_id: &str, status: &str) -> DbtTestOutcome {
        DbtTestOutcome {
            unique_id: unique_id.to_string(),
            status: status.to_string(),
            message: None,
            execution_time: 0.1,
        }
    }

    #[tokio::test]
    async fn record_all_persists_every_test_individually() {
        let store = DbtTestResultStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let results = vec![
            outcome("test.proj.not_null_orders_id", "pass"),
            outcome("test.proj.unique_orders_id", "fail"),
        ];
        store.record_all("pipe-1", 42, &results).await.unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored
            .iter()
            .any(|r| r.unique_id == "test.proj.not_null_orders_id" && r.status == "pass"));
        assert!(stored
            .iter()
            .any(|r| r.unique_id == "test.proj.unique_orders_id" && r.status == "fail"));
    }

    #[tokio::test]
    async fn record_all_is_append_only_across_runs() {
        let store = DbtTestResultStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store
            .record_all("pipe-1", 1, &[outcome("test.proj.a", "pass")])
            .await
            .unwrap();
        store
            .record_all("pipe-1", 2, &[outcome("test.proj.a", "fail")])
            .await
            .unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(
            stored.len(),
            2,
            "both runs' results must survive, not overwrite"
        );
    }

    #[tokio::test]
    async fn record_all_preserves_the_failure_message() {
        let store = DbtTestResultStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let mut r = outcome("test.proj.a", "fail");
        r.message = Some("42 rows failed".to_string());
        store.record_all("pipe-1", 1, &[r]).await.unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored[0].message.as_deref(), Some("42 rows failed"));
    }

    #[tokio::test]
    async fn postgres_backend_records_all() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = DbtTestResultStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store
            .record_all("pipe-1", 1, &[outcome("test.proj.a", "pass")])
            .await
            .unwrap();
        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored.len(), 1);
    }
}
