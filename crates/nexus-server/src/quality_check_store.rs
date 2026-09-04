use crate::db::{rewrite_placeholders, MetadataPool};
use nexus_core::QualityCheckOutcome;

/// Append-only history of native (dbt-independent) quality check results —
/// same shape/purpose as `DbtTestResultStore`, but fed by
/// `nexus_core::evaluate_quality_checks` instead of dbt's `run_results.json`.
/// One row per check per run; never overwritten, so the Quality tab can show
/// a check's pass/fail trend across runs same as it does for dbt tests.
#[derive(Clone)]
pub struct QualityCheckStore {
    pool: MetadataPool,
}

impl QualityCheckStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        let create = r#"
            CREATE TABLE IF NOT EXISTS quality_check_results (
                pipeline_id TEXT NOT NULL,
                run_id BIGINT NOT NULL,
                column_name TEXT NOT NULL,
                check_name TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                recorded_at TEXT NOT NULL
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_quality_check_results_pipeline_id \
                     ON quality_check_results(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_quality_check_results_pipeline_id \
                     ON quality_check_results(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    #[allow(dead_code)] // only reached from run_transform_pipeline's quality-check hook
    pub async fn record_all(
        &self,
        pipeline_id: &str,
        run_id: i64,
        results: &[QualityCheckOutcome],
    ) -> anyhow::Result<()> {
        let recorded_at = chrono::Utc::now().to_rfc3339();
        let sql = self.q("INSERT INTO quality_check_results \
             (pipeline_id, run_id, column_name, check_name, status, message, recorded_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)");
        for r in results {
            match &self.pool {
                MetadataPool::Sqlite(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(pipeline_id)
                        .bind(run_id)
                        .bind(&r.column)
                        .bind(&r.check)
                        .bind(&r.status)
                        .bind(&r.message)
                        .bind(&recorded_at)
                        .execute(p)
                        .await?;
                }
                MetadataPool::Postgres(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(pipeline_id)
                        .bind(run_id)
                        .bind(&r.column)
                        .bind(&r.check)
                        .bind(&r.status)
                        .bind(&r.message)
                        .bind(&recorded_at)
                        .execute(p)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Every recorded result for `pipeline_id`, ordered oldest-first —
    /// powers `GET /pipelines/{id}/quality-checks`.
    pub async fn list_for_pipeline(
        &self,
        pipeline_id: &str,
    ) -> anyhow::Result<Vec<QualityCheckOutcome>> {
        let sql = self.q(
            "SELECT column_name, check_name, status, message FROM quality_check_results \
             WHERE pipeline_id = ? ORDER BY recorded_at",
        );
        let rows: Vec<(String, String, String, Option<String>)> = match &self.pool {
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
            .map(|(column, check, status, message)| QualityCheckOutcome {
                column,
                check,
                status,
                message,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(column: &str, check: &str, status: &str) -> QualityCheckOutcome {
        QualityCheckOutcome {
            column: column.to_string(),
            check: check.to_string(),
            status: status.to_string(),
            message: None,
        }
    }

    #[tokio::test]
    async fn record_all_persists_every_check_individually() {
        let store = QualityCheckStore::connect("sqlite::memory:").await.unwrap();
        let results = vec![
            outcome("id", "not_null", "pass"),
            outcome("email", "unique", "fail"),
        ];
        store.record_all("pipe-1", 42, &results).await.unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored
            .iter()
            .any(|r| r.column == "id" && r.check == "not_null" && r.status == "pass"));
        assert!(stored
            .iter()
            .any(|r| r.column == "email" && r.check == "unique" && r.status == "fail"));
    }

    #[tokio::test]
    async fn record_all_is_append_only_across_runs() {
        let store = QualityCheckStore::connect("sqlite::memory:").await.unwrap();
        store
            .record_all("pipe-1", 1, &[outcome("id", "not_null", "pass")])
            .await
            .unwrap();
        store
            .record_all("pipe-1", 2, &[outcome("id", "not_null", "fail")])
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
        let store = QualityCheckStore::connect("sqlite::memory:").await.unwrap();
        let mut r = outcome("id", "not_null", "fail");
        r.message = Some("3 null values found".to_string());
        store.record_all("pipe-1", 1, &[r]).await.unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored[0].message.as_deref(), Some("3 null values found"));
    }

    #[tokio::test]
    async fn postgres_backend_records_all() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = QualityCheckStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store
            .record_all("pipe-1", 1, &[outcome("id", "not_null", "pass")])
            .await
            .unwrap();
        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored.len(), 1);
    }
}
