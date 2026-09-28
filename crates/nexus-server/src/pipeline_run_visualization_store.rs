//! One rendered chart per run (`PipelineSpec.visualization`, ROADMAP.md
//! Fase 31) — `run_id` is the primary key, not an autoincrement id: a run
//! renders at most one chart, so there's nothing to list, only
//! upsert-and-fetch-by-run_id (`GET /pipelines/{id}/runs/{run_id}/visualization`).

use crate::db::{rewrite_placeholders, MetadataPool};
use std::borrow::Cow;

#[derive(Clone)]
pub struct PipelineRunVisualizationStore {
    pool: MetadataPool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredVisualization {
    pub image_bytes: Vec<u8>,
    pub content_type: String,
}

impl PipelineRunVisualizationStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_run_visualizations (
                        run_id INTEGER PRIMARY KEY,
                        image_bytes BLOB NOT NULL,
                        content_type TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_run_visualizations (
                        run_id BIGINT PRIMARY KEY,
                        image_bytes BYTEA NOT NULL,
                        content_type TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    pub async fn store(
        &self,
        run_id: i64,
        image_bytes: &[u8],
        content_type: &str,
    ) -> Result<(), sqlx::Error> {
        let sql = self.q(
            "INSERT INTO pipeline_run_visualizations (run_id, image_bytes, content_type) \
             VALUES (?, ?, ?) \
             ON CONFLICT(run_id) DO UPDATE SET \
                image_bytes = excluded.image_bytes, content_type = excluded.content_type",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .bind(image_bytes)
                    .bind(content_type)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .bind(image_bytes)
                    .bind(content_type)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn get(&self, run_id: i64) -> Result<Option<StoredVisualization>, sqlx::Error> {
        let sql = self.q(
            "SELECT image_bytes, content_type FROM pipeline_run_visualizations WHERE run_id = ?",
        );
        let row: Option<(Vec<u8>, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(row.map(|(image_bytes, content_type)| StoredVisualization {
            image_bytes,
            content_type,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_chart_means_no_row() {
        let store = PipelineRunVisualizationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        assert!(store.get(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stores_and_fetches_a_chart() {
        let store = PipelineRunVisualizationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.store(1, b"png-bytes", "image/png").await.unwrap();

        let stored = store.get(1).await.unwrap().unwrap();
        assert_eq!(stored.image_bytes, b"png-bytes");
        assert_eq!(stored.content_type, "image/png");
    }

    #[tokio::test]
    async fn storing_again_for_the_same_run_replaces_the_chart() {
        let store = PipelineRunVisualizationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.store(1, b"first", "image/png").await.unwrap();
        store.store(1, b"second", "text/plain").await.unwrap();

        let stored = store.get(1).await.unwrap().unwrap();
        assert_eq!(stored.image_bytes, b"second");
        assert_eq!(stored.content_type, "text/plain");
    }

    #[tokio::test]
    async fn different_runs_are_independent() {
        let store = PipelineRunVisualizationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.store(1, b"run-1", "image/png").await.unwrap();
        store.store(2, b"run-2", "image/png").await.unwrap();

        assert_eq!(store.get(1).await.unwrap().unwrap().image_bytes, b"run-1");
        assert_eq!(store.get(2).await.unwrap().unwrap().image_bytes, b"run-2");
    }
}
