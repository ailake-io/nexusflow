use crate::db::{rewrite_placeholders, MetadataPool};
use std::borrow::Cow;

/// Aggregate LLM token/cost usage for one pipeline run
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L2) — one row per `run_id`,
/// incremented on every LLM call within that run rather than recomputed at
/// the end, same reasoning `RunLogger` persists each log line on emission
/// instead of buffering until the run finishes.
#[derive(Clone)]
pub struct PipelineRunLlmStatsStore {
    pool: MetadataPool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LlmRunStats {
    pub tokens_prompt: i64,
    pub tokens_completion: i64,
    pub cost_estimate: f64,
}

impl PipelineRunLlmStatsStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_run_llm_stats (
                        run_id INTEGER PRIMARY KEY,
                        tokens_prompt INTEGER NOT NULL DEFAULT 0,
                        tokens_completion INTEGER NOT NULL DEFAULT 0,
                        cost_estimate REAL NOT NULL DEFAULT 0
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pipeline_run_llm_stats (
                        run_id BIGINT PRIMARY KEY,
                        tokens_prompt BIGINT NOT NULL DEFAULT 0,
                        tokens_completion BIGINT NOT NULL DEFAULT 0,
                        cost_estimate DOUBLE PRECISION NOT NULL DEFAULT 0
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Adds one call's usage to `run_id`'s running total — creates the row
    /// on the first call of a run, increments on every call after.
    pub async fn record_call(
        &self,
        run_id: i64,
        tokens_prompt: u32,
        tokens_completion: u32,
        cost_estimate: f64,
    ) -> Result<(), sqlx::Error> {
        let sql = self.q(
            r#"
            INSERT INTO pipeline_run_llm_stats (run_id, tokens_prompt, tokens_completion, cost_estimate)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                tokens_prompt = pipeline_run_llm_stats.tokens_prompt + excluded.tokens_prompt,
                tokens_completion = pipeline_run_llm_stats.tokens_completion + excluded.tokens_completion,
                cost_estimate = pipeline_run_llm_stats.cost_estimate + excluded.cost_estimate
            "#,
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .bind(tokens_prompt as i64)
                    .bind(tokens_completion as i64)
                    .bind(cost_estimate)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .bind(tokens_prompt as i64)
                    .bind(tokens_completion as i64)
                    .bind(cost_estimate)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn get(&self, run_id: i64) -> Result<Option<LlmRunStats>, sqlx::Error> {
        let sql = self.q("SELECT tokens_prompt, tokens_completion, cost_estimate \
             FROM pipeline_run_llm_stats WHERE run_id = ?");
        let row: Option<(i64, i64, f64)> = match &self.pool {
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
        Ok(row.map(
            |(tokens_prompt, tokens_completion, cost_estimate)| LlmRunStats {
                tokens_prompt,
                tokens_completion,
                cost_estimate,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_calls_means_no_row() {
        let store = PipelineRunLlmStatsStore::connect("sqlite::memory:")
            .await
            .unwrap();
        assert!(store.get(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn records_and_aggregates_multiple_calls() {
        let store = PipelineRunLlmStatsStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.record_call(1, 10, 2, 0.001).await.unwrap();
        store.record_call(1, 20, 4, 0.002).await.unwrap();

        let stats = store.get(1).await.unwrap().unwrap();
        assert_eq!(stats.tokens_prompt, 30);
        assert_eq!(stats.tokens_completion, 6);
        assert!((stats.cost_estimate - 0.003).abs() < 1e-9);
    }

    #[tokio::test]
    async fn different_runs_are_tracked_independently() {
        let store = PipelineRunLlmStatsStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.record_call(1, 10, 2, 0.001).await.unwrap();
        store.record_call(2, 5, 1, 0.0005).await.unwrap();

        assert_eq!(store.get(1).await.unwrap().unwrap().tokens_prompt, 10);
        assert_eq!(store.get(2).await.unwrap().unwrap().tokens_prompt, 5);
    }
}
