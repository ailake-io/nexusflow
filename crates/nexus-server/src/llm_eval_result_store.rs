use crate::db::{rewrite_placeholders, MetadataPool};

/// One golden eval case's result, decoupled from `nexus_ai::llm::LlmEvalOutcome`
/// — that type lives behind the `llm` Cargo feature and has no
/// `prompt_version` (the caller, `runner.rs`, resolves that separately, same
/// reasoning `apply_llm_stage` already has for `resolved_version`). This
/// store (like `DbtTestResultStore`/`QualityCheckStore`) always compiles
/// regardless of which features are on; `Serialize` is what
/// `GET /pipelines/{id}/llm-eval-results` (the Quality tab) sends over the
/// wire.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct LlmEvalOutcome {
    pub eval_name: String,
    pub prompt_version: u32,
    pub score: f64,
    pub passed: bool,
    /// The LLM's actual answer for this case — kept for inspection when a
    /// case fails, same convention as `DbtTestOutcome.message`/
    /// `QualityCheckOutcome.message` (shown by the Quality tab only when the
    /// latest result isn't a pass).
    pub message: Option<String>,
}

/// Append-only history of golden-dataset eval results
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7) — one row per case per run, so
/// the Quality tab can show a case's score trend across runs/prompt
/// versions, same as it already does for dbt tests and native quality
/// checks.
#[derive(Clone)]
pub struct LlmEvalResultStore {
    pool: MetadataPool,
}

impl LlmEvalResultStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        let create = r#"
            CREATE TABLE IF NOT EXISTS llm_eval_results (
                pipeline_id TEXT NOT NULL,
                run_id BIGINT NOT NULL,
                eval_name TEXT NOT NULL,
                prompt_version INTEGER NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                passed BOOLEAN NOT NULL,
                message TEXT,
                recorded_at TEXT NOT NULL
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_llm_eval_results_pipeline_id \
                     ON llm_eval_results(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_llm_eval_results_pipeline_id \
                     ON llm_eval_results(pipeline_id)",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    // Only called from `runner.rs`'s `#[cfg(feature = "llm")]` eval hook —
    // see `DbtTestResultStore::record_all`'s identical note.
    #[allow(dead_code)]
    pub async fn record_all(
        &self,
        pipeline_id: &str,
        run_id: i64,
        results: &[LlmEvalOutcome],
    ) -> anyhow::Result<()> {
        let recorded_at = chrono::Utc::now().to_rfc3339();
        let sql = self.q("INSERT INTO llm_eval_results \
             (pipeline_id, run_id, eval_name, prompt_version, score, passed, message, recorded_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        for r in results {
            match &self.pool {
                MetadataPool::Sqlite(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(pipeline_id)
                        .bind(run_id)
                        .bind(&r.eval_name)
                        .bind(r.prompt_version)
                        .bind(r.score)
                        .bind(r.passed)
                        .bind(&r.message)
                        .bind(&recorded_at)
                        .execute(p)
                        .await?;
                }
                MetadataPool::Postgres(p) => {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(pipeline_id)
                        .bind(run_id)
                        .bind(&r.eval_name)
                        .bind(r.prompt_version as i32)
                        .bind(r.score)
                        .bind(r.passed)
                        .bind(&r.message)
                        .bind(&recorded_at)
                        .execute(p)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Every recorded result for `pipeline_id`, grouped by eval case and
    /// ordered oldest-first within each group — what the Quality tab renders
    /// as a per-case score history. Powers `GET /pipelines/{id}/llm-eval-results`.
    pub async fn list_for_pipeline(&self, pipeline_id: &str) -> anyhow::Result<Vec<LlmEvalOutcome>> {
        let sql = self.q(
            "SELECT eval_name, prompt_version, score, passed, message FROM llm_eval_results \
             WHERE pipeline_id = ? ORDER BY eval_name, recorded_at",
        );
        let rows: Vec<(String, i32, f64, bool, Option<String>)> = match &self.pool {
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
            .map(|(eval_name, prompt_version, score, passed, message)| LlmEvalOutcome {
                eval_name,
                prompt_version: prompt_version as u32,
                score,
                passed,
                message,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(eval_name: &str, prompt_version: u32, score: f64) -> LlmEvalOutcome {
        LlmEvalOutcome {
            eval_name: eval_name.to_string(),
            prompt_version,
            score,
            passed: score >= 0.5,
            message: None,
        }
    }

    #[tokio::test]
    async fn record_all_persists_every_case_individually() {
        let store = LlmEvalResultStore::connect("sqlite::memory:").await.unwrap();
        let results = vec![outcome("golden-1", 1, 0.9), outcome("golden-2", 1, 0.1)];
        store.record_all("pipe-1", 42, &results).await.unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored
            .iter()
            .any(|r| r.eval_name == "golden-1" && r.passed && r.score == 0.9));
        assert!(stored
            .iter()
            .any(|r| r.eval_name == "golden-2" && !r.passed && r.score == 0.1));
    }

    #[tokio::test]
    async fn record_all_is_append_only_across_runs() {
        let store = LlmEvalResultStore::connect("sqlite::memory:").await.unwrap();
        store
            .record_all("pipe-1", 1, &[outcome("golden-1", 1, 0.9)])
            .await
            .unwrap();
        store
            .record_all("pipe-1", 2, &[outcome("golden-1", 2, 0.3)])
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
    async fn record_all_preserves_the_answer_text() {
        let store = LlmEvalResultStore::connect("sqlite::memory:").await.unwrap();
        let mut r = outcome("golden-1", 1, 0.1);
        r.message = Some("wrong answer".to_string());
        store.record_all("pipe-1", 1, &[r]).await.unwrap();

        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored[0].message.as_deref(), Some("wrong answer"));
    }

    #[tokio::test]
    async fn postgres_backend_records_all() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = LlmEvalResultStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store
            .record_all("pipe-1", 1, &[outcome("golden-1", 1, 0.9)])
            .await
            .unwrap();
        let stored = store.list_for_pipeline("pipe-1").await.unwrap();
        assert_eq!(stored.len(), 1);
    }
}
