use crate::db::{rewrite_placeholders, MetadataPool};
use serde::Serialize;
use std::borrow::Cow;

/// One RAG answer (LLMOPS_IMPLEMENTATION_PLAN.md Marco L5) — "this
/// generation came from which row/chunk/source". Insert-only: a generation
/// is a historical fact about one `POST /rag/query` call, never edited
/// after the fact (same immutability principle as `prompt_template_store`/
/// `license_store`).
#[derive(Clone)]
pub struct LlmGenerationStore {
    pool: MetadataPool,
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmGeneration {
    pub id: i64,
    pub pipeline_id: String,
    pub question: String,
    pub answer: String,
    pub prompt_name: String,
    pub prompt_version: u32,
    pub model: String,
    pub tokens_prompt: i64,
    pub tokens_completion: i64,
    /// The single vector-sink resource the context was drawn from — same
    /// identifier shape as `lineage::resource_identifier` (`None` when the
    /// sink's connector/config didn't resolve to a stable identifier).
    pub resource_id: Option<String>,
    /// Primary-key values of the retrieved context rows, in the order the
    /// vector search returned them. Stored as a JSON array string (neither
    /// SQLite nor a dual-dialect-safe subset of Postgres has a portable
    /// native array column here) — parsed back out in `get`.
    pub context_keys: Vec<String>,
    pub created_at: String,
}

pub struct NewGeneration<'a> {
    pub pipeline_id: &'a str,
    pub question: &'a str,
    pub answer: &'a str,
    pub prompt_name: &'a str,
    pub prompt_version: u32,
    pub model: &'a str,
    pub tokens_prompt: u32,
    pub tokens_completion: u32,
    pub resource_id: Option<&'a str>,
    pub context_keys: &'a [String],
}

impl LlmGenerationStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS llm_generations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_id TEXT NOT NULL,
                        question TEXT NOT NULL,
                        answer TEXT NOT NULL,
                        prompt_name TEXT NOT NULL,
                        prompt_version INTEGER NOT NULL,
                        model TEXT NOT NULL,
                        tokens_prompt INTEGER NOT NULL,
                        tokens_completion INTEGER NOT NULL,
                        resource_id TEXT,
                        context_keys_json TEXT NOT NULL,
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
                    CREATE TABLE IF NOT EXISTS llm_generations (
                        id BIGSERIAL PRIMARY KEY,
                        pipeline_id TEXT NOT NULL,
                        question TEXT NOT NULL,
                        answer TEXT NOT NULL,
                        prompt_name TEXT NOT NULL,
                        prompt_version INTEGER NOT NULL,
                        model TEXT NOT NULL,
                        tokens_prompt BIGINT NOT NULL,
                        tokens_completion BIGINT NOT NULL,
                        resource_id TEXT,
                        context_keys_json TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    pub async fn record(&self, new: NewGeneration<'_>) -> Result<i64, sqlx::Error> {
        let context_keys_json =
            serde_json::to_string(new.context_keys).expect("Vec<String> always serializes");
        let sql = self.q("INSERT INTO llm_generations \
             (pipeline_id, question, answer, prompt_name, prompt_version, model, \
              tokens_prompt, tokens_completion, resource_id, context_keys_json) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        let id: i64 = match &self.pool {
            MetadataPool::Sqlite(p) => {
                let result = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(new.pipeline_id)
                    .bind(new.question)
                    .bind(new.answer)
                    .bind(new.prompt_name)
                    .bind(new.prompt_version as i64)
                    .bind(new.model)
                    .bind(new.tokens_prompt as i64)
                    .bind(new.tokens_completion as i64)
                    .bind(new.resource_id)
                    .bind(&context_keys_json)
                    .execute(p)
                    .await?;
                result.last_insert_rowid()
            }
            MetadataPool::Postgres(p) => {
                let sql_returning = format!("{sql} RETURNING id");
                sqlx::query_scalar(sqlx::AssertSqlSafe(sql_returning))
                    .bind(new.pipeline_id)
                    .bind(new.question)
                    .bind(new.answer)
                    .bind(new.prompt_name)
                    .bind(new.prompt_version as i64)
                    .bind(new.model)
                    .bind(new.tokens_prompt as i64)
                    .bind(new.tokens_completion as i64)
                    .bind(new.resource_id)
                    .bind(&context_keys_json)
                    .fetch_one(p)
                    .await?
            }
        };
        Ok(id)
    }

    pub async fn get(&self, id: i64) -> Result<Option<LlmGeneration>, sqlx::Error> {
        type Row = (
            i64,
            String,
            String,
            String,
            String,
            i64,
            String,
            i64,
            i64,
            Option<String>,
            String,
            String,
        );
        let sql = self.q(
            "SELECT id, pipeline_id, question, answer, prompt_name, prompt_version, model, \
             tokens_prompt, tokens_completion, resource_id, context_keys_json, created_at \
             FROM llm_generations WHERE id = ?",
        );
        let row: Option<Row> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(row.map(
            |(
                id,
                pipeline_id,
                question,
                answer,
                prompt_name,
                prompt_version,
                model,
                tokens_prompt,
                tokens_completion,
                resource_id,
                context_keys_json,
                created_at,
            )| LlmGeneration {
                id,
                pipeline_id,
                question,
                answer,
                prompt_name,
                prompt_version: prompt_version as u32,
                model,
                tokens_prompt,
                tokens_completion,
                resource_id,
                context_keys: serde_json::from_str(&context_keys_json).unwrap_or_default(),
                created_at,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(context_keys: &[String]) -> NewGeneration<'_> {
        NewGeneration {
            pipeline_id: "p1",
            question: "What is NexusFlow?",
            answer: "A data movement framework.",
            prompt_name: "rag-answer",
            prompt_version: 1,
            model: "gpt-test",
            tokens_prompt: 42,
            tokens_completion: 8,
            resource_id: Some("lancedb:docs"),
            context_keys,
        }
    }

    #[tokio::test]
    async fn record_then_get_round_trips() {
        let store = LlmGenerationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let keys = vec!["1".to_string(), "2".to_string()];
        let id = store.record(sample(&keys)).await.unwrap();

        let generation = store.get(id).await.unwrap().unwrap();
        assert_eq!(generation.pipeline_id, "p1");
        assert_eq!(generation.answer, "A data movement framework.");
        assert_eq!(generation.tokens_prompt, 42);
        assert_eq!(generation.resource_id.as_deref(), Some("lancedb:docs"));
        assert_eq!(generation.context_keys, keys);
    }

    #[tokio::test]
    async fn unknown_id_is_none() {
        let store = LlmGenerationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        assert!(store.get(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn resource_id_defaults_to_none_when_not_resolvable() {
        let store = LlmGenerationStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let keys = vec!["1".to_string(), "2".to_string()];
        let mut new = sample(&keys);
        new.resource_id = None;
        let id = store.record(new).await.unwrap();

        let generation = store.get(id).await.unwrap().unwrap();
        assert_eq!(generation.resource_id, None);
    }
}
