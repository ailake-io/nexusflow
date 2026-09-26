//! Persists `AgentSpec`s (ROADMAP.md Fase 31), same shape as
//! `pipeline_store.rs`: encrypted at rest via `SecretCipher` because
//! `AgentToolKind::QueryData`/`GenerateChart` embed a `NodeSpec`, which can
//! carry a connector config with a raw secret in it exactly like a
//! `PipelineSpec` source/sink can (CLAUDE.md §5). Deliberately its own
//! table/store rather than reusing `pipelines` — an agent isn't a pipeline
//! (`agent.rs`'s own doc comment), so it doesn't belong in a table whose
//! rows the scheduler/run engine already assume are `PipelineSpec`s.
//!
//! `#[allow(dead_code)]`: this store lands ahead of its consumer
//! (`agent.rs`'s handlers, ROADMAP.md Fase 31 checklist item after this
//! one) — every method here is exercised by its own tests but nothing in
//! the crate calls it yet. Remove the allow once `agent.rs` exists.
#![allow(dead_code)]

use crate::crypto::SecretCipher;
use crate::db::{rewrite_placeholders, MetadataPool};
use nexus_core::AgentSpec;
use std::borrow::Cow;

#[derive(Debug, thiserror::Error)]
pub enum AgentStoreError {
    #[error("agent {0:?} already exists")]
    AlreadyExists(String),
    #[error("agent {0:?} not found")]
    NotFound(String),
    #[error("stored agent is corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Clone)]
pub struct AgentStore {
    pool: MetadataPool,
}

/// Same reasoning as `pipeline_store::encode_spec` — `pub(crate)` so a
/// future version-history hook (if one ever covers agents too) can commit
/// the exact ciphertext this store persists, without re-encrypting.
pub(crate) fn encode_spec(spec: &AgentSpec, cipher: &SecretCipher) -> String {
    let json = serde_json::to_string(spec).expect("AgentSpec always serializes");
    cipher.encrypt(&json)
}

pub(crate) fn decode_spec(
    ciphertext: &str,
    cipher: &SecretCipher,
) -> Result<AgentSpec, AgentStoreError> {
    let json = cipher
        .decrypt(ciphertext)
        .map_err(|e| AgentStoreError::Corrupt(e.to_string()))?;
    serde_json::from_str(&json).map_err(|e| AgentStoreError::Corrupt(e.to_string()))
}

impl AgentStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS agents (
                        id TEXT PRIMARY KEY,
                        spec_ciphertext TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (datetime('now')),
                        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                        created_by TEXT,
                        updated_by TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS agents (
                        id TEXT PRIMARY KEY,
                        spec_ciphertext TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        updated_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        created_by TEXT,
                        updated_by TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    fn is_unique_violation(e: &sqlx::Error) -> bool {
        match e {
            sqlx::Error::Database(db) => {
                let code = db.code().map(|c| c.to_string());
                matches!(code.as_deref(), Some("2067" | "23505"))
                    || db.message().to_lowercase().contains("unique constraint")
            }
            _ => false,
        }
    }

    pub async fn create(
        &self,
        spec: &AgentSpec,
        cipher: &SecretCipher,
        author: &str,
    ) -> Result<(), AgentStoreError> {
        let ciphertext = encode_spec(spec, cipher);
        let sql = self.q(
            "INSERT INTO agents (id, spec_ciphertext, created_by, updated_by) VALUES (?, ?, ?, ?)",
        );
        let result: Result<(), sqlx::Error> = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(&spec.agent_id)
                .bind(&ciphertext)
                .bind(author)
                .bind(author)
                .execute(p)
                .await
                .map(|_| ()),
            MetadataPool::Postgres(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(&spec.agent_id)
                .bind(&ciphertext)
                .bind(author)
                .bind(author)
                .execute(p)
                .await
                .map(|_| ()),
        };
        match result {
            Ok(()) => Ok(()),
            Err(e) if Self::is_unique_violation(&e) => {
                Err(AgentStoreError::AlreadyExists(spec.agent_id.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn update(
        &self,
        id: &str,
        spec: &AgentSpec,
        cipher: &SecretCipher,
        author: &str,
    ) -> Result<(), AgentStoreError> {
        let ciphertext = encode_spec(spec, cipher);
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE agents SET spec_ciphertext = ?, updated_at = datetime('now'), updated_by = ? WHERE id = ?",
                )))
                .bind(&ciphertext)
                .bind(author)
                .bind(id)
                .execute(p)
                .await?
                .rows_affected()
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(self.q(
                    "UPDATE agents SET spec_ciphertext = ?, updated_at = (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')), updated_by = ? WHERE id = ?",
                )))
                .bind(&ciphertext)
                .bind(author)
                .bind(id)
                .execute(p)
                .await?
                .rows_affected()
            }
        };
        if rows_affected == 0 {
            return Err(AgentStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    pub async fn get_spec(
        &self,
        id: &str,
        cipher: &SecretCipher,
    ) -> Result<AgentSpec, AgentStoreError> {
        let sql = self.q("SELECT spec_ciphertext FROM agents WHERE id = ?");
        let row: Option<(String,)> = match &self.pool {
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
        let (ciphertext,) = row.ok_or_else(|| AgentStoreError::NotFound(id.to_string()))?;
        decode_spec(&ciphertext, cipher)
    }

    pub async fn list_all_specs(
        &self,
        cipher: &SecretCipher,
    ) -> Result<Vec<AgentSpec>, AgentStoreError> {
        let sql = self.q("SELECT spec_ciphertext FROM agents ORDER BY created_at");
        let rows: Vec<(String,)> = match &self.pool {
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
        rows.into_iter()
            .map(|(ciphertext,)| decode_spec(&ciphertext, cipher))
            .collect()
    }

    pub async fn delete(&self, id: &str) -> Result<(), AgentStoreError> {
        let sql = self.q("DELETE FROM agents WHERE id = ?");
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .execute(p)
                .await?
                .rows_affected(),
            MetadataPool::Postgres(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .execute(p)
                .await?
                .rows_affected(),
        };
        if rows_affected == 0 {
            return Err(AgentStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::{AgentToolConfig, AgentToolKind, ApprovalMode};
    use nexus_core::{LlmModelConfig, PromptRef};

    fn test_cipher() -> SecretCipher {
        SecretCipher::from_hex_key(&"ab".repeat(32)).unwrap()
    }

    fn sample_spec(agent_id: &str) -> AgentSpec {
        AgentSpec {
            agent_id: agent_id.to_string(),
            name: "Test Agent".to_string(),
            prompt: PromptRef {
                name: "test-prompt".to_string(),
                version: None,
            },
            model: LlmModelConfig::Api {
                base_url: "https://api.openai.com/v1".to_string(),
                model: "gpt-4o-mini".to_string(),
                api_key_env: Some("OPENAI_API_KEY".to_string()),
                cost_per_1k_prompt_tokens: None,
                cost_per_1k_completion_tokens: None,
            },
            tools: vec![AgentToolConfig {
                tool: AgentToolKind::SearchVectors {
                    pipeline_id: "docs-pipeline".to_string(),
                    top_k: 5,
                },
                approval: ApprovalMode::Auto,
            }],
            max_steps: 8,
            schedule: None,
        }
    }

    #[tokio::test]
    async fn create_and_get_roundtrips() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        let spec = sample_spec("agent-1");
        store.create(&spec, &cipher, "alice").await.unwrap();

        let fetched = store.get_spec("agent-1", &cipher).await.unwrap();
        assert_eq!(fetched.agent_id, "agent-1");
        assert_eq!(fetched.max_steps, 8);
    }

    #[tokio::test]
    async fn create_rejects_duplicate_id() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        let spec = sample_spec("agent-1");
        store.create(&spec, &cipher, "alice").await.unwrap();

        let err = store.create(&spec, &cipher, "alice").await.unwrap_err();
        assert!(matches!(err, AgentStoreError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn get_missing_agent_is_not_found() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        let err = store.get_spec("nope", &cipher).await.unwrap_err();
        assert!(matches!(err, AgentStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn update_persists_changes() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        let mut spec = sample_spec("agent-1");
        store.create(&spec, &cipher, "alice").await.unwrap();

        spec.max_steps = 20;
        store
            .update("agent-1", &spec, &cipher, "bob")
            .await
            .unwrap();

        let fetched = store.get_spec("agent-1", &cipher).await.unwrap();
        assert_eq!(fetched.max_steps, 20);
    }

    #[tokio::test]
    async fn update_missing_agent_is_not_found() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        let spec = sample_spec("agent-1");
        let err = store
            .update("agent-1", &spec, &cipher, "alice")
            .await
            .unwrap_err();
        assert!(matches!(err, AgentStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn list_all_specs_returns_every_agent() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        store
            .create(&sample_spec("agent-1"), &cipher, "alice")
            .await
            .unwrap();
        store
            .create(&sample_spec("agent-2"), &cipher, "alice")
            .await
            .unwrap();

        let all = store.list_all_specs(&cipher).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn delete_removes_agent() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let cipher = test_cipher();
        store
            .create(&sample_spec("agent-1"), &cipher, "alice")
            .await
            .unwrap();

        store.delete("agent-1").await.unwrap();
        let err = store.get_spec("agent-1", &cipher).await.unwrap_err();
        assert!(matches!(err, AgentStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn delete_missing_agent_is_not_found() {
        let store = AgentStore::connect("sqlite::memory:").await.unwrap();
        let err = store.delete("nope").await.unwrap_err();
        assert!(matches!(err, AgentStoreError::NotFound(_)));
    }
}
