use crate::db::{rewrite_placeholders, MetadataPool};
use serde::Serialize;
use std::borrow::Cow;

/// Named, versioned prompt templates (LLMOPS_IMPLEMENTATION_PLAN.md Marco
/// L4) — `create` always inserts a new version, never overwrites an
/// existing one (same immutability principle as `license_store`/
/// checkpoints: a `PipelineSpec` pinned to version 3 must keep resolving
/// to that exact text forever, even after version 4 is created).
#[derive(Clone)]
pub struct PromptTemplateStore {
    pool: MetadataPool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PromptTemplate {
    pub name: String,
    pub version: u32,
    pub template: String,
    pub created_at: String,
    /// Username (`Claims.sub`) that created this version — `None` for
    /// rows written before this column existed. Doubles as the git author
    /// when the `version-history` feature commits the same save to
    /// `git_history_store.rs` (see `lib.rs`'s `create_prompt_handler`).
    pub created_by: Option<String>,
}

impl PromptTemplateStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS prompt_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        template TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (datetime('now')),
                        created_by TEXT,
                        UNIQUE(name, version)
                    )
                    "#,
                )
                .execute(p)
                .await?;
                // Same migration pattern as `pipelines.created_by`/
                // `checkpoints.resume_state` — `CREATE TABLE IF NOT
                // EXISTS` is a no-op against a table that predates this
                // column, and SQLite has no `ADD COLUMN IF NOT EXISTS`.
                let _ = sqlx::query("ALTER TABLE prompt_templates ADD COLUMN created_by TEXT")
                    .execute(p)
                    .await;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS prompt_templates (
                        id BIGSERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        template TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        created_by TEXT,
                        UNIQUE(name, version)
                    )
                    "#,
                )
                .execute(p)
                .await?;
                sqlx::query(
                    "ALTER TABLE prompt_templates ADD COLUMN IF NOT EXISTS created_by TEXT",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Inserts the next version for `name` (1 if this is the first time
    /// `name` is used) — never overwrites. Returns the new version number.
    /// The `SELECT MAX(version)` + `INSERT` isn't wrapped in an explicit
    /// transaction: `UNIQUE(name, version)` makes a lost-update race land
    /// as a constraint-violation error on the losing insert, not a silent
    /// overwrite — acceptable here since prompt creation is a low-frequency
    /// admin action, not a hot path needing serialized-write throughput.
    pub async fn create(
        &self,
        name: &str,
        template: &str,
        author: &str,
    ) -> Result<u32, sqlx::Error> {
        let next_version = self.latest_version(name).await?.unwrap_or(0) + 1;
        let sql = self.q(
            "INSERT INTO prompt_templates (name, version, template, created_by) VALUES (?, ?, ?, ?)",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .bind(next_version as i64)
                    .bind(template)
                    .bind(author)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .bind(next_version as i64)
                    .bind(template)
                    .bind(author)
                    .execute(p)
                    .await?;
            }
        }
        Ok(next_version)
    }

    pub async fn latest_version(&self, name: &str) -> Result<Option<u32>, sqlx::Error> {
        let sql = self.q("SELECT MAX(version) FROM prompt_templates WHERE name = ?");
        let row: (Option<i64>,) = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .fetch_one(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .fetch_one(p)
                    .await?
            }
        };
        Ok(row.0.map(|v| v as u32))
    }

    /// Resolves a `PromptRef` to its template text — `version: None` means
    /// the newest version for `name`; `Some(v)` pins to exactly that one.
    pub async fn resolve(
        &self,
        name: &str,
        version: Option<u32>,
    ) -> Result<Option<String>, sqlx::Error> {
        let sql = match version {
            Some(_) => {
                self.q("SELECT template FROM prompt_templates WHERE name = ? AND version = ?")
            }
            None => self.q("SELECT template FROM prompt_templates WHERE name = ? \
                 ORDER BY version DESC LIMIT 1"),
        };
        let row: Option<(String,)> = match (&self.pool, version) {
            (MetadataPool::Sqlite(p), Some(v)) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .bind(v as i64)
                    .fetch_optional(p)
                    .await?
            }
            (MetadataPool::Sqlite(p), None) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .fetch_optional(p)
                    .await?
            }
            (MetadataPool::Postgres(p), Some(v)) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .bind(v as i64)
                    .fetch_optional(p)
                    .await?
            }
            (MetadataPool::Postgres(p), None) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(name)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(row.map(|(template,)| template))
    }

    /// Lists every version of every prompt, newest first within each name —
    /// `PromptLibrary.tsx`'s only read path (no per-name endpoint needed at
    /// this scale).
    pub async fn list(&self) -> Result<Vec<PromptTemplate>, sqlx::Error> {
        let sql = self.q(
            "SELECT name, version, template, created_at, created_by FROM prompt_templates \
             ORDER BY name ASC, version DESC",
        );
        let rows: Vec<(String, i64, String, String, Option<String>)> = match &self.pool {
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
            .map(
                |(name, version, template, created_at, created_by)| PromptTemplate {
                    name,
                    version: version as u32,
                    template,
                    created_at,
                    created_by,
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_create_is_version_1() {
        let store = PromptTemplateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let version = store
            .create("summarize", "Summarize: {text}", "alice")
            .await
            .unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn second_create_increments_the_version_without_overwriting() {
        let store = PromptTemplateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.create("summarize", "v1 text", "alice").await.unwrap();
        let v2 = store.create("summarize", "v2 text", "alice").await.unwrap();
        assert_eq!(v2, 2);

        assert_eq!(
            store.resolve("summarize", Some(1)).await.unwrap(),
            Some("v1 text".to_string())
        );
        assert_eq!(
            store.resolve("summarize", Some(2)).await.unwrap(),
            Some("v2 text".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_with_no_version_picks_the_newest() {
        let store = PromptTemplateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.create("summarize", "v1 text", "alice").await.unwrap();
        store.create("summarize", "v2 text", "bob").await.unwrap();

        assert_eq!(
            store.resolve("summarize", None).await.unwrap(),
            Some("v2 text".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_unknown_name_is_none() {
        let store = PromptTemplateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        assert_eq!(store.resolve("does-not-exist", None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_orders_by_name_then_newest_version_first() {
        let store = PromptTemplateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.create("b-prompt", "b text", "alice").await.unwrap();
        store.create("a-prompt", "a v1", "alice").await.unwrap();
        store.create("a-prompt", "a v2", "bob").await.unwrap();

        let all = store.list().await.unwrap();
        let names_versions: Vec<(&str, u32)> =
            all.iter().map(|p| (p.name.as_str(), p.version)).collect();
        assert_eq!(
            names_versions,
            vec![("a-prompt", 2), ("a-prompt", 1), ("b-prompt", 1)]
        );
    }
}
