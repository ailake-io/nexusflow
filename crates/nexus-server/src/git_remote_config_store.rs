use crate::crypto::SecretCipher;
use crate::db::{rewrite_placeholders, MetadataPool};
use std::borrow::Cow;

/// Persists the single, optional GitHub remote pipeline/prompt history
/// mirrors to ("caso o usuário queira" — see the git-versioning follow-up
/// plan's Part 4). One row only, same "single active config" pattern as
/// `LicenseStore` (`id INTEGER PRIMARY KEY CHECK (id = 1)`) — v1 scope has
/// no per-tenant/multi-remote support, same single-tenant assumption the
/// rest of RBAC already makes. The personal access token is encrypted at
/// rest via the same `SecretCipher` connector secrets use (CLAUDE.md §5);
/// `remote_url` is not a secret, stored plain.
#[derive(Clone)]
pub struct GitRemoteConfigStore {
    pool: MetadataPool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GitRemoteConfig {
    pub remote_url: String,
    pub token: String,
}

impl GitRemoteConfigStore {
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS git_remote_config (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        remote_url TEXT NOT NULL,
                        token_ciphertext TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS git_remote_config (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        remote_url TEXT NOT NULL,
                        token_ciphertext TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Replaces the single configured remote (or sets it for the first
    /// time). Like `LicenseStore::install`, an upsert on the fixed `id=1`
    /// row — there is only ever one.
    pub async fn set(
        &self,
        remote_url: &str,
        token: &str,
        cipher: &SecretCipher,
    ) -> Result<(), sqlx::Error> {
        let token_ciphertext = cipher.encrypt(token);
        let sql = self.q(
            r#"
            INSERT INTO git_remote_config (id, remote_url, token_ciphertext) VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                remote_url = excluded.remote_url,
                token_ciphertext = excluded.token_ciphertext,
                updated_at = excluded.updated_at
            "#,
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(remote_url)
                    .bind(&token_ciphertext)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(remote_url)
                    .bind(&token_ciphertext)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// `None` means no remote is configured — the push mirror is simply
    /// off, not an error state (same "absent config means the feature is
    /// off" contract as `AppState.alerts`' channels).
    pub async fn get(&self, cipher: &SecretCipher) -> Result<Option<GitRemoteConfig>, sqlx::Error> {
        let sql = self.q("SELECT remote_url, token_ciphertext FROM git_remote_config WHERE id = 1");
        let row: Option<(String, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(row.and_then(|(remote_url, token_ciphertext)| {
            cipher
                .decrypt(&token_ciphertext)
                .ok()
                .map(|token| GitRemoteConfig { remote_url, token })
        }))
    }

    pub async fn delete(&self) -> Result<(), sqlx::Error> {
        let sql = self.q("DELETE FROM git_remote_config WHERE id = 1");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql)).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql)).execute(p).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> SecretCipher {
        SecretCipher::from_hex_key(&"ab".repeat(32)).unwrap()
    }

    #[tokio::test]
    async fn no_remote_configured_means_none() {
        let store = GitRemoteConfigStore::connect("sqlite::memory:").await.unwrap();
        assert!(store.get(&cipher()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_then_get_round_trips_and_the_token_is_encrypted_at_rest() {
        let store = GitRemoteConfigStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store
            .set("https://github.com/acme/pipelines.git", "ghp_secrettoken", &cipher)
            .await
            .unwrap();

        let config = store.get(&cipher).await.unwrap().unwrap();
        assert_eq!(config.remote_url, "https://github.com/acme/pipelines.git");
        assert_eq!(config.token, "ghp_secrettoken");

        let MetadataPool::Sqlite(pool) = &store.pool else {
            unreachable!("this test always connects via sqlite::memory:")
        };
        let (raw,): (String,) =
            sqlx::query_as("SELECT token_ciphertext FROM git_remote_config WHERE id = 1")
                .fetch_one(pool)
                .await
                .unwrap();
        assert!(!raw.contains("ghp_secrettoken"));
    }

    #[tokio::test]
    async fn set_twice_replaces_the_single_row_not_appends() {
        let store = GitRemoteConfigStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store
            .set("https://github.com/acme/old.git", "token-1", &cipher)
            .await
            .unwrap();
        store
            .set("https://github.com/acme/new.git", "token-2", &cipher)
            .await
            .unwrap();

        let config = store.get(&cipher).await.unwrap().unwrap();
        assert_eq!(config.remote_url, "https://github.com/acme/new.git");
        assert_eq!(config.token, "token-2");
    }

    #[tokio::test]
    async fn delete_clears_the_config() {
        let store = GitRemoteConfigStore::connect("sqlite::memory:").await.unwrap();
        let cipher = cipher();
        store
            .set("https://github.com/acme/pipelines.git", "token", &cipher)
            .await
            .unwrap();

        store.delete().await.unwrap();
        assert!(store.get(&cipher).await.unwrap().is_none());
    }
}
