use crate::auth::Role;
use crate::db::{rewrite_placeholders, MetadataPool};
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use std::borrow::Cow;

fn validate_username(username: &str) -> anyhow::Result<()> {
    if username.is_empty() {
        anyhow::bail!("username must not be empty");
    }
    if username.len() > 64 {
        anyhow::bail!("username must not exceed 64 characters");
    }
    if !username
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        anyhow::bail!("username must only contain ASCII letters, digits, '_' or '-'");
    }
    Ok(())
}

/// User credentials + role, persisted separately from `CheckpointStore` (own
/// table). No public signup for MVP self-host (ARCHITECTURE.md §10,
/// ROADMAP.md Fase 7) — the only account is seeded from
/// `NEXUS_ADMIN_USERNAME`/`NEXUS_ADMIN_PASSWORD` the first time the table is
/// empty. Backed by SQLite (default) or Postgres (multi-replica deployments,
/// see `db::MetadataPool`) — the URL scheme picks the backend.
#[derive(Clone)]
pub struct AuthStore {
    pool: MetadataPool,
}

impl AuthStore {
    /// Every query below is written with `?` placeholders as a `&'static`
    /// literal; `q()` rewrites them to `$1, $2, ...` when running against
    /// Postgres (see `db::rewrite_placeholders`). `'static` (not just any
    /// lifetime) so the result satisfies sqlx 0.9's `SqlSafeStr` bound via
    /// `AssertSqlSafe<Cow<'static, str>>` at each call site below — the
    /// rewrite only renumbers placeholders in a fixed literal, never
    /// incorporates untrusted input, so asserting it's injection-safe is
    /// correct.
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS users (
                        username TEXT PRIMARY KEY,
                        password_hash TEXT NOT NULL,
                        role TEXT NOT NULL
                    )
                    "#,
                )
                .execute(p)
                .await?;

                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        happened_at TEXT NOT NULL DEFAULT (datetime('now')),
                        username TEXT,
                        action TEXT NOT NULL,
                        success INTEGER NOT NULL,
                        ip TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS users (
                        username TEXT PRIMARY KEY,
                        password_hash TEXT NOT NULL,
                        role TEXT NOT NULL
                    )
                    "#,
                )
                .execute(p)
                .await?;

                // `happened_at` stays TEXT (not TIMESTAMPTZ) in the same
                // "YYYY-MM-DD HH:MM:SS" shape SQLite's `datetime('now')`
                // produces — consistent with the other 2 stores, in case a
                // future consumer ever reads this column back and expects
                // one format across both backends (nothing does today; this
                // table is write-only).
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id BIGSERIAL PRIMARY KEY,
                        happened_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
                        username TEXT,
                        action TEXT NOT NULL,
                        success BOOLEAN NOT NULL,
                        ip TEXT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Seeds the admin account from env vars if (and only if) no users
    /// exist yet — safe to call on every startup, a no-op after the first.
    /// The check+insert is expressed as a single conditional INSERT so it
    /// remains race-free even if multiple processes boot at the same time
    /// (B29).
    pub async fn seed_admin_if_empty(
        &self,
        admin_username: &str,
        admin_password: &str,
    ) -> anyhow::Result<()> {
        validate_username(admin_username)?;
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(admin_password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("password hashing failed: {e}"))?
            .to_string();
        let role_str = serde_json::to_value(Role::Admin)?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Role serializes to a string"))?
            .to_string();

        let sql = self.q("INSERT INTO users (username, password_hash, role) \
             SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM users)");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(admin_username)
                    .bind(password_hash)
                    .bind(role_str)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(admin_username)
                    .bind(password_hash)
                    .bind(role_str)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> anyhow::Result<()> {
        validate_username(username)?;
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("password hashing failed: {e}"))?
            .to_string();
        let role_str = serde_json::to_value(role)?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Role serializes to a string"))?
            .to_string();

        let sql = self.q("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .bind(password_hash)
                    .bind(role_str)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .bind(password_hash)
                    .bind(role_str)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// Records a security-relevant event durably. `ip` is optional (e.g.
    /// extracted from the HTTP connection).
    pub async fn log_security_event(
        &self,
        username: Option<&str>,
        action: &str,
        success: bool,
        ip: Option<&str>,
    ) -> anyhow::Result<()> {
        let sql =
            self.q("INSERT INTO audit_log (username, action, success, ip) VALUES (?, ?, ?, ?)");
        match &self.pool {
            // SQLite has no native BOOLEAN type; the sqlx sqlite driver
            // encodes `bool` binds as 0/1 under the hood, matching the
            // `success INTEGER` column declared above.
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .bind(action)
                    .bind(success)
                    .bind(ip)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .bind(action)
                    .bind(success)
                    .bind(ip)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn list_users(&self) -> anyhow::Result<Vec<(String, Role)>> {
        let rows: Vec<(String, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as("SELECT username, role FROM users ORDER BY username")
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as("SELECT username, role FROM users ORDER BY username")
                    .fetch_all(p)
                    .await?
            }
        };
        rows.into_iter()
            .map(|(username, role_str)| {
                let role = serde_json::from_value(serde_json::Value::String(role_str))?;
                Ok((username, role))
            })
            .collect()
    }

    pub async fn get_user(&self, username: &str) -> anyhow::Result<Option<(String, Role)>> {
        let sql = self.q("SELECT username, role FROM users WHERE username = ?");
        let row: Option<(String, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .fetch_optional(p)
                    .await?
            }
        };
        row.map(|(username, role_str)| {
            let role = serde_json::from_value(serde_json::Value::String(role_str))?;
            Ok((username, role))
        })
        .transpose()
    }

    pub async fn update_user_role(&self, username: &str, role: Role) -> anyhow::Result<bool> {
        let role_str = serde_json::to_value(role)?
            .as_str()
            .expect("Role serializes to a string")
            .to_string();
        let sql = self.q("UPDATE users SET role = ? WHERE username = ?");
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(role_str)
                .bind(username)
                .execute(p)
                .await?
                .rows_affected(),
            MetadataPool::Postgres(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(role_str)
                .bind(username)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows_affected > 0)
    }

    pub async fn delete_user(&self, username: &str) -> anyhow::Result<bool> {
        let sql = self.q("DELETE FROM users WHERE username = ?");
        let rows_affected = match &self.pool {
            MetadataPool::Sqlite(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(username)
                .execute(p)
                .await?
                .rows_affected(),
            MetadataPool::Postgres(p) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(username)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows_affected > 0)
    }

    /// Returns the user's role iff `password` matches, `None` for either an
    /// unknown username or a wrong password — deliberately not
    /// distinguishable to the caller, so a login failure never leaks which
    /// half was wrong.
    pub async fn verify(&self, username: &str, password: &str) -> anyhow::Result<Option<Role>> {
        let sql = self.q("SELECT password_hash, role FROM users WHERE username = ?");
        let row: Option<(String, String)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(username)
                    .fetch_optional(p)
                    .await?
            }
        };
        let Some((password_hash, role_str)) = row else {
            return Ok(None);
        };

        let parsed_hash = PasswordHash::new(&password_hash)
            .map_err(|e| anyhow::anyhow!("stored password hash is corrupt: {e}"))?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_err()
        {
            return Ok(None);
        }

        let role: Role = serde_json::from_value(serde_json::Value::String(role_str))?;
        Ok(Some(role))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn seed_admin_is_idempotent_and_only_runs_once() {
        let store = AuthStore::connect("sqlite::memory:").await.unwrap();
        store.seed_admin_if_empty("admin", "hunter2").await.unwrap();
        // A second call must not error or duplicate/overwrite the row —
        // count stays 1 either way since it's a no-op past the first seed.
        store
            .seed_admin_if_empty("admin", "different-password")
            .await
            .unwrap();

        assert_eq!(
            store.verify("admin", "hunter2").await.unwrap(),
            Some(Role::Admin)
        );
    }

    #[tokio::test]
    async fn verify_rejects_wrong_password() {
        let store = AuthStore::connect("sqlite::memory:").await.unwrap();
        store
            .create_user("alice", "correct-horse", Role::Write)
            .await
            .unwrap();

        assert_eq!(store.verify("alice", "wrong").await.unwrap(), None);
        assert_eq!(
            store.verify("alice", "correct-horse").await.unwrap(),
            Some(Role::Write)
        );
    }

    #[tokio::test]
    async fn verify_rejects_unknown_username() {
        let store = AuthStore::connect("sqlite::memory:").await.unwrap();
        assert_eq!(store.verify("nobody", "anything").await.unwrap(), None);
    }

    /// Proves the Postgres branch (BIGSERIAL/BOOLEAN DDL, `$N` placeholders,
    /// `bool` bind for `audit_log.success`) behaves identically to the
    /// SQLite branch already covered above — not just that it compiles.
    #[tokio::test]
    async fn postgres_backend_supports_full_lifecycle() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = AuthStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store.seed_admin_if_empty("admin", "hunter2").await.unwrap();
        store
            .seed_admin_if_empty("admin", "different-password")
            .await
            .unwrap();
        assert_eq!(
            store.verify("admin", "hunter2").await.unwrap(),
            Some(Role::Admin)
        );

        store
            .create_user("alice", "correct-horse", Role::Write)
            .await
            .unwrap();
        assert_eq!(store.verify("alice", "wrong").await.unwrap(), None);
        assert_eq!(
            store.verify("alice", "correct-horse").await.unwrap(),
            Some(Role::Write)
        );

        let users = store.list_users().await.unwrap();
        assert_eq!(users.len(), 2);
        assert!(users.contains(&("alice".to_string(), Role::Write)));

        assert!(store.update_user_role("alice", Role::Read).await.unwrap());
        assert_eq!(
            store.get_user("alice").await.unwrap(),
            Some(("alice".to_string(), Role::Read))
        );

        store
            .log_security_event(Some("alice"), "login", true, Some("127.0.0.1"))
            .await
            .unwrap();
        store
            .log_security_event(Some("mallory"), "login", false, None)
            .await
            .unwrap();

        assert!(store.delete_user("alice").await.unwrap());
        assert!(store.get_user("alice").await.unwrap().is_none());
    }
}
