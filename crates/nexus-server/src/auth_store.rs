use crate::auth::Role;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::str::FromStr;

/// User credentials + role, persisted separately from `CheckpointStore` (own
/// table, same embedded sqlite pool pattern). No public signup for MVP
/// self-host (ARCHITECTURE.md §10, ROADMAP.md Fase 7) — the only account is
/// seeded from `NEXUS_ADMIN_USERNAME`/`NEXUS_ADMIN_PASSWORD` the first time
/// the table is empty.
#[derive(Clone)]
pub struct AuthStore {
    pool: SqlitePool,
}

impl AuthStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
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
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    /// Seeds the admin account from env vars if (and only if) no users
    /// exist yet — safe to call on every startup, a no-op after the first.
    pub async fn seed_admin_if_empty(
        &self,
        admin_username: &str,
        admin_password: &str,
    ) -> anyhow::Result<()> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await?;
        if count > 0 {
            return Ok(());
        }
        self.create_user(admin_username, admin_password, Role::Admin)
            .await
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> anyhow::Result<()> {
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("password hashing failed: {e}"))?
            .to_string();
        let role_str = serde_json::to_value(role)?
            .as_str()
            .expect("Role serializes to a string")
            .to_string();

        sqlx::query("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)")
            .bind(username)
            .bind(password_hash)
            .bind(role_str)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Records a security-relevant event durably. `success` is 1/0 and `ip`
    /// is optional (e.g. extracted from the HTTP connection).
    pub async fn log_security_event(
        &self,
        username: Option<&str>,
        action: &str,
        success: bool,
        ip: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO audit_log (username, action, success, ip) VALUES (?, ?, ?, ?)")
            .bind(username)
            .bind(action)
            .bind(if success { 1 } else { 0 })
            .bind(ip)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_users(&self) -> anyhow::Result<Vec<(String, Role)>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT username, role FROM users ORDER BY username")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(username, role_str)| {
                let role = serde_json::from_value(serde_json::Value::String(role_str))?;
                Ok((username, role))
            })
            .collect()
    }

    pub async fn get_user(&self, username: &str) -> anyhow::Result<Option<(String, Role)>> {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT username, role FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?;
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
        let result = sqlx::query("UPDATE users SET role = ? WHERE username = ?")
            .bind(role_str)
            .bind(username)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_user(&self, username: &str) -> anyhow::Result<bool> {
        let result = sqlx::query("DELETE FROM users WHERE username = ?")
            .bind(username)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Returns the user's role iff `password` matches, `None` for either an
    /// unknown username or a wrong password — deliberately not
    /// distinguishable to the caller, so a login failure never leaks which
    /// half was wrong.
    pub async fn verify(&self, username: &str, password: &str) -> anyhow::Result<Option<Role>> {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT password_hash, role FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?;
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
}
