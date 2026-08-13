use crate::db::{rewrite_placeholders, MetadataPool};
use crate::license::{self, LicenseClaims, LicenseError};
use std::borrow::Cow;

/// Persists the single active license key (raw JWT — its own signature
/// already makes it tamper-evident, so there's nothing extra to encrypt).
/// Only one license is active at a time: installing a new one replaces the
/// old (`docs/ENTERPRISE_LICENSING.md` v1 scope — no multi-license support).
///
/// Backed by the same `MetadataPool` abstraction as `AuthStore` and
/// `PipelineStore`, so it works with SQLite (default) or Postgres
/// (multi-replica deployments).
#[derive(Clone)]
pub struct LicenseStore {
    pool: MetadataPool,
}

#[derive(Debug, thiserror::Error)]
pub enum LicenseStoreError {
    #[error(transparent)]
    License(#[from] LicenseError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

impl LicenseStore {
    /// Every query is written with SQLite-style `?` placeholders; `q()`
    /// rewrites them to `$1, $2, ...` when running against Postgres.
    fn q(&self, sql: &'static str) -> Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS license (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        jwt TEXT NOT NULL,
                        installed_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS license (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        jwt TEXT NOT NULL,
                        installed_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    /// Verifies the key before storing it — never persist a license that
    /// wouldn't pass its own verification later.
    pub async fn install(&self, jwt: &str) -> Result<LicenseClaims, LicenseStoreError> {
        let claims = license::verify(jwt)?;
        let sql = self.q(
            r#"
            INSERT INTO license (id, jwt) VALUES (1, ?)
            ON CONFLICT(id) DO UPDATE SET jwt = excluded.jwt, installed_at = excluded.installed_at
            "#,
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql)).bind(jwt).execute(p).await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql)).bind(jwt).execute(p).await?;
            }
        }
        Ok(claims)
    }

    /// The currently active license, if one is installed and still passes
    /// verification (signature + not expired). An installed-but-now-expired
    /// license returns `None` rather than an error — from the caller's
    /// point of view, "expired" and "never installed" both mean "no active
    /// license."
    pub async fn active(&self) -> Result<Option<LicenseClaims>, sqlx::Error> {
        let sql = self.q("SELECT jwt FROM license WHERE id = 1");
        let row: Option<(String,)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql)).fetch_optional(p).await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql)).fetch_optional(p).await?
            }
        };
        Ok(row.and_then(|(jwt,)| license::verify(&jwt).ok()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::license::test_support::{claims, sign};

    #[tokio::test]
    async fn no_license_installed_means_nothing_is_licensed() {
        let store = LicenseStore::connect("sqlite::memory:").await.unwrap();
        assert!(store.active().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn install_then_active_round_trips_the_claims() {
        let store = LicenseStore::connect("sqlite::memory:").await.unwrap();
        let claims = claims(vec!["salesforce", "snowflake"]);
        let jwt = sign(&claims);

        let installed = store.install(&jwt).await.unwrap();
        assert_eq!(installed, claims);

        let active = store.active().await.unwrap().unwrap();
        assert_eq!(active, claims);
        assert!(active.covers("salesforce"));
        assert!(!active.covers("oracle"));
    }

    #[tokio::test]
    async fn installing_a_second_license_replaces_the_first() {
        let store = LicenseStore::connect("sqlite::memory:").await.unwrap();
        store
            .install(&sign(&claims(vec!["salesforce"])))
            .await
            .unwrap();
        store
            .install(&sign(&claims(vec!["snowflake"])))
            .await
            .unwrap();

        let active = store.active().await.unwrap().unwrap();
        assert!(!active.covers("salesforce"));
        assert!(active.covers("snowflake"));
    }

    #[tokio::test]
    async fn install_rejects_an_invalid_key_without_storing_it() {
        let store = LicenseStore::connect("sqlite::memory:").await.unwrap();
        assert!(store.install("not-a-jwt").await.is_err());
        assert!(store.active().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_expired_license_is_treated_as_no_active_license() {
        let store = LicenseStore::connect("sqlite::memory:").await.unwrap();
        let mut expired = claims(vec!["salesforce"]);
        // Past jsonwebtoken's default 60s leeway (see license.rs's own test).
        expired.exp = expired.iat - 120;
        // Bypass `install`'s own verification (which would reject this) by
        // writing the row directly — simulates a license that was valid
        // when installed and has since expired.
        sqlx::query("INSERT INTO license (id, jwt) VALUES (1, ?)")
            .bind(sign(&expired))
            .execute(match &store.pool {
                MetadataPool::Sqlite(p) => p,
                MetadataPool::Postgres(_) => unreachable!(),
            })
            .await
            .unwrap();

        assert!(store.active().await.unwrap().is_none());
    }
}
