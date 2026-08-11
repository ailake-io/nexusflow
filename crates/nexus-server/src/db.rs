//! Shared backend abstraction for the 3 metadata stores (`auth_store`,
//! `pipeline_store`, `checkpoint_store`) — SQLite by default, Postgres as an
//! alternative for multi-replica deployments (k8s). Not `sqlx::Any`: the
//! scheduler's leader election needs a real `PgPool` for
//! `pg_try_advisory_lock` (SQLite has no equivalent concept), and none of the
//! stores use the `query!`/`query_as!` macros today (all dynamic
//! `sqlx::query`/`query_as` with runtime bind), so there's no compile-time
//! type-checking to lose by not unifying through `Any`.

use std::borrow::Cow;
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPool};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};

/// One of the two backends a metadata store can run against. Every store
/// picks the branch to execute against at each call site (see
/// `auth_store.rs`/`pipeline_store.rs`/`checkpoint_store.rs`) — `sqlx::query`
/// resolves its `DB` type parameter from whichever pool it's ultimately run
/// against, so the query-building/bind chain is written once per branch, not
/// abstracted away, in exchange for keeping native `SqlitePool`/`PgPool`
/// access (no dynamic dispatch, no lost type safety on binds).
#[derive(Clone)]
pub enum MetadataPool {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

impl MetadataPool {
    /// Detects the backend from the URL scheme (`sqlite://` vs
    /// `postgres://`/`postgresql://`) and connects — replaces each store's
    /// old direct `SqliteConnectOptions::from_str` call.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
            let options = PgConnectOptions::from_str(database_url)?;
            let pool = PgPool::connect_with(options).await?;
            Ok(Self::Postgres(pool))
        } else {
            let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
            let pool = SqlitePool::connect_with(options).await?;
            Ok(Self::Sqlite(pool))
        }
    }

    pub fn is_postgres(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }

    /// `None` on `Sqlite` — only the scheduler's leader election
    /// (`pg_try_advisory_lock`, no SQLite equivalent) needs the raw pool.
    pub fn as_postgres(&self) -> Option<&PgPool> {
        match self {
            Self::Postgres(pool) => Some(pool),
            Self::Sqlite(_) => None,
        }
    }
}

/// Every query in the 3 stores is written once, with SQLite-style `?`
/// positional placeholders — Postgres requires `$1, $2, ...` instead. Rather
/// than maintaining two copies of every query string, rewrite at the call
/// site. `?` never appears inside a string literal in any query these stores
/// issue today (checked by hand), so a naive scan is safe; if that ever
/// changes, this needs a real SQL tokenizer instead.
pub fn rewrite_placeholders(sql: &str, backend_is_postgres: bool) -> Cow<'_, str> {
    if !backend_is_postgres || !sql.contains('?') {
        return Cow::Borrowed(sql);
    }
    let mut rewritten = String::with_capacity(sql.len() + 8);
    let mut n = 0usize;
    for ch in sql.chars() {
        if ch == '?' {
            n += 1;
            rewritten.push('$');
            rewritten.push_str(&n.to_string());
        } else {
            rewritten.push(ch);
        }
    }
    Cow::Owned(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_sql_unchanged_for_sqlite() {
        let sql = "SELECT * FROM users WHERE username = ? AND role = ?";
        assert_eq!(rewrite_placeholders(sql, false), sql);
    }

    #[test]
    fn leaves_sql_without_placeholders_unchanged_for_postgres() {
        let sql = "SELECT COUNT(*) FROM users";
        assert_eq!(rewrite_placeholders(sql, true), sql);
    }

    #[test]
    fn numbers_placeholders_in_order_for_postgres() {
        let sql = "UPDATE users SET role = ? WHERE username = ?";
        assert_eq!(
            rewrite_placeholders(sql, true),
            "UPDATE users SET role = $1 WHERE username = $2"
        );
    }

    #[test]
    fn handles_many_placeholders_past_single_digit() {
        let sql = "INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        let expected = "INSERT INTO t VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)";
        assert_eq!(rewrite_placeholders(sql, true), expected);
    }
}
