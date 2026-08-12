use serde::Deserialize;

/// SSL/TLS mode for the PostgreSQL connection used by the pgvector sink.
///
/// Maps to the `sslmode` parameter in PostgreSQL connection strings.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PgVectorSslMode {
    /// Only try a non-SSL connection.
    Disable,
    /// First try a non-SSL connection; if that fails, try an SSL connection.
    Allow,
    /// First try an SSL connection; if that fails, try a non-SSL connection.
    #[default]
    Prefer,
    /// Only try an SSL connection. If a root CA file is present, verify the
    /// server certificate in the same way as if `verify-ca` was specified.
    Require,
    /// Only try an SSL connection, and verify that the server certificate is
    /// issued by a trusted certificate authority (CA).
    VerifyCa,
    /// Only try an SSL connection. Verify that the server certificate is
    /// issued by a trusted CA and that the requested server host name matches
    /// that in the certificate.
    VerifyFull,
}

/// pgvector sink config — the AI Lakehouse destination for Marco 5
/// (chunk → embedding → pgvector). See ARCHITECTURE.md §4.3/§8.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill the individual connection fields (`host`, `port`, `username`, ...),
/// which are assembled into a PostgreSQL connection string when `uri` is empty.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PgVectorConnectorConfig {
    /// Full `postgresql://user:pass@host:port/db` URI — the `pgvector`
    /// extension must already be enabled on this database
    /// (`CREATE EXTENSION vector`).
    ///
    /// When provided, this value is used exactly as-is and all other
    /// connection fields are ignored. Keeps backward compatibility with older
    /// pipelines that already store a complete connection string.
    #[serde(default)]
    pub uri: Option<String>,
    /// Database server host name or IP address.
    #[serde(default = "default_host")]
    pub host: String,
    /// TCP port the PostgreSQL server is listening on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// User name to authenticate with.
    pub username: String,
    /// Password for the provided user name.
    #[serde(default)]
    pub password: String,
    /// Database name to connect to.
    pub database: String,
    /// Schema / `search_path` to use for the target table.
    ///
    /// Defaults to the user's default search path (usually `public`). Only
    /// used when building the connection string from individual fields.
    #[serde(default)]
    pub schema: Option<String>,
    /// SSL/TLS negotiation mode for this connection.
    ///
    /// Defaults to `prefer`: try SSL first, fall back to plaintext if the
    /// server does not support it.
    #[serde(default)]
    pub ssl_mode: PgVectorSslMode,
    /// Table name — must already exist with a `vector(N)` column matching
    /// `embedding_column`/`dimension`; this sink only writes rows.
    pub table: String,
    /// Column used to upsert on write.
    pub primary_key: String,
    /// Name of the `vector(N)` column the embedding is written to.
    pub embedding_column: String,
    /// Must match the `vector(N)` column's declared width.
    pub dimension: usize,
    /// Timeout in seconds for connecting and for each batch write (begin +
    /// upsert/delete + commit) — `tokio_postgres` has no timeout of its own,
    /// so a stalled connection would otherwise block the pipeline
    /// indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl PgVectorConnectorConfig {
    /// Returns the connection string to hand to `tokio_postgres`.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise a
    /// `postgresql://` URI is built from the individual fields.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = &self.uri {
            return uri.clone();
        }

        let password_part = percent_encode(&self.password);
        let mut uri = format!(
            "postgresql://{}:{}@{}:{}/{}",
            percent_encode(&self.username),
            password_part,
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        );

        let mut params: Vec<String> = Vec::new();
        if let Some(schema) = &self.schema {
            params.push(format!(
                "options=-csearch_path%3D{}",
                percent_encode(schema)
            ));
        }
        let ssl_mode = match self.ssl_mode {
            PgVectorSslMode::Disable => "disable",
            PgVectorSslMode::Allow => "allow",
            PgVectorSslMode::Prefer => "prefer",
            PgVectorSslMode::Require => "require",
            PgVectorSslMode::VerifyCa => "verify-ca",
            PgVectorSslMode::VerifyFull => "verify-full",
        };
        if ssl_mode != "prefer" {
            params.push(format!("sslmode={ssl_mode}"));
        }

        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

fn default_host() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    5432
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Minimal percent-encoding helper for connection-string components.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '%' | '@' | ':' | '/' | '?' | '#' | '[' | ']' | ' ' => {
                for b in c.encode_utf8(&mut [0; 4]).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string_returns_uri_when_present() {
        let cfg = PgVectorConnectorConfig {
            uri: Some("postgresql://legacy".to_string()),
            host: "ignored".to_string(),
            port: 9999,
            username: "ignored".to_string(),
            password: "ignored".to_string(),
            database: "ignored".to_string(),
            schema: None,
            ssl_mode: PgVectorSslMode::Disable,
            table: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_string(), "postgresql://legacy");
    }

    #[test]
    fn connection_string_builds_from_fields() {
        let cfg = PgVectorConnectorConfig {
            uri: None,
            host: "db.example.com".to_string(),
            port: 5433,
            username: "nexus".to_string(),
            password: "s3cr@t".to_string(),
            database: "vectors".to_string(),
            schema: Some("ai".to_string()),
            ssl_mode: PgVectorSslMode::Require,
            table: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        let cs = cfg.connection_string();
        assert!(cs.starts_with("postgresql://nexus:s3cr%40t@db.example.com:5433/vectors"));
        assert!(cs.contains("options=-csearch_path%3Dai"));
        assert!(cs.contains("sslmode=require"));
    }
}
