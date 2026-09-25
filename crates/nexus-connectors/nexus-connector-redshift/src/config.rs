use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// Deliberately simpler than `nexus-connector-postgres`'s config
/// (public repo): no legacy `uri` field, no `ssl_mode` choice (Redshift
/// clusters commonly require SSL, so this always connects with
/// `sslmode=require` rather than exposing a knob most deployments would
/// never turn off). v1 only supports username/password auth — Redshift
/// also supports IAM-based temporary credentials
/// (`GetClusterCredentials`, no fixed password, the AWS-recommended
/// method for production), documented here as a real fast-follow, not
/// implemented because it would pull in the AWS SDK just for this one
/// auth path.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RedshiftConnectorConfig {
    /// Cluster endpoint hostname (or Redshift Serverless workgroup
    /// endpoint).
    pub host: String,
    /// Port. Defaults to `5439` — Redshift's default, different from
    /// Postgres's `5432`.
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    /// Table name to read from (source) or write to (sink).
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// side; ignored by the source. Upserts use `MERGE INTO` (GA in
    /// Redshift since April 2023 — real SQL command, not simulated via
    /// staging tables the way older Redshift upsert guides describe).
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for each ADBC call. Defaults to `30` — same
    /// as Postgres's default in the public repo. Unlike Snowflake/
    /// BigQuery, a Redshift cluster doesn't have a "suspended
    /// warehouse" cold-start to account for (provisioned clusters stay
    /// up; Redshift Serverless resumes fast enough that the shorter
    /// default is fine).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ADBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl RedshiftConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector("redshift: host is required".to_string()));
        }
        if self.database.trim().is_empty() {
            return Err(NexusError::Connector("redshift: database is required".to_string()));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector("redshift: username is required".to_string()));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector("redshift: password is required".to_string()));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector("redshift: table is required".to_string()));
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "redshift: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Returns the `postgresql://` connection string to hand to the
    /// ADBC Postgres driver — Redshift is wire-compatible with the
    /// Postgres protocol (it's a fork of Postgres 8.0), so the same
    /// driver connects to it unmodified once given the right host/port.
    pub fn connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode=require",
            percent_encode(&self.username),
            percent_encode(&self.password),
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        )
    }
}

fn default_port() -> u16 {
    5439
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Minimal percent-encoding helper for connection-string components —
/// same approach `nexus-connector-postgres`'s `config.rs` uses (public
/// repo).
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
