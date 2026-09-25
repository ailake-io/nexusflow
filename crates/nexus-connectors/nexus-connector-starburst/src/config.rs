use serde::Deserialize;

/// Static connector config resolved at node-configuration time — see
/// `nexus-connector-trino::TrinoConnectorConfig` in the public repo for the
/// sibling this was copied from. Starburst has no free ADBC/ODBC driver
/// (JDBC/ODBC are gated behind the Starburst customer portal), but it's a
/// commercial fork of Trino that kept the exact same client-facing HTTP
/// protocol (`POST /v1/statement`, `X-Trino-*` headers) — so this crate
/// talks to it with a hand-rolled HTTP client instead (`client.rs`) rather
/// than ADBC.
///
/// `catalog`/`schema_name`/`table_name` are 3 separate fields rather than
/// one dotted string for the same reason as Trino's: `nexus_core::
/// validate_identifier` rejects `.`, so each segment is validated and
/// quoted on its own (`"catalog"."schema"."table"`, ANSI double-quote).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct StarburstConnectorConfig {
    /// Starburst coordinator host name or IP address.
    #[serde(default = "default_host")]
    pub host: String,
    /// Port the coordinator's HTTP(S) interface listens on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Identity every request is submitted as — sent as the `X-Trino-User`
    /// header on every query, independent of whatever auth mechanism below
    /// actually authenticates the request (this is how the protocol tracks
    /// query ownership even when auth is handled by e.g. LDAP mapping to a
    /// different principal).
    pub user: String,
    /// Password for HTTP Basic auth. Mutually exclusive with
    /// `access_token` in practice (only one auth mechanism is used per
    /// request) — when both are set, `access_token` (Bearer) takes
    /// precedence. Covers password- and LDAP-authenticated clusters, where
    /// the cluster itself validates the password against its configured
    /// identity provider; this connector just carries it over Basic auth.
    #[serde(default)]
    pub password: Option<String>,
    /// Bearer token for a cluster configured with OAuth2/token auth. Takes
    /// precedence over `password` when both are set.
    #[serde(default)]
    pub access_token: Option<String>,
    /// Whether to connect over TLS. Defaults to `true` — required for any
    /// real authentication (same rule as Trino's own docs).
    #[serde(default = "default_true")]
    pub ssl: bool,
    /// Whether to verify the server's TLS certificate. `false` only for a
    /// self-signed cert on a trusted internal cluster.
    #[serde(default = "default_true")]
    pub ssl_verify: bool,
    /// Catalog to query.
    pub catalog: String,
    /// Schema within `catalog`.
    pub schema_name: String,
    /// Table name to read from.
    pub table_name: String,
    /// Column used to partition reads by range for parallelism — any
    /// orderable column. `None` reads the whole table with no `WHERE`
    /// clause.
    #[serde(default)]
    pub partition_column: Option<String>,
    /// Timeout in seconds for the *entire* query — including every
    /// `nextUri` poll round-trip, not just the first request.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl StarburstConnectorConfig {
    /// Base URL for the coordinator's HTTP(S) API, no trailing slash.
    pub fn base_url(&self) -> String {
        let scheme = if self.ssl { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.port)
    }
}

fn default_host() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    443
}

fn default_true() -> bool {
    true
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> StarburstConnectorConfig {
        StarburstConnectorConfig {
            host: "localhost".to_string(),
            port: 443,
            user: "nexus".to_string(),
            password: None,
            access_token: None,
            ssl: true,
            ssl_verify: true,
            catalog: "hive".to_string(),
            schema_name: "default".to_string(),
            table_name: "events".to_string(),
            partition_column: None,
            timeout_seconds: 30,
        }
    }

    #[test]
    fn base_url_uses_https_when_ssl_is_true() {
        assert_eq!(base_cfg().base_url(), "https://localhost:443");
    }

    #[test]
    fn base_url_uses_http_when_ssl_is_false() {
        let mut cfg = base_cfg();
        cfg.ssl = false;
        cfg.host = "starburst.example.com".to_string();
        cfg.port = 8080;
        assert_eq!(cfg.base_url(), "http://starburst.example.com:8080");
    }
}
