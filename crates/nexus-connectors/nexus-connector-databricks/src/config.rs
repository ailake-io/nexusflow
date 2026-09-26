use nexus_core::NexusError;
use serde::Deserialize;

/// How to authenticate to Databricks. Values confirmed from the real ADBC
/// Driver Foundry documentation (adbc-drivers.org/drivers/databricks/,
/// 2026-08-22) — 3 connection URI forms the driver accepts, not guessed:
///
/// - `databricks://token:<pat>@<host>:<port>/<http-path>` (PAT)
/// - `databricks://<host>:<port>/<http-path>?authType=OauthU2M` (interactive)
/// - `databricks://<host>:<port>/<http-path>?authType=OAuthM2M&clientID=<id>&clientSecret=<secret>` (M2M)
///
/// Unlike Snowflake's driver (confirmed by reading its Python wheel
/// source, which exposes ~8 separate `adbc.snowflake.sql.*` option keys),
/// Databricks's driver takes the whole connection as **one** `"uri"`
/// option key — see `driver.rs`. `connection_uri()` below builds that
/// string from these typed fields.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum DatabricksAuth {
    /// Personal access token, generated in the Databricks UI (User
    /// Settings > Developer > Access tokens). Simplest option, fine for
    /// interactive/manual use; Databricks recommends OAuth M2M for
    /// unattended service accounts instead.
    PersonalAccessToken { token: String },
    /// OAuth User-to-Machine — interactive browser-based login, driver
    /// handles the flow. No credentials to store in config; not suitable
    /// for a headless/scheduled pipeline run (needs a human present the
    /// first time), included for completeness/interactive testing.
    OauthU2M,
    /// OAuth Machine-to-Machine via a Databricks service principal —
    /// Databricks's recommended method for unattended access, same role
    /// key-pair auth plays for Snowflake.
    OauthM2M {
        client_id: String,
        client_secret: String,
    },
}

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DatabricksConnectorConfig {
    /// Workspace hostname, e.g. `dbc-a1b2c3d4-e5f6.cloud.databricks.com`
    /// (no scheme, no port — those are added separately).
    pub host: String,
    /// Defaults to `443` — every Databricks SQL Warehouse/cluster
    /// endpoint uses HTTPS.
    #[serde(default = "default_port")]
    pub port: u16,
    /// SQL Warehouse or cluster HTTP path, e.g.
    /// `/sql/1.0/warehouses/abc123def456`. Found in the warehouse's
    /// "Connection details" tab in the Databricks UI.
    pub http_path: String,
    pub auth: DatabricksAuth,
    /// Unity Catalog catalog name.
    pub catalog: String,
    /// Unity Catalog schema name.
    pub schema: String,
    /// Table to read from (source) or write to (sink).
    pub table: String,
    /// Column used to upsert/delete on write — required for the sink
    /// side; ignored by the source. Upserts use `MERGE INTO` (Databricks
    /// SQL supports it natively, same as Snowflake/BigQuery).
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Timeout in seconds for connecting and for each query. Defaults to
    /// `60`, same reasoning as Snowflake's own default: a serverless SQL
    /// Warehouse that auto-suspended between runs needs time to resume
    /// on first query.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Shared retry/backoff configuration for transient ADBC failures.
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl DatabricksConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.host.trim().is_empty() {
            return Err(NexusError::Connector(
                "databricks: host is required".to_string(),
            ));
        }
        if self.http_path.trim().is_empty() {
            return Err(NexusError::Connector(
                "databricks: http_path is required".to_string(),
            ));
        }
        match &self.auth {
            DatabricksAuth::PersonalAccessToken { token } => {
                if token.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "databricks: token is required for personal_access_token auth".to_string(),
                    ));
                }
            }
            DatabricksAuth::OauthU2M => {}
            DatabricksAuth::OauthM2M {
                client_id,
                client_secret,
            } => {
                if client_id.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "databricks: client_id is required for oauth_m2m auth".to_string(),
                    ));
                }
                if client_secret.trim().is_empty() {
                    return Err(NexusError::Connector(
                        "databricks: client_secret is required for oauth_m2m auth".to_string(),
                    ));
                }
            }
        }
        if self.catalog.trim().is_empty() {
            return Err(NexusError::Connector(
                "databricks: catalog is required".to_string(),
            ));
        }
        if self.schema.trim().is_empty() {
            return Err(NexusError::Connector(
                "databricks: schema is required".to_string(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(NexusError::Connector(
                "databricks: table is required".to_string(),
            ));
        }
        if let Some(pk) = self.primary_key.as_deref() {
            if pk.trim().is_empty() {
                return Err(NexusError::Connector(
                    "databricks: primary_key must not be empty when provided".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Builds the `databricks://...` connection URI the driver's single
    /// `"uri"` option key expects (see `driver.rs`), from this struct's
    /// typed fields — the 3 forms confirmed at
    /// adbc-drivers.org/drivers/databricks/.
    pub(crate) fn connection_uri(&self) -> String {
        match &self.auth {
            DatabricksAuth::PersonalAccessToken { token } => format!(
                "databricks://token:{token}@{}:{}{}",
                self.host, self.port, self.http_path
            ),
            DatabricksAuth::OauthU2M => format!(
                "databricks://{}:{}{}?authType=OauthU2M",
                self.host, self.port, self.http_path
            ),
            DatabricksAuth::OauthM2M {
                client_id,
                client_secret,
            } => format!(
                "databricks://{}:{}{}?authType=OAuthM2M&clientID={client_id}&clientSecret={client_secret}",
                self.host, self.port, self.http_path
            ),
        }
    }
}

fn default_port() -> u16 {
    443
}

fn default_timeout_seconds() -> u64 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config(auth: DatabricksAuth) -> DatabricksConnectorConfig {
        DatabricksConnectorConfig {
            host: "dbc-a1b2c3d4-e5f6.cloud.databricks.com".to_string(),
            port: 443,
            http_path: "/sql/1.0/warehouses/abc123def456".to_string(),
            auth,
            catalog: "main".to_string(),
            schema: "default".to_string(),
            table: "events".to_string(),
            primary_key: None,
            timeout_seconds: 60,
            retry: Default::default(),
        }
    }

    #[test]
    fn personal_access_token_uri() {
        let cfg = base_config(DatabricksAuth::PersonalAccessToken {
            token: "dapi123".to_string(),
        });
        assert_eq!(
            cfg.connection_uri(),
            "databricks://token:dapi123@dbc-a1b2c3d4-e5f6.cloud.databricks.com:443/sql/1.0/warehouses/abc123def456"
        );
    }

    #[test]
    fn oauth_u2m_uri() {
        let cfg = base_config(DatabricksAuth::OauthU2M);
        assert_eq!(
            cfg.connection_uri(),
            "databricks://dbc-a1b2c3d4-e5f6.cloud.databricks.com:443/sql/1.0/warehouses/abc123def456?authType=OauthU2M"
        );
    }

    #[test]
    fn oauth_m2m_uri() {
        let cfg = base_config(DatabricksAuth::OauthM2M {
            client_id: "client-1".to_string(),
            client_secret: "secret-1".to_string(),
        });
        assert_eq!(
            cfg.connection_uri(),
            "databricks://dbc-a1b2c3d4-e5f6.cloud.databricks.com:443/sql/1.0/warehouses/abc123def456?authType=OAuthM2M&clientID=client-1&clientSecret=secret-1"
        );
    }
}
