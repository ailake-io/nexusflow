use crate::config::StarburstConnectorConfig;
use nexus_core::{with_timeout, NexusError};
use serde::Deserialize;
use serde_json::Value;

/// One column's metadata as reported by the statement protocol response.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub type_signature: String,
}

#[derive(Debug, Deserialize)]
struct QueryError {
    message: String,
}

/// One page of a `POST /v1/statement` (or `GET {nextUri}`) response.
/// `columns` only appears once the query plan is known (may be absent on
/// the very first page for some query shapes); `data` is absent on pages
/// that carry no rows (e.g. the final page). `next_uri` absent means the
/// query is done — no more pages to poll.
#[derive(Debug, Deserialize)]
struct StatementPage {
    #[serde(default)]
    columns: Option<Vec<ColumnMeta>>,
    #[serde(default)]
    data: Option<Vec<Vec<Value>>>,
    #[serde(rename = "nextUri", default)]
    next_uri: Option<String>,
    #[serde(default)]
    error: Option<QueryError>,
}

/// Hand-rolled client for the Trino/Starburst client-facing HTTP protocol
/// (`trino.io/docs/current/develop/client-protocol.html`) — Starburst is a
/// commercial fork of Trino that kept this protocol wire-compatible
/// (including the `X-Trino-*` header names), so the exact same client works
/// against either. No ADBC driver is used here; see `config.rs`'s doc
/// comment for why.
pub struct StarburstClient {
    http: reqwest::Client,
    base_url: String,
    user: String,
    password: Option<String>,
    access_token: Option<String>,
    catalog: String,
    schema_name: String,
    timeout_seconds: u64,
}

impl StarburstClient {
    pub fn new(cfg: &StarburstConnectorConfig) -> Result<Self, NexusError> {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(!cfg.ssl_verify)
            .build()
            .map_err(|e| NexusError::Connector(format!("starburst: building HTTP client: {e}")))?;

        Ok(Self {
            http,
            base_url: cfg.base_url(),
            user: cfg.user.clone(),
            password: cfg.password.clone(),
            access_token: cfg.access_token.clone(),
            catalog: cfg.catalog.clone(),
            schema_name: cfg.schema_name.clone(),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        // Bearer takes precedence over Basic when both are configured —
        // documented in config.rs's `access_token` field.
        if let Some(token) = &self.access_token {
            return req.bearer_auth(token);
        }
        if let Some(password) = &self.password {
            return req.basic_auth(&self.user, Some(password));
        }
        req
    }

    /// Runs `sql` to completion, following every `nextUri` page, and
    /// returns the full column list plus every row across all pages.
    /// Bounded by `timeout_seconds` for the whole exchange, not per-request
    /// — a cluster that keeps returning pages forever (or a query that
    /// legitimately takes longer than one page) must not hang the pipeline.
    pub async fn execute(
        &self,
        sql: &str,
    ) -> Result<(Vec<ColumnMeta>, Vec<Vec<Value>>), NexusError> {
        with_timeout(self.timeout_seconds, "starburst query", async {
            let mut columns: Option<Vec<ColumnMeta>> = None;
            let mut rows: Vec<Vec<Value>> = Vec::new();

            let initial_url = format!("{}/v1/statement", self.base_url);
            let mut page = self
                .fetch_page(
                    self.apply_auth(self.http.post(&initial_url))
                        .header("X-Trino-User", &self.user)
                        .header("X-Trino-Catalog", &self.catalog)
                        .header("X-Trino-Schema", &self.schema_name)
                        .body(sql.to_string()),
                )
                .await?;

            loop {
                if let Some(err) = page.error {
                    return Err(NexusError::Connector(format!(
                        "starburst query failed: {}",
                        err.message
                    )));
                }
                if let Some(cols) = page.columns {
                    columns = Some(cols);
                }
                if let Some(data) = page.data {
                    rows.extend(data);
                }

                let Some(next_uri) = page.next_uri else {
                    break;
                };
                page = self
                    .fetch_page(self.apply_auth(self.http.get(&next_uri)))
                    .await?;
            }

            let columns = columns.ok_or_else(|| {
                NexusError::Connector("starburst: query completed with no column metadata".into())
            })?;
            Ok((columns, rows))
        })
        .await
    }

    async fn fetch_page(&self, req: reqwest::RequestBuilder) -> Result<StatementPage, NexusError> {
        let response = req
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("starburst request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "starburst request failed ({status}): {text}"
            )));
        }

        response
            .json()
            .await
            .map_err(|e| NexusError::Connector(format!("starburst response parse failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StarburstConnectorConfig;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cfg_for(server: &MockServer) -> StarburstConnectorConfig {
        let addr = server.address();
        StarburstConnectorConfig {
            host: addr.ip().to_string(),
            port: addr.port(),
            user: "nexus".to_string(),
            password: None,
            access_token: None,
            ssl: false,
            ssl_verify: true,
            catalog: "hive".to_string(),
            schema_name: "default".to_string(),
            table_name: "events".to_string(),
            partition_column: None,
            timeout_seconds: 10,
        }
    }

    #[tokio::test]
    async fn execute_follows_next_uri_and_collects_rows_across_pages() {
        let server = MockServer::start().await;
        let next_uri = format!("{}/v1/statement/queued/next", server.uri());

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .and(header("X-Trino-User", "nexus"))
            .and(header("X-Trino-Catalog", "hive"))
            .and(header("X-Trino-Schema", "default"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
                "nextUri": next_uri,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/statement/queued/next"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [[2]],
            })))
            .mount(&server)
            .await;

        let client = StarburstClient::new(&cfg_for(&server)).unwrap();
        let (columns, rows) = client
            .execute("SELECT * FROM \"hive\".\"default\".\"events\"")
            .await
            .unwrap();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "id");
        assert_eq!(rows, vec![vec![json!(1)], vec![json!(2)]]);
    }

    #[tokio::test]
    async fn execute_returns_connector_error_on_query_error() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "error": {"message": "line 1:15: Table 'hive.default.missing' does not exist"},
            })))
            .mount(&server)
            .await;

        let client = StarburstClient::new(&cfg_for(&server)).unwrap();
        let err = client
            .execute("SELECT * FROM \"hive\".\"default\".\"missing\"")
            .await
            .expect_err("query error must surface as Err");
        assert!(matches!(err, NexusError::Connector(msg) if msg.contains("does not exist")));
    }

    #[tokio::test]
    async fn execute_sends_bearer_auth_when_access_token_is_set() {
        let server = MockServer::start().await;
        let mut cfg = cfg_for(&server);
        cfg.access_token = Some("tok_abc".to_string());

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .and(header("Authorization", "Bearer tok_abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            })))
            .mount(&server)
            .await;

        let client = StarburstClient::new(&cfg).unwrap();
        let (columns, rows) = client.execute("SELECT 1").await.unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn execute_sends_basic_auth_when_password_is_set() {
        let server = MockServer::start().await;
        let mut cfg = cfg_for(&server);
        cfg.password = Some("s3cr3t".to_string());

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .and(header(
                "Authorization",
                format!("Basic {}", base64_basic("nexus", "s3cr3t")).as_str(),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            })))
            .mount(&server)
            .await;

        let client = StarburstClient::new(&cfg).unwrap();
        let (columns, _rows) = client.execute("SELECT 1").await.unwrap();
        assert_eq!(columns.len(), 1);
    }

    /// Minimal base64 encoder for the one `user:password` Basic-auth value
    /// the test above needs — avoids pulling in a `base64` crate dependency
    /// just for a single test assertion.
    fn base64_basic(user: &str, password: &str) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let input = format!("{user}:{password}");
        let bytes = input.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = *chunk.get(1).unwrap_or(&0) as u32;
            let b2 = *chunk.get(2).unwrap_or(&0) as u32;
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
            out.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[(n >> 6 & 0x3F) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[(n & 0x3F) as usize] as char
            } else {
                '='
            });
        }
        out
    }
}
