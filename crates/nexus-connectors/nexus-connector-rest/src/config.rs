use nexus_core::NexusError;
use serde::Deserialize;
use std::collections::HashMap;
use url::Url;

/// HTTP method used by the REST source when fetching pages.
/// Serialized as an uppercase verb to match HTTP conventions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum RestMethod {
    /// Fetch data with GET (default). Query parameters for pagination are
    /// appended to the URL automatically.
    #[default]
    Get,
    /// Send an empty-body POST request. Useful for APIs that expose a search
    /// or list endpoint only through POST.
    Post,
    /// Send an empty-body PUT request.
    Put,
    /// Send an empty-body PATCH request.
    Patch,
    /// Send a DELETE request.
    Delete,
}

impl RestMethod {
    /// Convert to the corresponding `reqwest` HTTP method.
    pub fn as_reqwest(&self) -> reqwest::Method {
        match self {
            RestMethod::Get => reqwest::Method::GET,
            RestMethod::Post => reqwest::Method::POST,
            RestMethod::Put => reqwest::Method::PUT,
            RestMethod::Patch => reqwest::Method::PATCH,
            RestMethod::Delete => reqwest::Method::DELETE,
        }
    }
}

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RestConnectorConfig {
    /// Legacy full URL field. If present, it takes precedence over `url`,
    /// `base_url`, and `path`, preserving old configs that stored the
    /// complete endpoint address in a single `uri` key.
    #[serde(default)]
    pub uri: Option<String>,
    /// Legacy full URL field. Used when `uri` is absent and either replaces
    /// `base_url`/`path` entirely or fills in for a missing `base_url`.
    #[serde(default)]
    pub url: Option<String>,
    /// Scheme + host of the API, e.g. `"https://api.example.com"` — no
    /// trailing slash needed. Empty when the full URL is supplied via `uri`
    /// or `url`.
    #[serde(default)]
    pub base_url: String,
    /// Path appended to `base_url` for this request, e.g. `"/v1/items"`.
    /// Leading slashes are normalized automatically.
    #[serde(default)]
    pub path: String,
    /// HTTP method used when fetching pages. Defaults to `GET`.
    #[serde(default)]
    pub method: RestMethod,
    /// Extra HTTP headers sent with every request — this is where an API
    /// key/bearer token goes (e.g. `{"Authorization": "Bearer ..."}`).
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Explicit target schema — REST responses carry no schema of their own,
    /// so the node config must say what to project each field to.
    pub fields: Vec<RestFieldSpec>,
    /// Dot-separated path to the array of row objects in the response body
    /// (e.g. `"data.items"`). `None` means the response body itself is the array.
    #[serde(default)]
    pub rows_path: Option<String>,
    /// Pagination strategy applied across multiple requests.
    #[serde(default)]
    pub pagination: RestPagination,
    /// Hard cap on pages fetched, regardless of pagination signals — guards
    /// against a misbehaving API looping forever.
    #[serde(default = "default_max_pages")]
    pub max_pages: usize,
    /// Per-request timeout in seconds.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Number of retries on transient failures (5xx, timeouts, connect errors).
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Base delay between retries in seconds (exponential backoff).
    #[serde(default = "default_retry_backoff_seconds")]
    pub retry_backoff_seconds: u64,
    /// Maximum requests per second across this source (0 = unlimited).
    #[serde(default)]
    pub requests_per_second: u32,
}

impl RestConnectorConfig {
    /// Resolve the final request URL, honoring legacy single-key configs.
    ///
    /// Priority:
    /// 1. `uri` — old single-field URL.
    /// 2. `url` — alternate legacy single-field URL.
    /// 3. `base_url` + `path` — new split form.
    ///
    /// The resulting string is validated to be a non-protocol-relative,
    /// HTTP(S) URL.
    pub fn url(&self) -> Result<String, NexusError> {
        let raw = if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            uri.to_string()
        } else if let Some(url) = self.url.as_deref().filter(|s| !s.is_empty()) {
            url.to_string()
        } else {
            format!(
                "{}/{}",
                self.base_url.trim_end_matches('/'),
                self.path.trim_start_matches('/')
            )
        };

        if raw.starts_with("//") {
            return Err(NexusError::Schema(
                "REST URL must not be protocol-relative (//host)".to_string(),
            ));
        }

        let parsed = Url::parse(&raw)
            .map_err(|e| NexusError::Schema(format!("REST URL is invalid: {e}")))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return Err(NexusError::Schema(format!(
                "REST URL must use http(s), got {} scheme",
                parsed.scheme()
            )));
        }

        Ok(raw)
    }

    /// HTTP method to use when building the outgoing request.
    pub fn method(&self) -> reqwest::Method {
        self.method.as_reqwest()
    }
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RestFieldSpec {
    /// JSON field name to project from each row object — supports dot
    /// notation for nested fields (e.g. `"address.city"`).
    pub name: String,
    /// Arrow type this field's value gets converted to.
    pub data_type: RestDataType,
    /// Whether a missing/null value for this field is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a JSON field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RestDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

/// Pagination strategy for paginated REST sources.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RestPagination {
    /// No pagination — one request, one page.
    #[default]
    None,
    /// `?{offset_param}=N&{limit_param}=limit`, advances by `limit` each
    /// page, stops once a page returns fewer rows than `limit`.
    Offset {
        /// Query param name carrying the row offset, e.g. `"offset"`.
        offset_param: String,
        /// Query param name carrying the page size, e.g. `"limit"`.
        limit_param: String,
        /// Number of rows requested per page.
        limit: i64,
    },
    /// `?{cursor_param}={cursor}`, next cursor read from
    /// `next_cursor_path` in the response body, stops once that path is
    /// absent or null.
    Cursor {
        /// Query param name carrying the cursor, e.g. `"cursor"`.
        cursor_param: String,
        /// Dot-separated path to the next cursor value in the response
        /// body, e.g. `"meta.next_cursor"`.
        next_cursor_path: String,
    },
}

/// Generic outbound API sink — pushes each batch to an HTTP endpoint
/// instead of reading from one. Separate config shape from
/// `RestConnectorConfig` on purpose: a source's pagination fields
/// (`rows_path`/`pagination`/`max_pages`) have no write-side equivalent,
/// and a sink needs a method/body-shape choice a source never does. Same
/// connector crate as the source (shares the `reqwest` client + retry/
/// rate-limit helpers) but a distinct registered connector name
/// (`"webhook"`, see `lib.rs`) so the Canvas palette/config form shows the
/// right fields for each direction instead of one form trying to cover both.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct WebhookSinkConfig {
    /// Legacy full URL field. If present, it takes precedence over `url`,
    /// `base_url`, and `path`, preserving old configs that stored the
    /// complete endpoint address in a single `uri` key.
    #[serde(default)]
    pub uri: Option<String>,
    /// Legacy full URL field. Used when `uri` is absent and either replaces
    /// `base_url`/`path` entirely or fills in for a missing `base_url`.
    #[serde(default)]
    pub url: Option<String>,
    /// Scheme + host of the target API, e.g. `"https://api.example.com"` —
    /// no trailing slash needed. Empty when the full URL is supplied via
    /// `uri` or `url`.
    #[serde(default)]
    pub base_url: String,
    /// Path appended to `base_url`, e.g. `"/v1/events"`. Leading slashes
    /// are normalized automatically.
    #[serde(default)]
    pub path: String,
    /// HTTP method used to send each row/batch. Defaults to `POST`.
    #[serde(default)]
    pub method: WebhookMethod,
    /// Extra HTTP headers sent with every request — this is where an API
    /// key/bearer token goes (e.g. `{"Authorization": "Bearer ..."}`).
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// `"array"` sends one request per batch with every row as a JSON array
    /// body; `"per_row"` sends one request per row with that row as a JSON
    /// object body — pick whichever shape the target API expects.
    #[serde(default)]
    pub body_mode: WebhookBodyMode,
    /// Per-request timeout in seconds.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Number of retries on transient failures (5xx, timeouts, connect errors).
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Base delay between retries in seconds (exponential backoff).
    #[serde(default = "default_retry_backoff_seconds")]
    pub retry_backoff_seconds: u64,
    /// Maximum requests per second — only meaningful with `body_mode:
    /// "per_row"`, where a large batch means many requests (0 = unlimited).
    #[serde(default)]
    pub requests_per_second: u32,
}

impl WebhookSinkConfig {
    /// Resolve the final request URL, honoring legacy single-key configs.
    ///
    /// Priority:
    /// 1. `uri` — old single-field URL.
    /// 2. `url` — alternate legacy single-field URL.
    /// 3. `base_url` + `path` — new split form.
    ///
    /// The resulting string is validated to be a non-protocol-relative,
    /// HTTP(S) URL.
    pub fn url(&self) -> Result<String, NexusError> {
        let raw = if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            uri.to_string()
        } else if let Some(url) = self.url.as_deref().filter(|s| !s.is_empty()) {
            url.to_string()
        } else {
            format!(
                "{}/{}",
                self.base_url.trim_end_matches('/'),
                self.path.trim_start_matches('/')
            )
        };

        if raw.starts_with("//") {
            return Err(NexusError::Schema(
                "Webhook URL must not be protocol-relative (//host)".to_string(),
            ));
        }

        let parsed = Url::parse(&raw)
            .map_err(|e| NexusError::Schema(format!("Webhook URL is invalid: {e}")))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return Err(NexusError::Schema(format!(
                "Webhook URL must use http(s), got {} scheme",
                parsed.scheme()
            )));
        }

        Ok(raw)
    }
}

/// HTTP method used by the webhook sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum WebhookMethod {
    /// Send each payload via POST (default).
    #[default]
    Post,
    /// Send each payload via PUT.
    Put,
    /// Send each payload via PATCH.
    Patch,
    /// Send each payload via DELETE.
    Delete,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebhookBodyMode {
    /// Send the whole batch as one JSON array body.
    #[default]
    Array,
    /// Send one request per row, each with a JSON object body.
    PerRow,
}

fn default_max_pages() -> usize {
    1000
}

fn default_timeout_seconds() -> u64 {
    30
}

fn default_retries() -> u32 {
    3
}

fn default_retry_backoff_seconds() -> u64 {
    1
}
