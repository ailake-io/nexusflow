use serde::Deserialize;
use std::collections::HashMap;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RestConnectorConfig {
    /// Scheme + host of the API, e.g. `"https://api.example.com"` — no
    /// trailing slash needed.
    pub base_url: String,
    /// Path appended to `base_url` for this request, e.g. `"/v1/items"`.
    #[serde(default)]
    pub path: String,
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
    /// Full URL of the target endpoint, e.g. `"https://api.example.com/v1/events"`.
    pub url: String,
    /// HTTP method used to send each row/batch.
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum WebhookMethod {
    #[default]
    Post,
    Put,
    Patch,
    Delete,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebhookBodyMode {
    #[default]
    Array,
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
