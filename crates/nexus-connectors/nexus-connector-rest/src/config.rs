use serde::Deserialize;
use std::collections::HashMap;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
#[derive(Debug, Clone, Deserialize)]
pub struct RestConnectorConfig {
    pub base_url: String,
    #[serde(default)]
    pub path: String,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestFieldSpec {
    pub name: String,
    pub data_type: RestDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RestPagination {
    #[default]
    None,
    /// `?{offset_param}=N&{limit_param}=limit`, advances by `limit` each
    /// page, stops once a page returns fewer rows than `limit`.
    Offset {
        offset_param: String,
        limit_param: String,
        limit: i64,
    },
    /// `?{cursor_param}={cursor}`, next cursor read from
    /// `next_cursor_path` in the response body, stops once that path is
    /// absent or null.
    Cursor {
        cursor_param: String,
        next_cursor_path: String,
    },
}

fn default_max_pages() -> usize {
    1000
}
