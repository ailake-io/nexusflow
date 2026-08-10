use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Every Axum handler error funnels through here so internal errors never
/// leak raw to the client — see ARCHITECTURE.md §9.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
        }
    }

    /// 500s never carry the underlying error's text: connector/sqlx errors
    /// routinely embed connection URIs — credentials included — so the
    /// detail goes to the server log and the client gets a generic message.
    pub fn internal(err: impl std::fmt::Display) -> Self {
        tracing::error!(error = %err, "internal server error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after = if self.status == StatusCode::TOO_MANY_REQUESTS {
            Some((axum::http::header::RETRY_AFTER, "60".to_string()))
        } else {
            None
        };
        if let Some((header, value)) = retry_after {
            return (
                self.status,
                [(header, value)],
                Json(json!({ "error": self.message })),
            )
                .into_response();
        }
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

/// Maximum length of a sanitized error message — anything longer is
/// truncated so a pathological error can't bloat the database or a Slack
/// payload.
const MAX_SANITIZED_LEN: usize = 500;

/// Scrubs credentials out of an internal error message, making it safe to
/// persist in `pipeline_runs.error` (readable over the API by any `Read`
/// role) and to forward to Slack. The full, unsanitized error must still go
/// to the server log — this is strictly for copies that leave the process.
///
/// Redacts:
///
///   - URI userinfo (`postgres://user:pass@host/db` becomes
///     `postgres://***@host/db`);
///   - query parameters whose names look like secrets (`token`, `password`,
///     `api_key`, `secret`, ...) — replaced with `***`.
///
/// Also truncates anything pathologically long.
pub(crate) fn sanitize_error(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;

    while let Some(scheme_colon) = rest.find("://") {
        // Find the start of the scheme (first alphanumeric/+/-/.
        // character before "://"). Schemes start at the beginning of the
        // string or after whitespace/a delimiter.
        let url_start = rest[..scheme_colon]
            .rfind(|c: char| {
                c.is_whitespace() || c == '"' || c == '\'' || c == '(' || c == '[' || c == '<'
            })
            .map(|i| i + 1)
            .unwrap_or(0);

        // Copy everything before the URL as-is.
        out.push_str(&rest[..url_start]);

        // Find where this URL ends in free-form text.
        let url_end = rest[scheme_colon + 3..]
            .find([' ', '\t', '\n', '"', '\'', ')', ']', ',', ';', '<'])
            .map(|i| scheme_colon + 3 + i)
            .unwrap_or(rest.len());
        let url_str = &rest[url_start..url_end];

        out.push_str(&sanitize_url(url_str));
        rest = &rest[url_end..];
    }
    out.push_str(rest);

    if out.len() > MAX_SANITIZED_LEN {
        let mut boundary = MAX_SANITIZED_LEN;
        while !out.is_char_boundary(boundary) {
            boundary -= 1;
        }
        out.truncate(boundary);
        out.push_str("…[truncated]");
    }
    out
}

/// Sanitizes a single URL string: redacts userinfo and sensitive query
/// parameters. If parsing fails, falls back to the original text so no
/// information is lost.
fn sanitize_url(url_str: &str) -> String {
    let Ok(mut url) = url::Url::parse(url_str) else {
        return url_str.to_string();
    };

    // Redact userinfo, keeping the `@host` portion so logs still say *where*
    // the failure happened.
    if !url.username().is_empty() || url.password().is_some() {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        // `url::Url` keeps the port in `after_host`, so don't add it again.
        let host = url.host_str().unwrap_or("").to_string();
        let after_host = url[url::Position::AfterHost..].to_string();
        return format!("{}://***@{host}{after_host}", url.scheme());
    }

    // Redact sensitive query parameters.
    if url.query().is_some() {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                let key = k.into_owned();
                let value = if is_sensitive_query_key(&key) {
                    "***".to_string()
                } else {
                    v.into_owned()
                };
                (key, value)
            })
            .collect();
        if pairs.iter().any(|(_, v)| v == "***") {
            let new_query = pairs
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            url.set_query(Some(&new_query));
        }
    }

    url.to_string()
}

fn is_sensitive_query_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    matches!(
        lower.as_str(),
        "token"
            | "access_token"
            | "refresh_token"
            | "api_key"
            | "apikey"
            | "key"
            | "secret"
            | "password"
            | "passwd"
            | "pwd"
            | "credentials"
            | "auth"
            | "authorization"
    ) || lower.ends_with("_token")
        || lower.ends_with("_key")
        || lower.ends_with("_secret")
        || lower.ends_with("_password")
        || lower.ends_with("_pwd")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_uri_userinfo() {
        assert_eq!(
            sanitize_error("connect failed: postgres://user:pw@db.internal:5432/app timeout"),
            "connect failed: postgres://***@db.internal:5432/app timeout"
        );
    }

    #[test]
    fn redacts_multiple_uris_in_one_message() {
        assert_eq!(
            sanitize_error("src postgres://u:p@h1/db sink mongodb://u2:p2@h2/db"),
            "src postgres://***@h1/db sink mongodb://***@h2/db"
        );
    }

    #[test]
    fn redacts_sensitive_query_parameters() {
        assert_eq!(
            sanitize_error("GET https://api.example.com/v1?api_key=supersecret&foo=bar failed"),
            "GET https://api.example.com/v1?api_key=***&foo=bar failed"
        );
    }

    #[test]
    fn redacts_token_suffix_query_parameters() {
        assert_eq!(
            sanitize_error("request to https://x.com/?my_service_token=abc&ok=yes"),
            "request to https://x.com/?my_service_token=***&ok=yes"
        );
    }

    #[test]
    fn leaves_messages_without_credentials_untouched() {
        let msg = "unsupported connector: got source=\"mongodb\"";
        assert_eq!(sanitize_error(msg), msg);
        // A URI without userinfo is not mangled.
        assert_eq!(
            sanitize_error("GET http://example.com/health failed"),
            "GET http://example.com/health failed"
        );
    }

    #[test]
    fn truncates_pathologically_long_messages() {
        let long = "x".repeat(5000);
        let out = sanitize_error(&long);
        assert!(out.len() <= MAX_SANITIZED_LEN + "…[truncated]".len());
        assert!(out.ends_with("…[truncated]"));
    }

    #[tokio::test]
    async fn internal_error_response_body_is_generic() {
        let response = ApiError::internal("postgres://user:pw@host/db refused").into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "internal server error");
    }
}
