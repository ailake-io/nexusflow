use serde_json::Value;

/// Navigates a dot-separated path (`"data.items"`) into a JSON value.
/// `None`/empty path returns the value itself.
pub fn navigate<'a>(value: &'a Value, path: Option<&str>) -> Option<&'a Value> {
    let Some(path) = path.filter(|p| !p.is_empty()) else {
        return Some(value);
    };

    path.split('.')
        .try_fold(value, |current, segment| current.get(segment))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn none_path_returns_root() {
        let body = json!({"a": 1});
        assert_eq!(navigate(&body, None), Some(&body));
    }

    #[test]
    fn nested_path_resolves() {
        let body = json!({"data": {"items": [1, 2, 3]}});
        assert_eq!(navigate(&body, Some("data.items")), Some(&json!([1, 2, 3])));
    }

    #[test]
    fn missing_segment_is_none() {
        let body = json!({"data": {}});
        assert_eq!(navigate(&body, Some("data.items")), None);
    }
}
