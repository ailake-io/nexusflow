//! Minimal JSON→HCL literal serializer — covers exactly what a module's
//! `schemars`-derived config can contain (string/number/bool/array/object/
//! null), not the full HCL expression language. A pre-resolved edge
//! reference (`module.<id>.<output>`, `aws_glue_catalog_database.<id>.name`,
//! ...) is inserted as a raw unquoted string by the caller *before* this is
//! reached — this function never needs to know the difference between "a
//! literal string value" and "a Terraform reference", only real literal
//! JSON values ever pass through it.

use serde_json::Value;

pub(crate) fn literal(v: &Value) -> String {
    match v {
        Value::String(s) => quote(s),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(literal).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Object(map) => {
            let inner: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k} = {}", literal(v)))
                .collect();
            format!("{{\n    {}\n  }}", inner.join("\n    "))
        }
    }
}

/// HCL double-quoted string escaping — same characters JSON already
/// escapes (`"`, `\`, control chars), plus `$` and `%` which HCL treats
/// specially for interpolation/directives (`${...}`, `%{...}`) — a literal
/// dollar sign in a config value (e.g. a password) must not accidentally
/// start an interpolation sequence.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '$' => out.push_str("$$"),
            '%' => out.push_str("%%"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn escapes_interpolation_sigils() {
        assert_eq!(literal(&json!("$5 off, 100%")), "\"$$5 off, 100%%\"");
    }

    #[test]
    fn serializes_nested_object() {
        let v = json!({"a": 1, "b": [true, "x"]});
        let rendered = literal(&v);
        assert!(rendered.contains("a = 1"));
        assert!(rendered.contains("b = [true, \"x\"]"));
    }
}
