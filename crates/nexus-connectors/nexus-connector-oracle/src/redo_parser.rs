use nexus_core::NexusError;
use serde_json::{Map, Value};

/// Parses one `V$LOGMNR_CONTENTS.SQL_REDO` statement (the only
/// column-level data LogMiner exposes over plain SQL without
/// GoldenGate/XStream) into a column-name → value map. Hand-rolled,
/// quote-aware text parsing of Oracle's own documented `SQL_REDO`
/// format (no full SQL parser — just enough to split
/// column/value/assignment/condition lists correctly around quoted
/// string literals) — the same technique lightweight custom Oracle
/// CDC tools use when GoldenGate isn't available. **Not confirmed
/// against a real Oracle
/// instance this session** (no live LogMiner available in the
/// sandbox): quote-escaping (`''` → `'`) and whitespace assumptions
/// follow Oracle's documented examples, but haven't been checked
/// against a real driver's exact output.
///
/// `DATE`/`TIMESTAMP` values appear in `SQL_REDO` as function calls
/// (`TO_DATE('...', 'fmt')`), not plain string literals — v1 doesn't
/// parse those and fails loudly rather than silently mangling data.
pub(crate) fn parse_redo(operation: &str, sql_redo: &str) -> Result<Map<String, Value>, NexusError> {
    match operation.to_uppercase().as_str() {
        "INSERT" => parse_insert(sql_redo),
        "UPDATE" => parse_update(sql_redo),
        "DELETE" => parse_delete(sql_redo),
        other => Err(NexusError::Connector(format!(
            "oracle-cdc: unexpected OPERATION '{other}' from V$LOGMNR_CONTENTS"
        ))),
    }
}

pub(crate) fn opcode_letter(operation: &str) -> Result<&'static str, NexusError> {
    match operation.to_uppercase().as_str() {
        "INSERT" => Ok("I"),
        "UPDATE" => Ok("U"),
        "DELETE" => Ok("D"),
        other => Err(NexusError::Connector(format!(
            "oracle-cdc: unexpected OPERATION '{other}' from V$LOGMNR_CONTENTS"
        ))),
    }
}

/// `insert into "SCHEMA"."TABLE"("COL1","COL2") values ('v1','v2');`
fn parse_insert(sql: &str) -> Result<Map<String, Value>, NexusError> {
    let (before_values, vals_str) =
        split_once_ci(sql, "values").ok_or_else(|| redo_shape_error("INSERT", sql))?;

    // `before_values` is `insert into "SCHEMA"."TABLE"("COL1","COL2")` —
    // the column list is the first (and only) parenthesized group in it.
    let before_values = before_values.trim();
    let paren_idx = before_values
        .find('(')
        .ok_or_else(|| redo_shape_error("INSERT", sql))?;
    let cols_str = strip_outer_parens(before_values[paren_idx..].trim())
        .ok_or_else(|| redo_shape_error("INSERT", sql))?;

    let vals_str = strip_outer_parens(vals_str.trim().trim_end_matches(';').trim())
        .ok_or_else(|| redo_shape_error("INSERT", sql))?;

    let cols: Vec<String> = split_top_level(cols_str, ',')
        .into_iter()
        .map(|c| c.trim().trim_matches('"').to_string())
        .collect();
    let vals: Vec<Value> = split_top_level(vals_str, ',')
        .into_iter()
        .map(|v| parse_sql_literal(v.trim()))
        .collect::<Result<_, _>>()?;

    if cols.len() != vals.len() {
        return Err(NexusError::Connector(format!(
            "oracle-cdc: INSERT column/value count mismatch ({} cols, {} vals) parsing SQL_REDO: {sql}",
            cols.len(),
            vals.len()
        )));
    }

    Ok(cols.into_iter().zip(vals).collect())
}

/// `update "SCHEMA"."TABLE" set "COL1" = 'v1', "COL2" = 'v2' where ...;`
///
/// Only the `SET` clause is read — with `ADD SUPPLEMENTAL LOG DATA
/// (ALL) COLUMNS` enabled on the source table (a real, documented
/// prerequisite, not a v1 simplification — see README), Oracle
/// includes every column in `SET`, not just the changed ones, so this
/// yields the full after-image row.
fn parse_update(sql: &str) -> Result<Map<String, Value>, NexusError> {
    let after_set = split_once_ci(sql, "set")
        .map(|(_, rest)| rest)
        .ok_or_else(|| redo_shape_error("UPDATE", sql))?;
    let (set_clause, _where_clause) =
        split_once_ci(after_set, "where").ok_or_else(|| redo_shape_error("UPDATE", sql))?;

    let mut map = Map::new();
    for assignment in split_top_level(set_clause.trim(), ',') {
        let (col, val) = split_once(&assignment, '=').ok_or_else(|| {
            NexusError::Connector(format!("oracle-cdc: malformed SET assignment '{assignment}' in: {sql}"))
        })?;
        let col = col.trim().trim_matches('"').to_string();
        map.insert(col, parse_sql_literal(val.trim())?);
    }
    Ok(map)
}

/// `delete from "SCHEMA"."TABLE" where "COL1" = 'v1' and "COL2" IS NULL and ROWID = '...';`
///
/// Same supplemental-logging prerequisite as `UPDATE` — without it,
/// only the columns Oracle picked to identify the row appear here.
/// The trailing `ROWID = '...'` condition isn't a real column, so
/// it's dropped.
fn parse_delete(sql: &str) -> Result<Map<String, Value>, NexusError> {
    let where_clause = split_once_ci(sql, "where")
        .map(|(_, rest)| rest)
        .ok_or_else(|| redo_shape_error("DELETE", sql))?;
    let where_clause = where_clause.trim().trim_end_matches(';').trim();

    let mut map = Map::new();
    for condition in split_top_level_and(where_clause) {
        let condition = condition.trim();
        if condition.to_uppercase().starts_with("ROWID") {
            continue;
        }
        if let Some((col, _)) = split_once_ci(condition, "is null") {
            map.insert(col.trim().trim_matches('"').to_string(), Value::Null);
            continue;
        }
        let (col, val) = split_once(condition, '=').ok_or_else(|| {
            NexusError::Connector(format!("oracle-cdc: malformed WHERE condition '{condition}' in: {sql}"))
        })?;
        map.insert(col.trim().trim_matches('"').to_string(), parse_sql_literal(val.trim())?);
    }
    Ok(map)
}

fn redo_shape_error(operation: &str, sql: &str) -> NexusError {
    NexusError::Connector(format!("oracle-cdc: could not parse {operation} SQL_REDO: {sql}"))
}

/// `'v1'` → string (with `''` unescaped to `'`), bare `NULL` → JSON
/// null. Anything else (function calls like `TO_DATE(...)`, bare
/// numbers without quotes) is rejected — Oracle always quotes scalar
/// values in `SQL_REDO`, so an unquoted non-`NULL` token means a
/// value shape v1 doesn't support yet.
fn parse_sql_literal(raw: &str) -> Result<Value, NexusError> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }
    if raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'') {
        let inner = &raw[1..raw.len() - 1];
        return Ok(Value::String(inner.replace("''", "'")));
    }
    Err(NexusError::Connector(format!(
        "oracle-cdc: unsupported SQL_REDO literal '{raw}' (only quoted strings and NULL are supported in v1 — \
         DATE/TIMESTAMP function-call literals aren't parsed)"
    )))
}

/// Splits `s` on the first top-level (outside single-quoted strings)
/// occurrence of `sep`, case-insensitively, surrounded by word
/// boundaries (whitespace). Returns `(before, after)` with `sep`
/// itself excluded from both halves.
fn split_once_ci<'a>(s: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let sep_lower = sep.to_ascii_lowercase();
    let mut in_quote = false;
    let mut idx = 0usize;
    while idx < s.len() {
        let Some(c) = s[idx..].chars().next() else { break };
        if c == '\'' {
            let rest = &s[idx + 1..];
            if in_quote && rest.starts_with('\'') {
                idx += 2;
                continue;
            }
            in_quote = !in_quote;
            idx += c.len_utf8();
            continue;
        }
        if !in_quote {
            if let Some(chunk) = s.get(idx..idx + sep_lower.len()) {
                let boundary_before = idx == 0 || s.as_bytes()[idx - 1].is_ascii_whitespace();
                let boundary_after = s[idx + sep_lower.len()..]
                    .chars()
                    .next()
                    .map(|c| c.is_whitespace())
                    .unwrap_or(true);
                if boundary_before && boundary_after && chunk.eq_ignore_ascii_case(&sep_lower) {
                    return Some((&s[..idx], &s[idx + sep_lower.len()..]));
                }
            }
        }
        idx += c.len_utf8();
    }
    None
}

/// Splits `s` on the first occurrence of `sep` (a plain char, e.g.
/// `=`), not quote-aware — used only on already-isolated fragments
/// (e.g. a single `"COL" = 'val'` assignment) where `sep` can't
/// legitimately appear before the split point.
fn split_once(s: &str, sep: char) -> Option<(&str, &str)> {
    let idx = s.find(sep)?;
    Some((&s[..idx], &s[idx + sep.len_utf8()..]))
}

fn strip_outer_parens(s: &str) -> Option<&str> {
    let s = s.trim();
    s.strip_prefix('(').and_then(|s| s.strip_suffix(')'))
}

/// Splits `s` on top-level (outside single-quoted strings)
/// occurrences of `sep`, honoring `''` as an escaped quote rather than
/// a string terminator.
fn split_top_level(s: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if in_quote && chars.peek() == Some(&'\'') {
                current.push(c);
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
                continue;
            }
            in_quote = !in_quote;
            current.push(c);
            continue;
        }
        if c == sep && !in_quote {
            parts.push(current.trim().to_string());
            current.clear();
            continue;
        }
        current.push(c);
    }
    if !current.trim().is_empty() || !parts.is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

/// Splits `s` on top-level (outside single-quoted strings)
/// case-insensitive `" and "` boundaries.
fn split_top_level_and(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_quote = false;
    let mut idx = 0usize;
    while idx < s.len() {
        let Some(c) = s[idx..].chars().next() else { break };
        if c == '\'' {
            let rest = &s[idx + 1..];
            if in_quote && rest.starts_with('\'') {
                idx += 2;
                continue;
            }
            in_quote = !in_quote;
            idx += c.len_utf8();
            continue;
        }
        if !in_quote {
            if let Some(chunk) = s.get(idx..idx + 5) {
                if chunk.eq_ignore_ascii_case(" and ") {
                    parts.push(s[start..idx].trim().to_string());
                    idx += 5;
                    start = idx;
                    continue;
                }
            }
        }
        idx += c.len_utf8();
    }
    parts.push(s[start..].trim().to_string());
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_insert() {
        let sql = r#"insert into "HR"."EMPLOYEES"("EMPLOYEE_ID","FIRST_NAME","LAST_NAME") values ('306','Nandini','Shastry');"#;
        let row = parse_redo("INSERT", sql).unwrap();
        assert_eq!(row["EMPLOYEE_ID"], "306");
        assert_eq!(row["FIRST_NAME"], "Nandini");
        assert_eq!(row["LAST_NAME"], "Shastry");
    }

    #[test]
    fn parses_insert_with_null_and_escaped_quote() {
        let sql = r#"insert into "HR"."EMPLOYEES"("EMPLOYEE_ID","NICKNAME","MGR_ID") values ('1','O''Brien',NULL);"#;
        let row = parse_redo("INSERT", sql).unwrap();
        assert_eq!(row["NICKNAME"], "O'Brien");
        assert_eq!(row["MGR_ID"], Value::Null);
    }

    #[test]
    fn parses_update_set_clause_ignoring_where() {
        let sql = r#"update "HR"."EMPLOYEES" set "FIRST_NAME" = 'Nandini', "LAST_NAME" = 'Shastri' where "EMPLOYEE_ID" = '306' and "FIRST_NAME" = 'Nandini' and ROWID = 'AAAHskAABAAAY+CAAB';"#;
        let row = parse_redo("UPDATE", sql).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row["FIRST_NAME"], "Nandini");
        assert_eq!(row["LAST_NAME"], "Shastri");
    }

    #[test]
    fn parses_delete_where_clause_dropping_rowid() {
        let sql = r#"delete from "HR"."EMPLOYEES" where "EMPLOYEE_ID" = '306' and "FIRST_NAME" = 'Nandini' and "MGR_ID" IS NULL and ROWID = 'AAAHskAABAAAY+CAAB';"#;
        let row = parse_redo("DELETE", sql).unwrap();
        assert_eq!(row["EMPLOYEE_ID"], "306");
        assert_eq!(row["FIRST_NAME"], "Nandini");
        assert_eq!(row["MGR_ID"], Value::Null);
        assert!(!row.contains_key("ROWID"));
    }

    #[test]
    fn rejects_unparsed_function_call_literal() {
        let sql = r#"insert into "HR"."EMPLOYEES"("EMPLOYEE_ID","HIRE_DATE") values ('306',TO_DATE('2024-01-01', 'YYYY-MM-DD'));"#;
        let err = parse_redo("INSERT", sql).unwrap_err();
        assert!(matches!(err, NexusError::Connector(_)));
    }

    #[test]
    fn opcode_letter_maps_operations() {
        assert_eq!(opcode_letter("INSERT").unwrap(), "I");
        assert_eq!(opcode_letter("UPDATE").unwrap(), "U");
        assert_eq!(opcode_letter("DELETE").unwrap(), "D");
        assert!(opcode_letter("DDL").is_err());
    }
}
