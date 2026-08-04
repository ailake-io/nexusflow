use crate::error::NexusError;

/// Shared by every SQL-based connector (Postgres, SQLite, ...). Table/column
/// names come from the pipeline spec (attacker-controlled HTTP request body)
/// and get spliced into SQL text — ADBC's `bind` only covers row *values*,
/// not identifiers, so there's no parameterized-query escape hatch for them.
/// Every identifier must go through here before it touches a `format!` that
/// builds SQL: reject anything outside a strict safe subset, then
/// double-quote it (ANSI SQL identifier quoting, doubling any embedded `"`)
/// so mixed-case names and reserved words round-trip correctly too.
pub fn quote_identifier(name: &str) -> Result<String, NexusError> {
    validate_identifier(name).map(|valid| format!("\"{}\"", valid.replace('"', "\"\"")))
}

/// Validates that `name` is a safe identifier and returns it unchanged.
///
/// Sinks that build their own predicate syntax (LanceDB, Milvus, etc.) can
/// use this to reject attacker-controlled column names without being forced
/// into ANSI double-quote quoting, which those engines may not accept.
pub fn validate_identifier(name: &str) -> Result<&str, NexusError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');

    if !valid {
        return Err(NexusError::Schema(format!(
            "invalid identifier {name:?}: must match [A-Za-z_][A-Za-z0-9_]*, max 128 chars"
        )));
    }

    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_identifiers() {
        assert_eq!(quote_identifier("events").unwrap(), "\"events\"");
        assert_eq!(quote_identifier("_id").unwrap(), "\"_id\"");
        assert_eq!(quote_identifier("Events_2").unwrap(), "\"Events_2\"");
    }

    #[test]
    fn rejects_sql_injection_attempts() {
        assert!(quote_identifier("events; DROP TABLE users; --").is_err());
        assert!(quote_identifier("events\" OR \"1\"=\"1").is_err());
        assert!(quote_identifier("id) UNION SELECT password FROM pg_shadow--").is_err());
        assert!(quote_identifier("events--").is_err());
        assert!(quote_identifier("").is_err());
        assert!(quote_identifier(" events").is_err());
        assert!(quote_identifier("events;").is_err());
        assert!(quote_identifier("public.events").is_err());
        assert!(quote_identifier("1events").is_err());
    }

    #[test]
    fn validate_identifier_rejects_injection_and_returns_original_name() {
        assert_eq!(validate_identifier("events").unwrap(), "events");
        assert_eq!(validate_identifier("_id").unwrap(), "_id");
        assert!(validate_identifier("events; DROP TABLE users; --").is_err());
        assert!(validate_identifier("events\" OR \"1\"=\"1").is_err());
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn rejects_names_longer_than_the_limit() {
        let too_long = "a".repeat(129);
        assert!(quote_identifier(&too_long).is_err());
        assert!(quote_identifier(&"a".repeat(128)).is_ok());
    }
}
