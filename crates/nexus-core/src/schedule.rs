use std::str::FromStr;

/// Parses a pipeline's `schedule` cron expression. The underlying `cron`
/// crate expects a 6-field Quartz-style expression (leading seconds field:
/// `sec min hour day-of-month month day-of-week`), but that's not the
/// format most people mean by "cron" — a bare 5-field Unix expression
/// (`minute hour day-of-month month day-of-week`, e.g. `"0 */6 * * *"` for
/// every 6 hours) gets `"0 "` prepended automatically so users don't have
/// to remember the extra field. A 6-field expression is passed through
/// as-is.
pub fn parse_cron_expression(expr: &str) -> Result<cron::Schedule, String> {
    let normalized = if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    };
    cron::Schedule::from_str(&normalized).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_familiar_5_field_unix_cron() {
        parse_cron_expression("0 */6 * * *").expect("5-field cron parses");
    }

    #[test]
    fn accepts_native_6_field_quartz_cron() {
        parse_cron_expression("0 0 */6 * * *").expect("6-field cron parses");
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_cron_expression("not a cron expression").is_err());
    }
}
