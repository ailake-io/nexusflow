use serde::Serialize;

/// How far outside the historical baseline a value fell — `Warning` at
/// `WARNING_Z` standard deviations, `Critical` at `CRITICAL_Z`. Deliberately
/// just two tiers (matches the pass/fail vocabulary the rest of the Quality
/// tab already uses) rather than a raw z-score exposed to the user —
/// simple, explainable thresholds, same philosophy as `QualityCheckKind`'s
/// fixed min/max bounds, just computed instead of configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalySeverity {
    Warning,
    Critical,
}

/// Below this many historical points, `detect_anomaly` always returns
/// `None` — a z-score computed from 1-2 prior runs is noise, not signal,
/// and would false-positive constantly on a pipeline that's simply new.
pub const MIN_HISTORY_FOR_DETECTION: usize = 5;

const WARNING_Z: f64 = 2.0;
const CRITICAL_Z: f64 = 3.0;

/// Z-score anomaly detection over a simple historical window — pure and
/// DB-free (the caller loads `history`, e.g. from
/// `pipeline_run_volume_store::PipelineRunVolumeStore::recent`), same
/// testability rationale as `nexus_core::quality::evaluate_quality_checks`.
/// `history` must not include `latest` itself — it's the baseline `latest`
/// is compared against, not part of its own comparison.
pub fn detect_anomaly(history: &[f64], latest: f64) -> Option<AnomalySeverity> {
    if history.len() < MIN_HISTORY_FOR_DETECTION {
        return None;
    }
    let (mean, stddev) = mean_and_stddev(history);

    if stddev == 0.0 {
        // Every historical value identical (a z-score would divide by
        // zero) — any deviation at all from a perfectly flat baseline is
        // real, but conservatively a `Warning`, not `Critical`: a single
        // data point can't establish how far outside "normal" is
        // catastrophic vs. just different.
        return if latest != mean {
            Some(AnomalySeverity::Warning)
        } else {
            None
        };
    }

    let z = (latest - mean).abs() / stddev;
    if z >= CRITICAL_Z {
        Some(AnomalySeverity::Critical)
    } else if z >= WARNING_Z {
        Some(AnomalySeverity::Warning)
    } else {
        None
    }
}

/// Population mean and standard deviation of `history` — `(0.0, 0.0)` for
/// an empty slice (never called that way by `detect_anomaly` itself, which
/// guards on `MIN_HISTORY_FOR_DETECTION` first, but exposed as its own
/// function so `GET /pipelines/{id}/anomalies` can report the baseline
/// numbers a user sees alongside the severity verdict, without
/// recomputing them differently).
pub fn mean_and_stddev(history: &[f64]) -> (f64, f64) {
    if history.is_empty() {
        return (0.0, 0.0);
    }
    let mean = history.iter().sum::<f64>() / history.len() as f64;
    let variance = history.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / history.len() as f64;
    (mean, variance.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_none_below_minimum_history() {
        let history = vec![100.0, 105.0, 98.0];
        assert_eq!(detect_anomaly(&history, 1_000_000.0), None);
    }

    #[test]
    fn returns_none_for_a_value_close_to_the_mean() {
        let history = vec![100.0, 102.0, 98.0, 101.0, 99.0];
        assert_eq!(detect_anomaly(&history, 100.0), None);
    }

    #[test]
    fn flags_warning_for_a_moderate_deviation() {
        // mean=100, stddev=~1.414 -> a value ~2.5 stddev out
        let history = vec![100.0, 101.0, 99.0, 102.0, 98.0];
        let severity = detect_anomaly(&history, 103.5);
        assert_eq!(severity, Some(AnomalySeverity::Warning));
    }

    #[test]
    fn flags_critical_for_a_large_deviation() {
        let history = vec![100.0, 105.0, 95.0, 102.0, 98.0];
        let severity = detect_anomaly(&history, 10.0);
        assert_eq!(severity, Some(AnomalySeverity::Critical));
    }

    #[test]
    fn zero_variance_baseline_flags_any_deviation_as_warning() {
        let history = vec![100.0, 100.0, 100.0, 100.0, 100.0];
        assert_eq!(detect_anomaly(&history, 150.0), Some(AnomalySeverity::Warning));
        assert_eq!(detect_anomaly(&history, 50.0), Some(AnomalySeverity::Warning));
    }

    #[test]
    fn zero_variance_baseline_matching_value_is_not_an_anomaly() {
        let history = vec![100.0, 100.0, 100.0, 100.0, 100.0];
        assert_eq!(detect_anomaly(&history, 100.0), None);
    }

    #[test]
    fn mean_and_stddev_matches_hand_computed_values() {
        let (mean, stddev) = mean_and_stddev(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert!((mean - 5.0).abs() < 1e-9);
        assert!((stddev - 2.0).abs() < 1e-9);
    }

    #[test]
    fn mean_and_stddev_of_empty_history_is_zero() {
        assert_eq!(mean_and_stddev(&[]), (0.0, 0.0));
    }

    #[test]
    fn exactly_at_the_minimum_history_threshold_still_detects() {
        let history = vec![100.0, 100.0, 100.0, 100.0, 100.0];
        assert_eq!(history.len(), MIN_HISTORY_FOR_DETECTION);
        assert!(detect_anomaly(&history, 200.0).is_some());
    }
}
