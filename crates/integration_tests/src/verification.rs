// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The result document the shared verification runner collects.
//! See `nomad-jobs/services/platform/verification-runner/MAINTAINERS.md`.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::benchmark::RunOutcome;

/// Prefix the job strips from stdout before its poststop task logs the stamped
/// document. It must match the example pack byte for byte.
pub const RESULT_MARKER: &str = "VERIFICATION_RESULT";

#[derive(Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Ok,
    Warning,
    Critical,
    Unknown,
}

impl Status {
    /// A gate passes on OK and WARNING. Everything else stops a release.
    fn passes(self) -> bool {
        matches!(self, Status::Ok | Status::Warning)
    }
}

#[derive(Serialize)]
pub struct Check {
    pub name: String,
    pub status: Status,
    pub duration_ms: u64,
    pub detail: String,
}

impl Check {
    fn new(name: &str, status: Status, duration_ms: u64, detail: String) -> Self {
        // Stdout reaches VictoriaLogs with 90-day retention. Keep a runaway
        // detail out of the log line.
        const MAX_DETAIL: usize = 2000;
        let detail = if detail.len() > MAX_DETAIL {
            format!("{}…", &detail[..MAX_DETAIL])
        } else {
            detail
        };
        Self {
            name: name.to_string(),
            status,
            duration_ms,
            detail,
        }
    }
}

#[derive(Serialize)]
pub struct RunResult {
    pub schema_version: u32,
    pub started: String,
    pub duration_ms: u64,
    pub status: Status,
    pub endpoint: String,
    pub versions: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    pub checks: Vec<Check>,
}

/// Where a run flips from WARNING to CRITICAL, and how few verdicts make a run
/// undecidable.
#[derive(Debug)]
pub struct Thresholds {
    pub min_samples: u64,
    pub error_rate_critical: f64,
}

impl RunResult {
    pub fn write(&self, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let body = serde_json::to_string_pretty(self)?;
        std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
    }

    pub fn to_json_line(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn exit_code(&self) -> i32 {
        if self.status.passes() { 0 } else { 1 }
    }
}

/// Builds the document from one benchmark run.
///
/// The comparison path emits one latency result per endpoint, so
/// `total_requests` counts two per compared request. Every rate here divides by
/// the number of verdicts instead: comparisons plus rpc1 errors.
pub fn from_run(
    outcome: &RunOutcome,
    endpoint: &str,
    versions: BTreeMap<String, String>,
    started: chrono::DateTime<chrono::Utc>,
    duration_ms: u64,
    thresholds: &Thresholds,
) -> RunResult {
    let s = &outcome.stats;
    let verdicts = s.total_compared + s.total_rpc1_errors;
    let errors = s.total_mismatches + s.total_no_context_mismatches + s.total_rpc1_errors;

    let mut checks = Vec::new();

    checks.push(sample_size_check(verdicts, thresholds));
    checks.push(match_rate_check(
        verdicts,
        errors,
        s.total_mismatches,
        s.total_no_context_mismatches,
        s.total_rpc1_errors,
        thresholds,
    ));
    checks.push(rpc1_error_check(
        verdicts,
        s.total_rpc1_errors,
        &s.rpc1_name,
        thresholds,
    ));
    checks.push(slot_lag_check(&s.slot_diffs, s.total_with_context));
    checks.push(dropped_check(outcome.dropped));
    checks.push(rescue_check(
        verdicts,
        s.total_recovered_by_retry,
        thresholds,
    ));

    // The run status is the worst check status.
    let status = checks
        .iter()
        .map(|c| c.status)
        .max()
        .unwrap_or(Status::Unknown);

    RunResult {
        schema_version: 1,
        started: started.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        duration_ms,
        status,
        endpoint: endpoint.to_string(),
        versions,
        notes: None,
        checks,
    }
}

/// Too few verdicts makes a rate meaningless. Zero verdicts is never a pass:
/// an empty run means the harness failed, not that the target is healthy.
fn sample_size_check(verdicts: u64, t: &Thresholds) -> Check {
    if verdicts == 0 {
        return Check::new(
            "sample-size",
            Status::Unknown,
            0,
            "no request produced a verdict. The source returned nothing, or every send failed on \
             both endpoints. This is not a pass."
                .to_string(),
        );
    }
    if verdicts < t.min_samples {
        return Check::new(
            "sample-size",
            Status::Unknown,
            0,
            format!(
                "only {verdicts} verdicts, below the floor of {}. One failure would swing the \
                 rate past the threshold, so the run cannot decide.",
                t.min_samples
            ),
        );
    }
    Check::new(
        "sample-size",
        Status::Ok,
        0,
        format!("{verdicts} verdicts"),
    )
}

fn match_rate_check(
    verdicts: u64,
    errors: u64,
    mismatches: u64,
    no_context: u64,
    rpc1_errors: u64,
    t: &Thresholds,
) -> Check {
    if verdicts == 0 {
        return Check::new(
            "response-match-rate",
            Status::Unknown,
            0,
            "no comparison ran".to_string(),
        );
    }
    let rate = errors as f64 / verdicts as f64;
    let detail = format!(
        "{errors} of {verdicts} verdicts failed ({:.2}%): {mismatches} mismatches, {no_context} \
         no-context mismatches, {rpc1_errors} endpoint errors",
        rate * 100.0
    );
    let status = if errors == 0 {
        Status::Ok
    } else if rate < t.error_rate_critical {
        Status::Warning
    } else {
        Status::Critical
    };
    Check::new("response-match-rate", status, 0, detail)
}

/// Broken out of the rate so the detail names the endpoint that failed.
fn rpc1_error_check(verdicts: u64, rpc1_errors: u64, rpc1_name: &str, t: &Thresholds) -> Check {
    if rpc1_errors == 0 {
        return Check::new(
            "endpoint-errors",
            Status::Ok,
            0,
            format!("{rpc1_name} answered every request the reference answered"),
        );
    }
    let rate = rpc1_errors as f64 / verdicts.max(1) as f64;
    let status = if rate < t.error_rate_critical {
        Status::Warning
    } else {
        Status::Critical
    };
    Check::new(
        "endpoint-errors",
        status,
        0,
        format!(
            "{rpc1_name} failed to answer {rpc1_errors} of {verdicts} requests ({:.2}%) that the \
             reference answered",
            rate * 100.0
        ),
    )
}

/// `slot_diffs` holds the non-zero `rpc1 slot - rpc2 slot` values, so a
/// negative entry means rpc1 trailed the reference.
fn slot_lag_check(slot_diffs: &[i64], with_context: u64) -> Check {
    if with_context == 0 {
        return Check::new(
            "slot-lag",
            Status::Ok,
            0,
            "no response carried a context slot, so lag was not measured".to_string(),
        );
    }
    let behind = slot_diffs.iter().filter(|d| **d < 0).count() as u64;
    let share = behind as f64 / with_context as f64;
    let detail = format!(
        "behind the reference on {behind} of {with_context} responses with context ({:.1}%)",
        share * 100.0
    );
    let status = if share > 0.5 {
        Status::Warning
    } else {
        Status::Ok
    };
    Check::new("slot-lag", status, 0, detail)
}

/// Backpressure is a fact about the runner, not about the target.
fn dropped_check(dropped: u64) -> Check {
    if dropped == 0 {
        return Check::new("dropped-requests", Status::Ok, 0, "none".to_string());
    }
    Check::new(
        "dropped-requests",
        Status::Warning,
        0,
        format!(
            "{dropped} requests were never sent because the in-flight limit was full. The run \
             tested less than it intended."
        ),
    )
}

/// A rising rescue count is the early signal that temporal consistency is
/// degrading, even while the run still passes.
fn rescue_check(verdicts: u64, rescued: u64, t: &Thresholds) -> Check {
    if rescued == 0 {
        return Check::new("recovered-by-retry", Status::Ok, 0, "none".to_string());
    }
    let rate = rescued as f64 / verdicts.max(1) as f64;
    let detail = format!(
        "{rescued} of {verdicts} verdicts matched only after a retry ({:.2}%). They would have \
         been mismatches without retry-in-place.",
        rate * 100.0
    );
    let status = if rate < t.error_rate_critical {
        Status::Ok
    } else {
        Status::Warning
    };
    Check::new("recovered-by-retry", status, 0, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds() -> Thresholds {
        Thresholds {
            min_samples: 100,
            error_rate_critical: 0.01,
        }
    }

    #[test]
    fn zero_verdicts_is_unknown_not_ok() {
        let t = thresholds();
        assert_eq!(sample_size_check(0, &t).status, Status::Unknown);
        assert_eq!(
            match_rate_check(0, 0, 0, 0, 0, &t).status,
            Status::Unknown,
            "an empty run must never pass"
        );
    }

    #[test]
    fn below_the_floor_is_unknown() {
        assert_eq!(sample_size_check(99, &thresholds()).status, Status::Unknown);
        assert_eq!(sample_size_check(100, &thresholds()).status, Status::Ok);
    }

    #[test]
    fn error_rate_thresholds() {
        let t = thresholds();
        assert_eq!(match_rate_check(1000, 0, 0, 0, 0, &t).status, Status::Ok);
        // 9 of 1000 is 0.9%, under the 1% line.
        assert_eq!(
            match_rate_check(1000, 9, 9, 0, 0, &t).status,
            Status::Warning
        );
        // 10 of 1000 is exactly 1%, which stops a release.
        assert_eq!(
            match_rate_check(1000, 10, 10, 0, 0, &t).status,
            Status::Critical
        );
    }

    #[test]
    fn rpc1_errors_count_toward_the_rate() {
        let t = thresholds();
        let check = match_rate_check(1000, 20, 0, 0, 20, &t);
        assert_eq!(check.status, Status::Critical);
        assert!(check.detail.contains("20 endpoint errors"));
    }

    #[test]
    fn run_status_is_the_worst_check() {
        let checks = [Status::Ok, Status::Warning, Status::Critical, Status::Ok];
        assert_eq!(checks.iter().copied().max().unwrap(), Status::Critical);
        // UNKNOWN outranks CRITICAL, so an undecidable run never reports as a
        // clean failure.
        let checks = [Status::Critical, Status::Unknown];
        assert_eq!(checks.iter().copied().max().unwrap(), Status::Unknown);
    }

    #[test]
    fn exit_code_passes_on_warning_only() {
        for (status, code) in [
            (Status::Ok, 0),
            (Status::Warning, 0),
            (Status::Critical, 1),
            (Status::Unknown, 1),
        ] {
            let doc = RunResult {
                schema_version: 1,
                started: String::new(),
                duration_ms: 0,
                status,
                endpoint: String::new(),
                versions: BTreeMap::new(),
                notes: None,
                checks: vec![],
            };
            assert_eq!(doc.exit_code(), code, "{status:?}");
        }
    }

    #[test]
    fn status_serializes_uppercase() {
        assert_eq!(serde_json::to_string(&Status::Critical).unwrap(), "\"CRITICAL\"");
        assert_eq!(serde_json::to_string(&Status::Ok).unwrap(), "\"OK\"");
    }

    #[test]
    fn slot_lag_warns_only_when_mostly_behind() {
        assert_eq!(slot_lag_check(&[-1, -2, -3], 4).status, Status::Warning);
        assert_eq!(slot_lag_check(&[-1, 2], 10).status, Status::Ok);
        assert_eq!(slot_lag_check(&[], 0).status, Status::Ok);
    }
}
