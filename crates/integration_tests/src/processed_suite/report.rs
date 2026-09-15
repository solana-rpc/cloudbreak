// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Final report data: per-front tallies, correctness rows, speed rows and coverage, the JSON
//! shape written with `--output`, and the exit decision. Text rendering lives in `render.rs`.
//!
//! A gating front that ran but passed no sample reads `no coverage`. It does not fail the run.

use super::{CLOUDBREAK, Front, MAX_EXAMPLES};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Per-front correctness counts. `failures` holds the failing classes, `classes` the rest.
/// `covered` of `covered_of` counts the checks that reached a definite verdict.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Tally {
    pub samples: u64,
    pub passed: u64,
    pub failed: u64,
    pub abandoned: u64,
    pub fresh: u64,
    pub fresh_of: u64,
    pub covered: u64,
    pub covered_of: u64,
    pub classes: BTreeMap<String, u64>,
    pub failures: BTreeMap<String, u64>,
    pub examples: Vec<String>,
}

impl Tally {
    pub fn pass(&mut self) {
        self.passed += 1;
    }

    pub fn note(&mut self, class: &str) {
        *self.classes.entry(class.to_string()).or_default() += 1;
    }

    pub fn fail(&mut self, class: &str, example: impl Into<String>) {
        self.failed += 1;
        *self.failures.entry(class.to_string()).or_default() += 1;
        if self.examples.len() < MAX_EXAMPLES {
            self.examples.push(format!("{class}: {}", example.into()));
        }
    }

    pub fn cover(&mut self, covered: u64, of: u64) {
        (self.covered, self.covered_of) = (self.covered + covered, self.covered_of + of);
    }

    pub fn merge(&mut self, other: Tally) {
        self.samples += other.samples;
        self.passed += other.passed;
        self.failed += other.failed;
        self.abandoned += other.abandoned;
        (self.fresh, self.fresh_of) = (self.fresh + other.fresh, self.fresh_of + other.fresh_of);
        self.cover(other.covered, other.covered_of);
        for (class, n) in other.classes {
            *self.classes.entry(class).or_default() += n;
        }
        for (class, n) in other.failures {
            *self.failures.entry(class).or_default() += n;
        }
        let room = MAX_EXAMPLES.saturating_sub(self.examples.len());
        self.examples.extend(other.examples.into_iter().take(room));
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Summary {
    pub n: usize,
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

/// Nearest-rank percentile of an ascending slice, 0 when empty.
pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    let rank = (q * sorted.len() as f64).ceil() as usize;
    let index = rank.clamp(1, sorted.len().max(1)) - 1;
    sorted.get(index).copied().unwrap_or(0.0)
}

pub fn summarize(mut samples: Vec<f64>) -> Summary {
    samples.sort_by(f64::total_cmp);
    let at = |q| percentile(&samples, q);
    let (min, max) = (samples.first().copied(), samples.last().copied());
    let (n, min, max) = (samples.len(), min.unwrap_or(0.0), max.unwrap_or(0.0));
    Summary {
        n,
        min,
        p50: at(0.5),
        p95: at(0.95),
        p99: at(0.99),
        max,
    }
}

pub fn pct(n: u64, d: u64) -> f64 {
    n as f64 * 100.0 / d.max(1) as f64
}

#[derive(Serialize)]
pub struct FrontRow {
    pub front: String,
    /// False for rows that only inform, such as the confirmed control group.
    pub gating: bool,
    pub skipped: Option<String>,
    /// pass, FAIL, no coverage, info or skipped, set after the exit decision.
    pub verdict: String,
    pub covered_pct: Option<f64>,
    pub tally: Tally,
}

impl FrontRow {
    pub fn new(front: &str, gating: bool, skipped: Option<String>, tally: Tally) -> Self {
        let covered_pct = (tally.covered_of > 0).then(|| pct(tally.covered, tally.covered_of));
        Self {
            front: front.to_string(),
            gating,
            skipped,
            verdict: String::new(),
            covered_pct,
            tally,
        }
    }
}

#[derive(Serialize)]
pub struct LatencyRow {
    pub source: String,
    pub method: String,
    pub commitment: String,
    /// Successful replies only. Timeouts and errors are counted apart.
    pub latency_ms: Summary,
    pub requests: u64,
    pub timeouts: u64,
    pub http_errors: u64,
    pub rpc_errors: BTreeMap<i64, u64>,
    pub avg_bytes: u64,
}

impl LatencyRow {
    pub fn error_pct(&self) -> f64 {
        let errors = self.timeouts + self.http_errors + self.rpc_errors.values().sum::<u64>();
        pct(errors, self.requests)
    }
}

#[derive(Serialize)]
pub struct RawRow {
    pub source: String,
    /// Block receipt to the matching reply, minus the local queue wait.
    pub propagation_ms: Summary,
    /// Local queue wait per completed probe.
    pub queued_ms: Summary,
    pub timeouts: u64,
    pub dropped: u64,
    pub excluded: u64,
}

#[derive(Serialize)]
pub struct FreshRow {
    pub source: String,
    pub fresh_pct: f64,
    pub samples: u64,
    /// Watcher tip at send minus `context.slot`.
    pub behind_tip: Summary,
}

#[derive(Serialize)]
pub struct DeltaRow {
    pub reference: String,
    pub equal_pct: f64,
    /// Cloudbreak slot minus reference slot, by delta.
    pub counts: BTreeMap<i64, u64>,
}

#[derive(Serialize)]
pub struct RateRow {
    pub source: String,
    pub requests: u64,
    pub rps: f64,
    pub cap_rps: f64,
}

#[derive(Default, Serialize)]
pub struct Coverage {
    pub run_secs: u64,
    pub interrupted: bool,
    pub blocks: u64,
    pub forks: u64,
    pub dead_slots: u64,
    pub restarted_slots: u64,
    pub bursts: u64,
    pub burst_responses: u64,
    pub burst_regressions: u64,
    pub watcher_reconnects: u64,
    pub pool_keys: usize,
    pub keys_sampled: u64,
    pub skipped: Vec<(String, String)>,
    pub getblocks_checked: u64,
    pub getblocks_disagreements: u64,
    pub disagreement_examples: Vec<String>,
    pub source_rates: Vec<RateRow>,
    pub load_skipped_ticks: BTreeMap<String, u64>,
    pub max_rss_mb: f64,
}

#[derive(Clone, Default, Serialize)]
pub struct Thresholds {
    pub max_cross_mismatch: u64,
    pub max_abandoned_pct: f64,
    pub min_fresh_pct: f64,
    pub max_p99_ms: Option<f64>,
}

#[derive(Default, Serialize)]
pub struct Report {
    pub started_at: String,
    pub duration_secs: f64,
    pub inputs: JsonValue,
    pub thresholds: Thresholds,
    pub correctness: Vec<FrontRow>,
    pub latency: Vec<LatencyRow>,
    pub read_after_write: Vec<RawRow>,
    pub freshness: Vec<FreshRow>,
    pub slot_delta: Vec<DeltaRow>,
    pub metrics: Vec<(String, String)>,
    pub fork_metric_deltas: BTreeMap<String, f64>,
    pub coverage: Coverage,
    pub failures: Vec<String>,
}

/// Every failed correctness rule, prefixed with its front. Empty means exit zero.
pub fn decide(rows: &[FrontRow], latency: &[LatencyRow], th: &Thresholds) -> Vec<String> {
    let mut failures = Vec::new();
    for row in rows.iter().filter(|r| r.gating && r.skipped.is_none()) {
        let (t, front) = (&row.tally, row.front.as_str());
        for (class, n) in &t.failures {
            let cross = front == Front::CrossSource.name() && class == "mismatch";
            if !cross || *n > th.max_cross_mismatch {
                failures.push(format!("{front}: {n} {class}"));
            }
        }
        let (abandoned, max) = (pct(t.abandoned, t.samples), th.max_abandoned_pct);
        if t.abandoned > 0 && abandoned > max {
            failures.push(format!(
                "{front}: abandoned served {abandoned:.1}% above {max}%"
            ));
        }
        let (fresh, min) = (pct(t.fresh, t.fresh_of), th.min_fresh_pct);
        if t.fresh_of > 0 && fresh < min {
            failures.push(format!("{front}: fresh {fresh:.1}% below {min}%"));
        }
    }
    let Some(max) = th.max_p99_ms else {
        return failures;
    };
    for row in latency
        .iter()
        .filter(|r| r.source == CLOUDBREAK && r.latency_ms.p99 > max)
    {
        let (method, commitment, p99) = (&row.method, &row.commitment, row.latency_ms.p99);
        failures.push(format!(
            "latency: {method} {commitment} p99 {p99:.1} ms above {max} ms"
        ));
    }
    failures
}

/// Verdict of one row given the failed rules from [`decide`].
pub fn verdict(row: &FrontRow, failures: &[String]) -> &'static str {
    let prefix = format!("{}:", row.front);
    let failed = failures.iter().any(|f| f.starts_with(&prefix));
    match (&row.skipped, row.gating, failed) {
        (Some(_), _, _) => "skipped",
        (None, false, _) => "info",
        (None, true, true) => "FAIL",
        (None, true, false) if row.tally.passed == 0 => "no coverage",
        (None, true, false) => "pass",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_nearest_rank() {
        assert_eq!(percentile(&[], 0.5), 0.0);
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        let at = |q| percentile(&v, q);
        assert_eq!((at(0.5), at(0.95), at(0.99)), (50.0, 95.0, 99.0));
        assert_eq!((at(0.0), at(1.0)), (1.0, 100.0));
        let s = summarize(vec![3.0, 1.0, 2.0]);
        assert_eq!((s.n, s.min, s.p50, s.max), (3, 1.0, 2.0, 3.0));
    }

    fn row(front: Front, tally: Tally) -> FrontRow {
        FrontRow::new(front.name(), true, None, tally)
    }

    fn latency(source: &str, p99: f64) -> LatencyRow {
        LatencyRow {
            source: source.into(),
            method: "getAccountInfo base64".into(),
            commitment: "processed".into(),
            latency_ms: Summary {
                p99,
                ..Summary::default()
            },
            requests: 1,
            timeouts: 0,
            http_errors: 0,
            rpc_errors: BTreeMap::new(),
            avg_bytes: 0,
        }
    }

    #[test]
    fn exit_decision() {
        let th = Thresholds {
            max_cross_mismatch: 1,
            max_abandoned_pct: 5.0,
            min_fresh_pct: 50.0,
            max_p99_ms: Some(100.0),
        };
        let mut ok = Tally {
            samples: 100,
            abandoned: 5,
            fresh: 60,
            fresh_of: 100,
            ..Tally::default()
        };
        assert!(decide(&[row(Front::SameNode, ok.clone())], &[], &th).is_empty());

        let mut cross = Tally::default();
        cross.fail("mismatch", "k");
        assert!(decide(&[row(Front::CrossSource, cross.clone())], &[], &th).is_empty());
        cross.fail("mismatch", "k");
        assert_eq!(decide(&[row(Front::CrossSource, cross)], &[], &th).len(), 1);

        let mut forks = Tally::default();
        forks.fail("dead_served", "slot 9");
        let mut info = row(Front::Forks, forks.clone());
        info.gating = false;
        assert!(decide(&[info], &[], &th).is_empty());
        let failures = decide(&[row(Front::Forks, forks)], &[], &th);
        assert_eq!(failures, ["forks: 1 dead_served"]);

        (ok.abandoned, ok.fresh) = (6, 40);
        assert_eq!(decide(&[row(Front::Convergence, ok)], &[], &th).len(), 2);

        let rows = [latency(CLOUDBREAK, 150.0), latency("agave", 500.0)];
        assert_eq!(decide(&[], &rows, &th).len(), 1);
        let no_p99 = Thresholds {
            max_p99_ms: None,
            ..th
        };
        assert!(decide(&[], &rows, &no_p99).is_empty());
    }

    #[test]
    fn verdicts_mark_no_coverage() {
        let mut idle = Tally::default();
        idle.note("unresolved");
        idle.cover(0, 4);
        let idle = row(Front::Forks, idle);
        assert!(decide(std::slice::from_ref(&idle), &[], &Thresholds::default()).is_empty());
        assert_eq!(verdict(&idle, &[]), "no coverage");
        assert_eq!(idle.covered_pct, Some(0.0));

        let passed = row(
            Front::Forks,
            Tally {
                passed: 1,
                ..Tally::default()
            },
        );
        assert_eq!(verdict(&passed, &[]), "pass");
        assert_eq!(verdict(&passed, &["forks: 1 mismatch".into()]), "FAIL");
        let skipped = FrontRow::new("forks", true, Some("why".into()), Tally::default());
        assert_eq!(verdict(&skipped, &[]), "skipped");
        let info = FrontRow::new("metrics", false, None, Tally::default());
        assert_eq!(verdict(&info, &[]), "info");
    }
}
