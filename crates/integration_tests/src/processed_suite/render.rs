// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Text rendering of the final report as aligned tables.

use super::Tally;
use super::report::{Coverage, Report};
use std::fmt::Write;

macro_rules! cells {
    ($($cell:expr),* $(,)?) => { vec![$($cell.to_string()),*] };
}

/// Left-aligned columns separated by two spaces. `headers` is a `|` separated list.
pub fn table(headers: &str, rows: &[Vec<String>]) -> String {
    let headers: Vec<String> = headers.split('|').map(str::to_string).collect();
    let mut widths: Vec<usize> = headers.iter().map(String::len).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }
    let line = |cells: &[String]| {
        let padded = cells.iter().zip(&widths).map(|(c, w)| format!("{c:<w$}"));
        padded.collect::<Vec<_>>().join("  ").trim_end().to_string() + "\n"
    };
    let rule: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    let mut out = line(&headers) + &line(&rule);
    rows.iter().for_each(|row| out += &line(row));
    out
}

fn section(out: &mut String, title: &str, headers: &str, rows: Vec<Vec<String>>) {
    let _ = write!(out, "\n== {title} ==\n{}", table(headers, &rows));
}

fn classes(t: &Tally) -> String {
    let failing = t.failures.iter().map(|(c, n)| format!("{c}={n}!"));
    let other = t.classes.iter().map(|(c, n)| format!("{c}={n}"));
    failing.chain(other).collect::<Vec<_>>().join(" ")
}

fn ms(v: f64) -> String {
    format!("{v:.1}")
}

pub fn render(r: &Report) -> String {
    let mut out = String::new();
    let rows = r.correctness.iter().map(|row| {
        let t = &row.tally;
        let detail = row.skipped.clone().unwrap_or_else(|| classes(t));
        let covered = row
            .covered_pct
            .map_or("-".to_string(), |p| format!("{p:.1}"));
        cells![
            row.front,
            row.verdict,
            t.samples,
            t.passed,
            t.failed,
            covered,
            detail
        ]
    });
    let headers = "front|verdict|samples|passed|failed|covered%|classes (! fails)";
    section(&mut out, "correctness", headers, rows.collect());

    let rows = r.latency.iter().map(|l| {
        let (s, err) = (l.latency_ms, format!("{:.2}", l.error_pct()));
        let (p50, p95, p99, max) = (ms(s.p50), ms(s.p95), ms(s.p99), ms(s.max));
        let (source, method, commitment) = (&l.source, &l.method, &l.commitment);
        cells![
            source,
            method,
            commitment,
            l.requests,
            s.n,
            p50,
            p95,
            p99,
            max,
            l.timeouts,
            err,
            l.avg_bytes
        ]
    });
    let headers = "source|method|commitment|requests|ok|p50|p95|p99|max|timeouts|err%|avg bytes";
    section(
        &mut out,
        "speed: latency (ok replies, from send)",
        headers,
        rows.collect(),
    );

    let rows = r.read_after_write.iter().map(|w| {
        let (s, q) = (w.propagation_ms, w.queued_ms);
        let (p50, p95, p99) = (ms(s.p50), ms(s.p95), ms(s.p99));
        let queue = format!("{} / {}", ms(q.p50), ms(q.p99));
        cells![
            w.source, s.n, p50, p95, p99, queue, w.timeouts, w.dropped, w.excluded
        ]
    });
    let headers = "source|done|p50|p95|p99|queue p50 / p99|timeouts|dropped|excluded";
    let title = "speed: read-after-write (ms from block receipt, queue wait removed)";
    section(&mut out, title, headers, rows.collect());

    let rows = r.freshness.iter().map(|f| {
        let (b, fresh) = (f.behind_tip, format!("{:.1}", f.fresh_pct));
        cells![f.source, f.samples, fresh, b.p50, b.p95, b.p99, b.max]
    });
    let headers = "source|samples|fresh%|behind tip p50|p95|p99|max";
    section(
        &mut out,
        "speed: freshness (processed)",
        headers,
        rows.collect(),
    );

    let rows = r.slot_delta.iter().map(|d| {
        let counts = d.counts.iter().map(|(k, n)| format!("{k:+}:{n}"));
        let counts = counts.collect::<Vec<_>>().join(" ");
        cells![d.reference, format!("{:.1}", d.equal_pct), counts]
    });
    let title = "speed: slot delta (cloudbreak minus reference, processed)";
    section(
        &mut out,
        title,
        "reference|equal%|delta:count",
        rows.collect(),
    );

    if !r.metrics.is_empty() || !r.fork_metric_deltas.is_empty() {
        let metrics = r.metrics.iter().map(|(k, v)| cells![k, v]);
        let deltas =
            (r.fork_metric_deltas.iter()).map(|(k, v)| cells![format!("fork bursts {k}"), v]);
        let rows = metrics.chain(deltas).collect();
        section(&mut out, "metrics", "metric|value", rows);
    }

    section(
        &mut out,
        "coverage",
        "item|value",
        coverage_rows(&r.coverage),
    );

    for row in r
        .correctness
        .iter()
        .filter(|row| !row.tally.examples.is_empty())
    {
        let _ = writeln!(out, "\n{} examples:", row.front);
        row.tally
            .examples
            .iter()
            .for_each(|e| _ = writeln!(out, "  {e}"));
    }
    let _ = writeln!(out);
    if r.failures.is_empty() {
        out += "PASS\n";
    }
    r.failures
        .iter()
        .for_each(|f| _ = writeln!(out, "FAIL {f}"));
    out
}

fn coverage_rows(c: &Coverage) -> Vec<Vec<String>> {
    let pair =
        |a: &dyn ToString, b: &dyn ToString| format!("{} / {}", a.to_string(), b.to_string());
    let mut rows = vec![
        cells!["run time", format!("{} s", c.run_secs)],
        cells!["stopped by Ctrl-C", c.interrupted],
        cells!["blocks seen", c.blocks],
        cells![
            "forks / dead / restarted slots",
            format!("{} / {} / {}", c.forks, c.dead_slots, c.restarted_slots)
        ],
        cells![
            "bursts / responses / regressions",
            format!(
                "{} / {} / {}",
                c.bursts, c.burst_responses, c.burst_regressions
            )
        ],
        cells!["watcher reconnects", c.watcher_reconnects],
        cells![
            "keys in pool / sampled",
            pair(&c.pool_keys, &c.keys_sampled)
        ],
        cells![
            "getBlocks checked / disagreements",
            pair(&c.getblocks_checked, &c.getblocks_disagreements)
        ],
        cells!["max RSS", format!("{:.0} MB", c.max_rss_mb)],
    ];
    for rate in &c.source_rates {
        let value = format!(
            "{} requests, {:.2} rps of {} cap",
            rate.requests, rate.rps, rate.cap_rps
        );
        rows.push(cells![format!("rate {}", rate.source), value]);
    }
    let ticks = c.load_skipped_ticks.iter();
    rows.extend(ticks.map(|(source, n)| cells![format!("latency skipped ticks {source}"), n]));
    let skipped = c.skipped.iter();
    rows.extend(skipped.map(|(front, why)| cells![format!("skipped {front}"), why]));
    let examples = c.disagreement_examples.iter();
    rows.extend(examples.map(|e| cells!["disagreement", e]));
    rows
}
