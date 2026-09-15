// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Scrapes the API's `/metrics` and reports the two processed series across the run: the share of
//! `requests_total` by route and by reason, and confirm latency quantiles from the histogram
//! bucket deltas with linear interpolation.

use super::Ctx;
use std::collections::BTreeMap;
use std::sync::Arc;

const PREFIX: &str = "cloudbreak_api_processed_";

#[derive(Clone, Debug)]
pub struct Series {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

pub type Snapshot = Vec<Series>;

pub struct MetricsReport {
    pub rows: Vec<(String, String)>,
}

/// Processed series of a Prometheus text body.
pub fn parse(body: &str) -> Snapshot {
    body.lines()
        .filter(|l| l.starts_with(PREFIX))
        .filter_map(parse_line)
        .collect()
}

fn parse_line(line: &str) -> Option<Series> {
    let (head, value) = line.rsplit_once(' ')?;
    let (name, labels) = match head.split_once('{') {
        Some((name, rest)) => (name, rest.strip_suffix('}')?),
        None => (head, ""),
    };
    let pairs = labels.split("\",").filter(|p| !p.is_empty());
    let labels = pairs.filter_map(|p| {
        let (k, v) = p.split_once('=')?;
        Some((
            k.trim_matches(',').to_string(),
            v.trim_matches('"').to_string(),
        ))
    });
    let labels = labels.collect();
    Some(Series {
        name: name.to_string(),
        labels,
        value: value.parse().ok()?,
    })
}

/// One scrape, paced by cloudbreak's token bucket like any other request to the instance.
pub async fn scrape(ctx: &Ctx, url: &str) -> Option<Snapshot> {
    ctx.cloudbreak.pace().await;
    let response = ctx.client.get(url).send().await.ok()?;
    Some(parse(&response.text().await.ok()?))
}

/// Sum of a metric's series grouped by one label value, an empty label groups everything.
pub fn by_label(snap: &Snapshot, name: &str, label: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for s in snap.iter().filter(|s| s.name == format!("{PREFIX}{name}")) {
        let group = s.labels.get(label).cloned().unwrap_or_default();
        *out.entry(group).or_insert(0.0) += s.value;
    }
    out
}

pub fn delta_by_label(
    before: &Snapshot,
    after: &Snapshot,
    name: &str,
    label: &str,
) -> BTreeMap<String, f64> {
    let old = by_label(before, name, label);
    let mut new = by_label(after, name, label);
    new.iter_mut()
        .for_each(|(k, v)| *v -= old.get(k).copied().unwrap_or(0.0));
    new
}

pub fn delta(before: &Snapshot, after: &Snapshot, name: &str) -> f64 {
    delta_by_label(before, after, name, "").values().sum()
}

/// Quantile `q` of a histogram from its bucket deltas, None without observations.
pub fn histogram_quantile(before: &Snapshot, after: &Snapshot, name: &str, q: f64) -> Option<f64> {
    let deltas = delta_by_label(before, after, &format!("{name}_bucket"), "le");
    let mut buckets: Vec<(f64, f64)> = deltas
        .iter()
        .filter_map(|(le, n)| Some((le.parse::<f64>().ok()?, *n)))
        .collect();
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total = buckets.last()?.1;
    if total <= 0.0 {
        return None;
    }
    let rank = q * total;
    let (mut lower, mut below) = (0.0, 0.0);
    for (bound, count) in buckets {
        if count >= rank {
            if bound.is_infinite() {
                return Some(lower);
            }
            let share = if count > below {
                (rank - below) / (count - below)
            } else {
                0.0
            };
            return Some(lower + (bound - lower) * share);
        }
        (lower, below) = (bound, count);
    }
    Some(lower)
}

/// `requests_total` deltas by reason, which the fork front reports across one burst.
pub fn fork_deltas(before: &Snapshot, after: &Snapshot) -> BTreeMap<String, f64> {
    delta_by_label(before, after, "requests_total", "reason")
        .into_iter()
        .map(|(reason, n)| (format!("requests_total{{reason={reason}}}"), n))
        .collect()
}

fn share(part: f64, total: f64) -> String {
    format!("{:.2}%", part * 100.0 / total.max(1.0))
}

fn quantiles(before: &Snapshot, after: &Snapshot, name: &str) -> String {
    let q = |q| {
        histogram_quantile(before, after, name, q).map_or("-".to_string(), |v| format!("{v:.0}"))
    };
    format!("p50 {} p95 {} p99 {}", q(0.5), q(0.95), q(0.99))
}

pub async fn run(ctx: Arc<Ctx>) -> MetricsReport {
    let url = ctx
        .args
        .cloudbreak_metrics
        .clone()
        .expect("metrics front needs --cloudbreak-metrics");
    let failed = |which: &str| MetricsReport {
        rows: vec![("scrape".into(), format!("{which} scrape failed"))],
    };
    let Some(before) = scrape(&ctx, &url).await else {
        return failed("first");
    };
    ctx.stopped().await;
    let Some(after) = scrape(&ctx, &url).await else {
        return failed("final");
    };
    MetricsReport {
        rows: rows(&before, &after),
    }
}

fn rows(before: &Snapshot, after: &Snapshot) -> Vec<(String, String)> {
    let requests = delta(before, after, "requests_total");
    let shares = |label: &str| {
        let groups = delta_by_label(before, after, "requests_total", label);
        let cell = |(k, v): (String, f64)| format!("{k}={}", share(v, requests));
        groups.into_iter().map(cell).collect::<Vec<_>>().join(" ")
    };
    vec![
        ("processed requests".into(), requests.to_string()),
        ("share by route".into(), shares("route")),
        ("share by reason".into(), shares("reason")),
        (
            "confirm_latency_ms".into(),
            quantiles(before, after, "confirm_latency_ms"),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_and_fork_deltas_from_requests_total() {
        let before =
            parse("cloudbreak_api_processed_requests_total{route=\"live\",reason=\"\"} 3\n");
        let after = parse(
            "cloudbreak_api_processed_requests_total{route=\"live\",reason=\"\"} 7\n\
             cloudbreak_api_processed_requests_total{route=\"confirmed\",reason=\"gap\"} 1\n",
        );
        let rows: BTreeMap<String, String> = rows(&before, &after).into_iter().collect();
        assert_eq!(rows["processed requests"], "5");
        assert_eq!(rows["share by route"], "confirmed=20.00% live=80.00%");
        assert_eq!(rows["share by reason"], "=80.00% gap=20.00%");
        assert_eq!(rows["confirm_latency_ms"], "p50 - p95 - p99 -");
        let deltas = fork_deltas(&before, &after);
        assert_eq!(deltas["requests_total{reason=gap}"], 1.0);
        assert_eq!(deltas.len(), 2);
    }
}
