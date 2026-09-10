// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Indexer backpressure — use the indexer metrics to decide how busy is the DB.
//!
//! CREATE INDEX and DROP INDEX both take heavy locks on the hot `accounts` /
//! `snapshot_accounts` tables. When the indexer is behind we defer DDL so we do
//! not make ingest lag worse. Both the creation loop and the eviction pass gate
//! on this.
//!
//! Two gauges say the indexer is behind, and either one defers.
//! `cloudbreak_finalize_slot_handler_queue_size` is the finalize backlog, which a gap-fill pause
//! still builds. `cloudbreak_cleanup_lag_slots` is the cleanup backlog, which is the signal that
//! database pressure shows up in once cleanup runs off the finalize worker. Reading only the
//! first would report "safe" while the cleanup drainer is drowning.
//!
//! The cleanup gauge is optional: an indexer that does not publish it reads as no pressure, so
//! this is inert against a build without a cleanup drainer. The finalize gauge is not optional.
//! A body that does not carry it is not a healthy indexer answering, it is the wrong endpoint or
//! one that has not registered its collectors, and that defers. An endpoint that cannot be read
//! at all defers too.

use tracing::{debug, error};

/// The indexer gauges that gate DDL. `None` means the indexer does not publish that gauge.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexerPressure {
    pub finalize_queue: Option<u64>,
    pub cleanup_lag: Option<u64>,
}

/// Scrape the indexer's Prometheus endpoint for both backpressure gauges.
/// Returns `None` on any transport/parse failure (caller treats that as
/// "cannot confirm safe" and defers).
pub async fn read_indexer_pressure(metrics_url: &str) -> Option<IndexerPressure> {
    let client = reqwest::Client::new();
    let body = client
        .get(metrics_url)
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;

    Some(parse_indexer_pressure(&body))
}

/// Reads both gauges out of a Prometheus exposition body.
fn parse_indexer_pressure(body: &str) -> IndexerPressure {
    let mut pressure = IndexerPressure::default();
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(value) = gauge_value(line, "cloudbreak_finalize_slot_handler_queue_size") {
            pressure.finalize_queue = Some(value);
        } else if let Some(value) = gauge_value(line, "cloudbreak_cleanup_lag_slots") {
            pressure.cleanup_lag = Some(value);
        }
    }
    debug!(target: "query_tracker_backpressure", "indexer pressure: {pressure:?}");
    pressure
}

/// Parses `<name> <value>`, taking a negative gauge as zero.
fn gauge_value(line: &str, name: &str) -> Option<u64> {
    let rest = line.strip_prefix(name)?;
    if !rest.starts_with(' ') {
        return None;
    }
    let value: f64 = rest.split_whitespace().last()?.parse().ok()?;
    Some(if value < 0.0 { 0 } else { value as u64 })
}

/// `true` when DDL(CREATE/DROP INDEX) should be deferred: either indexer gauge is above its
/// threshold, or the endpoint could not be read at all.
///
/// A gauge the indexer does not publish is not pressure. That keeps this inert against an
/// indexer build that has no cleanup drainer.
pub async fn is_under_pressure(
    metrics_url: &str,
    queue_threshold: u64,
    cleanup_lag_threshold: u64,
) -> bool {
    let Some(pressure) = read_indexer_pressure(metrics_url).await else {
        error!(
            target: "query_tracker_backpressure",
            "failed to read indexer metrics at {metrics_url}; deferring DDL"
        );
        return true;
    };

    let Some(finalize_queue) = pressure.finalize_queue else {
        error!(
            target: "query_tracker_backpressure",
            "indexer metrics at {metrics_url} carry no finalize queue gauge; deferring DDL"
        );
        return true;
    };

    if finalize_queue > queue_threshold {
        debug!(
            target: "query_tracker_backpressure",
            "indexer finalize queue {finalize_queue} > threshold {queue_threshold}; deferring DDL"
        );
        return true;
    }
    if pressure
        .cleanup_lag
        .is_some_and(|lag| lag > cleanup_lag_threshold)
    {
        debug!(
            target: "query_tracker_backpressure",
            "indexer cleanup lag {:?} > threshold {cleanup_lag_threshold}; deferring DDL",
            pressure.cleanup_lag
        );
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "# HELP cloudbreak_finalize_slot_handler_queue_size q\ncloudbreak_finalize_slot_handler_queue_size 3\ncloudbreak_cleanup_lag_slots 7\n";

    #[test]
    fn reads_both_gauges() {
        let pressure = parse_indexer_pressure(BODY);
        assert_eq!(pressure.finalize_queue, Some(3));
        assert_eq!(pressure.cleanup_lag, Some(7));
    }

    #[test]
    fn an_absent_cleanup_gauge_is_not_pressure() {
        let pressure = parse_indexer_pressure("cloudbreak_finalize_slot_handler_queue_size 1\n");
        assert_eq!(pressure.finalize_queue, Some(1));
        assert_eq!(pressure.cleanup_lag, None);
    }

    #[test]
    fn an_absent_finalize_gauge_is_pressure() {
        // The finalize gauge is not optional: a body without it is not a healthy indexer.
        let pressure = parse_indexer_pressure("cloudbreak_cleanup_lag_slots 0\n");
        assert_eq!(pressure.finalize_queue, None);
    }

    #[test]
    fn a_comment_line_is_not_a_sample() {
        let pressure = parse_indexer_pressure("# TYPE cloudbreak_cleanup_lag_slots gauge\n");
        assert_eq!(pressure.cleanup_lag, None);
    }

    #[test]
    fn a_negative_gauge_reads_as_zero() {
        let pressure = parse_indexer_pressure("cloudbreak_cleanup_lag_slots -1\n");
        assert_eq!(pressure.cleanup_lag, Some(0));
    }
}
