// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Indexer backpressure — use the indexer metrics to decide how busy is the DB.
//!
//! CREATE INDEX and DROP INDEX both take heavy locks on the hot `accounts` /
//! `snapshot_accounts` tables. When the indexer is behind (its
//! `cloudbreak_finalize_slot_handler_queue_size` is high) we defer DDL so we do
//! not make ingest lag worse. Both the creation loop and the eviction pass gate
//! on this.

use tracing::{debug, error};

/// Scrape the indexer's Prometheus endpoint for the finalize-slot queue size.
/// Returns `None` on any transport/parse failure (caller treats that as
/// "cannot confirm safe" and defers).
pub async fn read_indexer_queue_size(metrics_url: &str) -> Option<u64> {
    let client = reqwest::Client::new();
    let body = client
        .get(metrics_url)
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;

    for line in body.lines() {
        if line.starts_with("cloudbreak_finalize_slot_handler_queue_size") {
            let value = line
                .split_whitespace()
                .last()
                .and_then(|v| v.parse::<u64>().ok());
            debug!(target: "query_tracker_backpressure", "indexer queue size: {value:?}");
            return value;
        }
    }
    None
}

/// `true` when DDL(CREATE/DROP INDEX) should be deferred: the indexer queue is above `threshold`,
/// or we could not read it at all.
pub async fn is_under_pressure(metrics_url: &str, threshold: u64) -> bool {
    match read_indexer_queue_size(metrics_url).await {
        Some(size) if size > threshold => {
            debug!(
                target: "query_tracker_backpressure",
                "indexer queue {size} > threshold {threshold}; deferring DDL"
            );
            true
        }
        Some(_) => false,
        None => {
            error!(
                target: "query_tracker_backpressure",
                "failed to read indexer metrics at {metrics_url}; deferring DDL"
            );
            true
        }
    }
}
