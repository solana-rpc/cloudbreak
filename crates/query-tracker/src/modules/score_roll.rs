// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Score roll — keeps the windowed activity counters fresh for `Weighted` mode.
//!
//! `PriorityMode::Weighted` ranks patterns by their *recent* demand/supply/
//! failure counts rather than lifetime totals. This task wakes once per
//! `score-rate-window` and calls [`Store::roll_scores`], which records the count
//! observed during the window (`current - prev`) into the `*_rate` columns and
//! snapshots the new totals into `*_prev`. Between rolls the `*_rate` values are
//! held steady, so the ranking is stable within a window (no post-roll noise).
//!
//! Only spawned when the configured mode actually needs it
//! ([`uses_windowed_rate`](cloudbreak_core::PriorityMode::uses_windowed_rate));
//! every other mode ranks on totals and this loop never runs.

use crate::modules::store::Store;
use cloudbreak_core::QueryTrackerConfig;
use tracing::{error, info};

#[tracing::instrument(name = "query_tracker_score_roll", skip_all)]
pub async fn run(store: Store, config: QueryTrackerConfig) {
    let window = config
        .priority_mode
        .rate_window()
        .unwrap_or_else(|| std::time::Duration::from_secs(3600));
    info!(
        target: "query_tracker_score_roll",
        "score roll task started (window: {window:?})"
    );
    loop {
        tokio::time::sleep(window).await;
        match store.roll_scores().await {
            Ok(n) => info!(
                target: "query_tracker_score_roll",
                "rolled windowed scores for {n} pattern(s)"
            ),
            Err(e) => error!(target: "query_tracker_score_roll", "score roll failed: {e:?}"),
        }
    }
}
