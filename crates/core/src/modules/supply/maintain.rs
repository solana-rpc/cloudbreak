// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The cache sweeper: a slot-driven, health-gated background task that evicts
//! stale unpinned entries off the block path. No-op when the feature is disabled.

use crate::metrics::TokioTaskCounterGuard;
use crate::modules::service_health::is_healthy;
use crate::modules::supply::tracker::SupplyTracker;
use sea_orm::DatabaseConnection;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;

/// Spawns the sweeper. It wakes on each finalize-slot change, skips while the
/// node is unhealthy, and runs the sweep only when it is due.
pub fn spawn_supply_cache_sweeper(
    db: DatabaseConnection,
    tracker: SupplyTracker,
    mut slot_rx: Receiver<u64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _guard = TokioTaskCounterGuard::new("supply_cache_sweeper");
        if !tracker.is_enabled() {
            return;
        }
        loop {
            if slot_rx.changed().await.is_err() {
                return;
            }
            let slot = *slot_rx.borrow_and_update();
            if slot == 0 || !is_healthy(&db).await {
                continue;
            }
            tracker.sweep_if_due(slot);
        }
    })
}
