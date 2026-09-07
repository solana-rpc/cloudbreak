// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Metrics shared across crates. The collectors are defined here so the modules
//! that own their logic (in `core`) can record them directly. The index crate
//! registers them with its Prometheus registry via `register_collectors`.

use prometheus::{
    Counter, IntCounter, IntGauge, IntGaugeVec, Opts,
};

lazy_static::lazy_static! {
    /// Current number of live Tokio tasks, labelled by task type.
    pub static ref CURRENT_TOKIO_TASKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("cloudbreak_current_tokio_tasks", "Current number of Tokio tasks"),
        &["task_type"],
    )
    .expect("Failed to create current tokio tasks gauge");

    pub static ref LARGEST_ACCOUNTS_DB_ERRORS: Counter = Counter::new(
        "cloudbreak_largest_accounts_db_errors", "Number of largest accounts DB write/prune errors"
    )
    .expect("Failed to create largest accounts DB errors counter");

    pub static ref LARGEST_ACCOUNTS_STALE_MINTS: IntGauge = IntGauge::new(
        "cloudbreak_largest_accounts_stale_mints", "Number of tracked mints marked stale in the largest accounts tracker"
    )
    .expect("Failed to create largest accounts stale mints gauge");

    /// Hot-accounts cache population, labelled `pinned` (stake) and `hot` (unpinned).
    pub static ref SUPPLY_CACHE_ENTRIES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("cloudbreak_supply_cache_entries", "Hot-accounts cache entries by kind"),
        &["kind"],
    )
    .expect("Failed to create supply cache entries gauge");

    /// Per-block cache hits (previous balance served from memory).
    pub static ref SUPPLY_CACHE_HITS_TOTAL: IntCounter = IntCounter::new(
        "cloudbreak_supply_cache_hits_total", "Hot-accounts cache hits"
    )
    .expect("Failed to create supply cache hits counter");

    /// Per-block cache misses (previous balance resolved by a DB read).
    pub static ref SUPPLY_CACHE_MISSES_TOTAL: IntCounter = IntCounter::new(
        "cloudbreak_supply_cache_misses_total", "Hot-accounts cache misses"
    )
    .expect("Failed to create supply cache misses counter");

}

/// We use a guard to increment the current tokio tasks metric when a task is created and
/// decrement it when the task is dropped. This way the counter is going to be decremented
/// even in the case of panics.
pub struct TokioTaskCounterGuard {
    task_type: String,
}

impl TokioTaskCounterGuard {
    pub fn new(task_type: &str) -> Self {
        CURRENT_TOKIO_TASKS.with_label_values(&[task_type]).inc();
        Self {
            task_type: task_type.to_string(),
        }
    }

    pub fn decrement(&self) {
        CURRENT_TOKIO_TASKS
            .with_label_values(&[&self.task_type])
            .dec();
    }
}

impl Drop for TokioTaskCounterGuard {
    fn drop(&mut self) {
        self.decrement();
    }
}
