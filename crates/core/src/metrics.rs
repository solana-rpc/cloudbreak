// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Metrics shared across crates. The collectors are defined here so the modules
//! that own their logic (in `core`) can record them directly. The index crate
//! registers them with its Prometheus registry via `register_collectors`.

use prometheus::{
    Counter, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
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

    /// Hot-accounts cache population, labelled `pinned` (by a write failure)
    /// and `hot` (unpinned).
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

    /// Supply block path: `apply_block` entry (lock wait included) to the last
    /// miss write-back, in microseconds.
    pub static ref SUPPLY_BLOCK_MICROSECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("cloudbreak_supply_block_microseconds", "Per-block supply delta time in microseconds")
            .buckets(vec![
                100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0,
                100_000.0, 250_000.0, 500_000.0, 1_000_000.0,
            ]),
    )
    .expect("Failed to create supply block histogram");

    /// Supply phase one: the cache probes under the state mutex, in microseconds.
    pub static ref SUPPLY_PROBE_MICROSECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("cloudbreak_supply_probe_microseconds", "Per-block supply cache probe time in microseconds")
            .buckets(vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0]),
    )
    .expect("Failed to create supply probe histogram");

    /// Supply DB queries in microseconds, labelled `prev_balances` (the miss
    /// read), `persist_row` (the row upsert) and `startup_balances` (the resolve).
    pub static ref SUPPLY_DB_MICROSECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("cloudbreak_supply_db_microseconds", "Supply DB query time in microseconds")
            .buckets(vec![
                500.0, 1_000.0, 2_500.0, 5_000.0, 7_500.0, 10_000.0, 15_000.0, 20_000.0, 30_000.0,
                50_000.0, 100_000.0, 250_000.0, 1_000_000.0,
            ]),
        &["query"],
    )
    .expect("Failed to create supply db histogram");

    /// One hot-accounts cache sweep pass, in microseconds.
    pub static ref SUPPLY_SWEEP_MICROSECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("cloudbreak_supply_sweep_microseconds", "Supply cache sweep time in microseconds")
            .buckets(vec![
                100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0, 100_000.0, 250_000.0, 500_000.0,
                1_000_000.0, 5_000_000.0,
            ]),
    )
    .expect("Failed to create supply sweep histogram");

    /// The per-block stake map apply plus the expiry pop, in microseconds.
    pub static ref NON_CIRCULATING_BLOCK_MICROSECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("cloudbreak_non_circulating_block_microseconds", "Per-block non-circulating membership time in microseconds")
            .buckets(vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0]),
    )
    .expect("Failed to create non-circulating block histogram");

    /// Current non-circulating member count.
    pub static ref NON_CIRCULATING_MEMBERS: IntGauge = IntGauge::new(
        "cloudbreak_non_circulating_members", "Non-circulating member count"
    )
    .expect("Failed to create non-circulating members gauge");

    /// Membership changes, labelled `join` and `leave`.
    pub static ref NON_CIRCULATING_CHANGES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("cloudbreak_non_circulating_changes_total", "Non-circulating membership changes by kind"),
        &["kind"],
    )
    .expect("Failed to create non-circulating changes counter");

    /// Time from receiving a processed block to the store applying the Postgres
    /// anchor that covers it, for blocks on the confirmed chain. Includes the
    /// slot syncronizer poll.
    pub static ref PROCESSED_CONFIRM_LATENCY_MS: Histogram = Histogram::with_opts(
        HistogramOpts::new("cloudbreak_api_processed_confirm_latency_ms", "Processed block receipt to Postgres confirmed, in milliseconds")
            .buckets(vec![
                100.0, 200.0, 300.0, 400.0, 500.0, 750.0, 1_000.0, 1_500.0, 2_000.0, 3_000.0,
                5_000.0, 10_000.0, 20_000.0,
            ]),
    )
    .expect("Failed to create processed confirm latency histogram");
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
