// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Metrics shared across crates. The collectors are defined here so the modules
//! that own their logic (in `core`) can record them directly. The index crate
//! registers them with its Prometheus registry via `register_collectors`.

use prometheus::{
    Counter, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
};

lazy_static::lazy_static! {
    /// Current number of live Tokio tasks, labelled by task type.
    pub static ref CURRENT_TOKIO_TASKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("cloudbreak_current_tokio_tasks", "Current number of Tokio tasks"),
        &["task_type"],
    )
    .expect("Failed to create current tokio tasks gauge");

    pub static ref GRPC_BUFFER_CHANNEL_SIZE_SENDER: IntGauge = IntGauge::new(
        "cloudbreak_grpc_buffer_channel_size_sender", "Size of the GRPC buffer channel sender"
    )
    .expect("Failed to create GRPC buffer channel size sender gauge");

    pub static ref GRPC_TIMEOUT_ERRORS: Counter = Counter::new(
        "cloudbreak_grpc_timeout_errors", "Number of GRPC timeout errors"
    )
    .expect("Failed to create GRPC timeout errors counter");

    pub static ref GRPC_ERRORS: Counter = Counter::new(
        "cloudbreak_grpc_errors", "Number of GRPC errors"
    )
    .expect("Failed to create GRPC errors counter");

    pub static ref GRPC_TOTAL_UPDATES_RECEIVED: Counter = Counter::new(
        "cloudbreak_grpc_total_updates_received", "Number of GRPC updates received"
    )
    .expect("Failed to create GRPC total updates received counter");

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

    /// Mint cache entries by `side` (`api`, `indexer`) and map (`immutable`, `mutable`, `tombstone`).
    pub static ref MINT_CACHE_ENTRIES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("cloudbreak_mint_cache_entries", "Mint cache entries by side and map"),
        &["side", "map"],
    )
    .expect("Failed to create mint cache entries gauge");

    /// Mint changes applied per confirmed block, by `side` and kind.
    pub static ref MINT_CACHE_CHANGES_PER_BLOCK: HistogramVec = HistogramVec::new(
        HistogramOpts::new("cloudbreak_mint_cache_changes_per_block", "Mint changes applied per confirmed block, by side and kind")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1_000.0]),
        &["side", "kind"],
    )
    .expect("Failed to create mint cache changes histogram");

    /// Mint cache time in microseconds, by `side` and stage. Indexer: `classify`, `apply` per
    /// block, `write` per write to `mints`. API: `ingest` (feed thread), `apply` (confirmed slot).
    pub static ref MINT_CACHE_BLOCK_MICROSECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("cloudbreak_mint_cache_block_microseconds", "Mint cache time in microseconds, by side and stage")
            .buckets(vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
                25_000.0, 50_000.0, 100_000.0, 250_000.0, 500_000.0, 1_000_000.0,
            ]),
        &["side", "stage"],
    )
    .expect("Failed to create mint cache block histogram");

    /// Seconds until the cache was ready, by `side`. Indexer: the final `mints` write of the
    /// bootstrap. API: the full `mints` table load.
    pub static ref MINT_CACHE_STARTUP_SECONDS: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_mint_cache_startup_seconds", "Mint cache startup duration by side"),
        &["side"],
    )
    .expect("Failed to create mint cache startup gauge");

    /// Indexer: mint writes with no stored change (supply only).
    pub static ref MINT_CACHE_UNCHANGED_WRITES_TOTAL: IntCounter = IntCounter::new(
        "cloudbreak_mint_cache_unchanged_writes_total", "Mint writes that changed nothing jsonParsed reads"
    )
    .expect("Failed to create mint cache unchanged writes counter");

    /// Indexer: failed writes to `mints`. The rows are retried with the next write.
    pub static ref MINT_CACHE_DB_WRITE_ERRORS_TOTAL: IntCounter = IntCounter::new(
        "cloudbreak_mint_cache_db_write_errors_total", "Failed writes to the mints table"
    )
    .expect("Failed to create mint cache write errors counter");

    /// API: mint lookups by where they were served and why.
    pub static ref MINT_CACHE_LOOKUPS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("cloudbreak_mint_cache_lookups_total", "Mint lookups by source and reason"),
        &["source", "reason"],
    )
    .expect("Failed to create mint cache lookups counter");

    /// API: `mints` table reloads by reason and result.
    pub static ref MINT_CACHE_RELOADS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("cloudbreak_mint_cache_reloads_total", "Mint cache reloads from the mints table"),
        &["reason", "result"],
    )
    .expect("Failed to create mint cache reloads counter");

    /// API: blocks waiting in the mint buffer for their confirmed notification.
    pub static ref MINT_CACHE_BUFFER_BLOCKS: IntGauge = IntGauge::new(
        "cloudbreak_mint_cache_buffer_blocks", "Blocks in the mint buffer"
    )
    .expect("Failed to create mint cache buffer gauge");

    /// API: last confirmed slot applied to the mint cache.
    pub static ref MINT_CACHE_APPLIED_THROUGH: IntGauge = IntGauge::new(
        "cloudbreak_mint_cache_applied_through", "Last confirmed slot applied to the mint cache"
    )
    .expect("Failed to create mint cache applied-through gauge");
}

pub fn increment_grpc_timeout_errors() {
    GRPC_TIMEOUT_ERRORS.inc();
}

pub fn increment_grpc_errors() {
    GRPC_ERRORS.inc();
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

// ============================================================================
// Mint cache helpers
// ============================================================================

/// Which half of the mint cache a metric comes from: the `side` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MintCacheSide {
    Api,
    Indexer,
}

impl MintCacheSide {
    fn label(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Indexer => "indexer",
        }
    }
}

/// Counts one API mint cache lookup in `cloudbreak_mint_cache_lookups_total`.
pub fn count_mint_lookup(source: &str, reason: &str) {
    MINT_CACHE_LOOKUPS_TOTAL
        .with_label_values(&[source, reason])
        .inc();
}

/// Sets `cloudbreak_mint_cache_entries` for `side` per map: `immutable`, `mutable`, `tombstone`.
pub fn set_mint_entries(side: MintCacheSide, store: &crate::modules::mint_cache::MintStore) {
    for (map, count) in [
        ("immutable", store.immutable_len()),
        ("mutable", store.mutable_len()),
        ("tombstone", store.tombstones()),
    ] {
        MINT_CACHE_ENTRIES
            .with_label_values(&[side.label(), map])
            .set(count as i64);
    }
}

/// Sets the API mint cache gauges: the entries per map and the applied slot.
pub fn set_mint_cache_api_gauges(
    store: &crate::modules::mint_cache::MintStore,
    applied_through: u64,
) {
    set_mint_entries(MintCacheSide::Api, store);
    MINT_CACHE_APPLIED_THROUGH.set(applied_through as i64);
}

/// Records one block's mint changes of `kind` for `side` in
/// `cloudbreak_mint_cache_changes_per_block`.
pub fn observe_mint_changes(side: MintCacheSide, kind: &str, count: usize) {
    MINT_CACHE_CHANGES_PER_BLOCK
        .with_label_values(&[side.label(), kind])
        .observe(count as f64);
}

/// Records the time since `started` for `side` and `stage` in
/// `cloudbreak_mint_cache_block_microseconds`.
pub fn observe_mint_cache_stage(side: MintCacheSide, stage: &str, started: std::time::Instant) {
    MINT_CACHE_BLOCK_MICROSECONDS
        .with_label_values(&[side.label(), stage])
        .observe(started.elapsed().as_micros() as f64);
}

/// Sets `cloudbreak_mint_cache_startup_seconds` for `side`.
pub fn set_mint_cache_startup_seconds(side: MintCacheSide, seconds: f64) {
    MINT_CACHE_STARTUP_SECONDS
        .with_label_values(&[side.label()])
        .set(seconds);
}
