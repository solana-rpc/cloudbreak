// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Metrics — Prometheus registry and the gauges/counters the tracker exposes.
//!
//! This module only *defines and holds* the metrics; the `/metrics` endpoint is
//! served by `server::operational`. Per-index gauges are labelled with the
//! human-readable index name (see `IndexIdentity::human_name`) so a dashboard
//! can show, per active index, both its demand (API) and its supply
//! (`idx_scan`) — which is exactly what makes discrepancies visible.

use prometheus::{IntCounter, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    /// Current number of indexes present on the capped table.
    pub static ref SNAPSHOT_ACCOUNTS_INDEXES: IntGauge = register_gauge(
        "query_tracker_snapshot_accounts_indexes_total",
        "Current number of indexes on the snapshot_accounts table",
    );

    /// Distinct index patterns by lifecycle `status` (`created` / `candidate` /
    /// `evicted` / `rejected`). These partition the `index_patterns` table, so
    /// the total is `sum(query_tracker_patterns)` in PromQL and is intentionally
    /// not a separate series (a `total` label value would double-count on
    /// aggregation).
    pub static ref PATTERNS: IntGaugeVec = register_gauge_vec(
        "query_tracker_patterns",
        "Number of distinct index patterns tracked, by lifecycle status",
        "status",
    );

    /// Patterns currently flagged as demand/supply discrepant.
    pub static ref DISCREPANT_INDEXES: IntGauge = register_gauge(
        "query_tracker_discrepant_indexes",
        "Number of created indexes where demand and supply currently diverge",
    );

    /// Cumulative index-pair creations.
    pub static ref INDEX_CREATED_TOTAL: IntCounter = register_counter(
        "query_tracker_index_created_total",
        "Cumulative number of auto-index pairs created",
    );

    /// Cumulative index-pair evictions.
    pub static ref INDEX_EVICTED_TOTAL: IntCounter = register_counter(
        "query_tracker_index_evicted_total",
        "Cumulative number of auto-index pairs evicted",
    );

    /// Cumulative failed creation attempts.
    pub static ref INDEX_CREATE_FAILURES_TOTAL: IntCounter = register_counter(
        "query_tracker_index_create_failures_total",
        "Cumulative number of failed auto-index creation attempts",
    );

    /// Created indexes **currently** measuring slower with the index than
    /// without it (a latency regression). Refreshed by the eviction pass's
    /// regression guard; stays 0 when the guard is off. In `warn` mode it
    /// reflects the regressions being tolerated; in `evict` mode it stays near
    /// zero, since drops move those patterns into the `rejected` status
    /// (`query_tracker_patterns{status="rejected"}`).
    pub static ref REGRESSED_INDEXES: IntGauge = register_gauge(
        "query_tracker_regressed_indexes",
        "Number of created indexes currently slower with the index than without it",
    );

    /// Cumulative accepted track observations.
    pub static ref OBSERVATIONS_TOTAL: IntCounter = register_counter(
        "query_tracker_observations_total",
        "Cumulative number of accepted demand observations",
    );

    /// Per-index demand (cumulative API request count), labelled by index name.
    pub static ref INDEX_DEMAND: IntGaugeVec = register_gauge_vec(
        "query_tracker_index_demand",
        "Cumulative API demand (request count) per created index",
        "index",
    );

    /// Per-index supply, labelled by index name. This is the **compensated**
    /// `idx_scan` (raw ÷ `SCANS_PER_REQUEST`, since a served GPA scans both
    /// tables of the pair), so it lines up ~1:1 with `INDEX_DEMAND` for a healthy
    /// index — the raw Postgres counter is never exported.
    pub static ref INDEX_COMPENSATED_IDX_SCAN: IntGaugeVec = register_gauge_vec(
        "query_tracker_index_compensated_idx_scan",
        "Compensated Postgres idx_scan (supply, raw idx_scan / 2) per created index",
        "index",
    );

    /// Per-index distinct-value variety estimate, labelled by index name.
    pub static ref INDEX_VARIETY: IntGaugeVec = register_gauge_vec(
        "query_tracker_index_variety",
        "Estimated distinct filter values served per created index",
        "index",
    );

    /// Result of the **last** EXPLAIN sampling pass: number of created indexes in
    /// each `explain_state` (`none` / `accounts_table` / `snapshot_accounts_table`
    /// / `both`). Overwritten every pass, so it is a snapshot of the latest run,
    /// not a cumulative count; `none` is the "planner would not use it on either
    /// table" bucket worth alerting on. Stays at its initial zeros when
    /// `explain-enabled` is off (the pass never runs).
    pub static ref EXPLAIN_STATE: IntGaugeVec = register_gauge_vec(
        "query_tracker_explain_state",
        "Created indexes by planner-usage verdict from the last EXPLAIN sampling pass",
        "state",
    );
}

fn register_gauge(name: &str, help: &str) -> IntGauge {
    let g = IntGauge::new(name, help).expect("failed to create gauge");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("failed to register gauge");
    g
}

fn register_counter(name: &str, help: &str) -> IntCounter {
    let c = IntCounter::new(name, help).expect("failed to create counter");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("failed to register counter");
    c
}

fn register_gauge_vec(name: &str, help: &str, label: &str) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), &[label]).expect("failed to create gaugevec");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("failed to register gaugevec");
    g
}

/// Render the registry in Prometheus text format.
pub fn encode() -> String {
    TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            tracing::error!(target: "query_tracker_metrics", "could not encode metrics: {error}");
            String::new()
        })
}

/// Force-initialise the lazy statics so metrics appear before the first update.
pub fn init() {
    lazy_static::initialize(&SNAPSHOT_ACCOUNTS_INDEXES);
    lazy_static::initialize(&PATTERNS);
    lazy_static::initialize(&DISCREPANT_INDEXES);
    lazy_static::initialize(&INDEX_CREATED_TOTAL);
    lazy_static::initialize(&INDEX_EVICTED_TOTAL);
    lazy_static::initialize(&INDEX_CREATE_FAILURES_TOTAL);
    lazy_static::initialize(&REGRESSED_INDEXES);
    lazy_static::initialize(&OBSERVATIONS_TOTAL);
    lazy_static::initialize(&INDEX_DEMAND);
    lazy_static::initialize(&INDEX_COMPENSATED_IDX_SCAN);
    lazy_static::initialize(&INDEX_VARIETY);
    lazy_static::initialize(&EXPLAIN_STATE);
}
