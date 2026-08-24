// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `index_patterns` — the single durable table backing the reworked query
//! tracker.
//!
//! One row per `IndexIdentity` (keyed by its deterministic `pattern_id`) holds
//! **everything** the tracker needs to make decisions, so the service is fully
//! restartable and never loses history:
//!
//! - identity: `program`, `offsets_lengths` (JSONB), `datasize`, `human_name`
//! - `example_request`: one representative request (`RpcProgramAccountsConfig`
//!   JSON) that maps to this identity, captured on first sight and kept. Used to
//!   build realistic `EXPLAIN` probes (real filter values instead of zeros) and
//!   for documentation/sampling on the debug endpoints.
//! - demand (API side): `demand_count`, `demand_at_create`, `total_cost_us`,
//!   `failed_count`, `variety_hll` / `variety_estimate`, `first_seen_at`,
//!   `last_demand_at`
//! - latency (with vs without the index): `cost_with_index_us`/`_count`,
//!   `cost_without_index_us`/`_count` — cost is routed into the matching bucket
//!   by the row's `status` at record time, so we can compare how fast the
//!   pattern is served with the index versus without it
//! - regression bookkeeping: `rejected_with_index_avg_us` (the with-index avg at
//!   the moment we rejected the index) and `without_index_at_reject_us`/`_count`
//!   (a snapshot of the without-index bucket then, so recovery is measured on
//!   *fresh* without-index samples only)
//! - windowed scoring (`Weighted` mode): `demand_prev`, `failed_prev`,
//!   `idx_scan_prev`, `demand_rate`, `failed_rate`, `supply_rate`
//! - lifecycle: `status`, `created_at`, `evicted_at`, `create_attempts`,
//!   `last_create_error`
//! - supply (Postgres side): `last_idx_scan`, `last_seen_used`, `index_bytes`
//! - discrepancy: `discrepancy_state`, `discrepancy_ratio`, `discrepancy_at`
//! - explain (optional pass): `explain_state` — which physical tables the
//!   planner would use the index on (`none` / `accounts_table` /
//!   `snapshot_accounts_table` / `both`), `NULL` until the pass runs
//!
//! `status` is one of `candidate` / `created` / `evicted` / `rejected` (the last
//! set by the latency regression guard and, unlike `evicted`, not resurrected by
//! demand alone).
//!
//! The in-memory view of a row, with per-field docs, lives in the query-tracker
//! crate as `cloudbreak_query_tracker::modules::store::patterns::PatternRow`
//! (`crates/query-tracker/src/modules/store/patterns.rs`).

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const CREATE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS index_patterns (
    pattern_id        TEXT PRIMARY KEY,
    program           BYTEA NOT NULL,
    human_name        TEXT NOT NULL,
    offsets_lengths   JSONB NOT NULL DEFAULT '[]'::jsonb,
    datasize          BIGINT,
    -- one representative request (RpcProgramAccountsConfig JSON) for this
    -- identity, set on first sight and kept (COALESCE). Gives EXPLAIN probes
    -- real filter values and serves as a human-readable example on /debug.
    example_request   JSONB,

    demand_count      BIGINT NOT NULL DEFAULT 0,
    demand_at_create  BIGINT NOT NULL DEFAULT 0,
    total_cost_us     BIGINT NOT NULL DEFAULT 0,
    failed_count      BIGINT NOT NULL DEFAULT 0,
    variety_hll       BYTEA,
    variety_estimate  BIGINT NOT NULL DEFAULT 0,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_demand_at    TIMESTAMPTZ,

    -- latency split: cost accrued while the index existed vs while it did not,
    -- routed by `status` at record time. Lets us tell whether the index actually
    -- made this pattern faster. (with + without == total_cost_us / demand_count.)
    cost_with_index_us       BIGINT NOT NULL DEFAULT 0,
    cost_with_index_count    BIGINT NOT NULL DEFAULT 0,
    cost_without_index_us    BIGINT NOT NULL DEFAULT 0,
    cost_without_index_count BIGINT NOT NULL DEFAULT 0,

    -- regression guard: set when an index is dropped for being slower than no
    -- index. `rejected_with_index_avg_us` is the with-index average observed at
    -- that moment; the *_at_reject columns snapshot the without-index bucket so
    -- recovery is judged only on without-index samples gathered afterwards.
    rejected_with_index_avg_us    DOUBLE PRECISION,
    without_index_at_reject_us    BIGINT,
    without_index_at_reject_count BIGINT,

    -- windowed scoring (PriorityMode::Weighted only): every score-rate-window the
    -- roll task snapshots the cumulative counters into *_prev and stores the count
    -- observed during that window in *_rate (current - prev). *_rate is NULL until
    -- the first roll, which makes scoring fall back to the running total so a fresh
    -- pattern is still ranked within the first window (bootstrap).
    demand_prev       BIGINT NOT NULL DEFAULT 0,
    failed_prev       BIGINT NOT NULL DEFAULT 0,
    idx_scan_prev     BIGINT NOT NULL DEFAULT 0,
    demand_rate       BIGINT,
    failed_rate       BIGINT,
    supply_rate       BIGINT,

    status            TEXT NOT NULL DEFAULT 'candidate',
    created_at        TIMESTAMPTZ,
    evicted_at        TIMESTAMPTZ,
    create_attempts   INTEGER NOT NULL DEFAULT 0,
    last_create_error TEXT,

    last_idx_scan     BIGINT NOT NULL DEFAULT 0,
    last_seen_used    TIMESTAMPTZ,
    index_bytes       BIGINT NOT NULL DEFAULT 0,

    discrepancy_state TEXT,
    discrepancy_ratio DOUBLE PRECISION,
    discrepancy_at    TIMESTAMPTZ,

    -- latest verdict from the optional EXPLAIN sampling pass: which physical
    -- tables the planner would actually use this index on. NULL until the pass
    -- has run (or when explain-enabled is off). One of 'none' / 'accounts_table'
    -- / 'snapshot_accounts_table' / 'both'.
    explain_state     TEXT
);
CREATE INDEX IF NOT EXISTS idx_index_patterns_status ON index_patterns (status);
";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(CREATE_SQL)
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS index_patterns;")
            .await?;
        Ok(())
    }
}
