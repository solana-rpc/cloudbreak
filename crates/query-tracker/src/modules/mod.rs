// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Core pipeline of the query tracker:
//!
//! ```text
//! store                 the only SQL: index_patterns CRUD, candidates, supply,
//!                       discrepancy; plus prioritization (`PriorityMode` -> score)
//! ingest                demand in: a batch of observations -> store (handles the requests from `/track` endpoint)
//! indexer_backpressure  gate DDL(`CREATE`/`DROP INDEX`) on the [`crate indexer`]'s load
//! creation              demand -> `CREATE INDEX` pair -> mark created
//! eviction              refresh supply, flag discrepancies, `DROP INDEX` idle pairs past fill %
//! score_roll            (Weighted mode only) roll per-window activity counters
//! ```
//!
//! The observability/statistics side (variety, discrepancy verdict, EXPLAIN
//! sampling, Prometheus metrics) lives in the sibling [`crate::stats`] module.
//! The canonical [`IndexIdentity`](cloudbreak_core::modules::index_identity::IndexIdentity)
//! itself — parsing, deterministic `pattern_id`,
//! physical names, `CREATE INDEX` SQL — lives in `cloudbreak-core` so the API
//! client and the tracker agree on the exact same key; the tracker only adds the
//! detail that every auto-index is built as a **pair** (below).

pub mod creation;
pub mod eviction;
pub mod indexer_backpressure;
pub mod ingest;
pub mod score_roll;
pub mod store;

/// The two tables an auto-index is always created on (and dropped from)
/// together. Keeping them paired means the planner has the index available on
/// whichever table a GPA query targets.
pub const INDEX_TABLES: [&str; 2] = ["accounts", "snapshot_accounts"];

/// The table whose index count is used for the `max-auto-indexes` cap and the
/// eviction fill ratio (the snapshot table carries the full working set).
pub const CAP_TABLE: &str = "snapshot_accounts";
