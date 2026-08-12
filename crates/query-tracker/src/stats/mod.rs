// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Statistics & observability — how the tracker *understands* what it built.
//!
//! **Nothing in here changes what the tracker does.** These modules only
//! measure and explain; every create/drop decision is made by [`crate::modules`]
//! from demand, supply idle-time, and the index cap. The signals below are for
//! operators (logs, `/metrics`, `/debug/*`) — none of them is currently read
//! back as an input to creation or eviction. Concretely, per module:
//!
//! - `variety` — a per-index HyperLogLog estimate of *how many distinct* filter
//!   values one index serves. **Effect:** the estimate is stored
//!   (`index_patterns.variety_estimate`), exported as the `query_tracker_index_variety`
//!   gauge, shown in the debug endpoints, and logged when an index is created.
//!   It does **not** feed prioritization or eviction — `score_expr` ignores it —
//!   so today it is purely informational.
//!
//! - `discrepancy` — the demand-vs-supply verdict (`Ok`/`Starved`/`OverScanned`),
//!   evaluated once per eviction pass. **Effect:** persisted onto the row
//!   (`discrepancy_state`/`discrepancy_ratio`), counted by the
//!   `query_tracker_discrepant_indexes` gauge, surfaced on `/debug/discrepancies`
//!   and `/debug/created`, and logged (warning) when `Starved`. It does **not**
//!   protect a starved index from being dropped — that protection comes from the
//!   eviction candidate query requiring demand *and* scans to be idle, so a
//!   still-demanded index never becomes an eviction candidate in the first place.
//!   The verdict is diagnostic only.
//!
//! - `explain` — opt-in periodic `EXPLAIN` (plan-only, never `ANALYZE`) on a
//!   synthetic probe query, a third planner-level signal on top of demand (API)
//!   and supply (`idx_scan`). Off unless `explain-enabled`. **Effect:** logs a
//!   warning when the planner would not use an index for its own probe query.
//!   It writes nothing to the DB and updates no metrics — log-only.
//!
//! - `metrics` — the Prometheus registry plus the gauges/counters served on
//!   `/metrics`. **Effect:** the exposition surface itself; the other modules and
//!   the pipeline loops push values into it.

pub mod discrepancy;
pub mod explain;
pub mod metrics;
pub mod variety;
