// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Prioritization — the *single* criterion shared by creation and eviction.
//!
//! There is no separate priority queue. Patterns live in `index_patterns` and
//! are ranked on read by translating the configured [`PriorityMode`] into a SQL
//! score *expression* ([`score_expr`]). Creation sorts that expression
//! **descending** (build the highest first); eviction sorts the *same*
//! expression **ascending** (drop the lowest first). So "what we most want to
//! build" and "what we least mind dropping" are, by construction, two ends of
//! one ordering.
//!
//! Most modes rank on lifetime totals. [`PriorityMode::Weighted`] instead ranks
//! on **windowed** activity (the per-window counts maintained by the score roll
//! task, see `store::Store::roll_scores`), so decisions track *current*
//! throughput rather than all-time popularity. Until a pattern has been rolled
//! once its `*_rate` columns are `NULL`; the expression then falls back to the
//! running total so a fresh pattern is still ranked within its first window.
//!
//! `Weighted` also folds in the measured **latency gain** — how much faster the
//! pattern is served *with* the index than without (see [`gain_expr`]) — as a
//! multiplier on `avg_cost`. Unlike the windowed counts this uses lifetime
//! averages (stable ratios), and stays neutral until the pattern has served
//! requests both with and without the index.

use cloudbreak_core::PriorityMode;

/// Index scans Postgres records per served request. A
/// `getProgramAccounts` is a `UNION ALL` over the `accounts` and
/// `snapshot_accounts` tables, so a single request scans **both** indexes of the
/// pair → ~2 `idx_scan` increments per request. Supply is therefore divided by
/// this before being compared to demand (here and in `stats::discrepancy`).
pub const SCANS_PER_REQUEST: i64 = 2;

/// SQL ranking expression for `mode` (higher = higher priority). Creation
/// appends `DESC`, eviction appends `ASC`; the expression itself is direction-
/// agnostic so both stay in lockstep. Weights in [`PriorityMode::Weighted`] are
/// numeric and embedded directly; they never come from untrusted input.
///
/// `compensation` is the `without-index-compensation-factor`, inflating the
/// without-index cost inside the latency [`gain_expr`] (see there); it is inert
/// for every mode except `Weighted` with a non-zero `latency_weight`.
pub fn score_expr(mode: PriorityMode, compensation: f64) -> String {
    match mode {
        PriorityMode::Frequency => "demand_count".to_string(),
        PriorityMode::Cost => "total_cost_us".to_string(),
        PriorityMode::CostPerHit => avg_cost_expr(),
        PriorityMode::Weighted { .. } => {
            let (avg, gain, counts) = score_component_exprs(mode, compensation)
                .expect("Weighted decomposes into its three factors");
            format!("({avg}) * ({gain}) * ({counts})")
        }
    }
}

/// The three multiplicative factors of the [`PriorityMode::Weighted`] score, as
/// SQL expressions: `(avg_cost_us, gain_multiplier, counts_multiplier)`. Their
/// product is exactly [`score_expr`] for `Weighted`, which is defined in terms of
/// this function so the two cannot diverge. Returns `None` for the
/// single-quantity modes (Frequency/Cost/CostPerHit), which do not decompose.
///
/// - **avg** — average cost per request in µs ([`avg_cost_expr`]).
/// - **gain** — `1 + latency_weight · ln(gain_ratio)`, the latency multiplier
///   (neutral `1` until both with/without-index samples exist; see [`gain_expr`]).
/// - **counts** — `1 + demand_weight·demand + supply_weight·(supply/2) + failure_weight·failed`,
///   the windowed-volume multiplier. The `+ 1` baseline keeps it non-zero so a
///   zero-activity pattern still ranks by `avg · gain` alone (and idle eviction
///   candidates never all tie at zero).
pub fn score_component_exprs(
    mode: PriorityMode,
    compensation: f64,
) -> Option<(String, String, String)> {
    let PriorityMode::Weighted {
        demand_weight,
        supply_weight,
        failure_weight,
        latency_weight,
        ..
    } = mode
    else {
        return None;
    };
    // Per-window quantities, bootstrapped to the running total until the first
    // roll materializes a real window delta.
    let demand = "COALESCE(demand_rate, demand_count)";
    let supply = "COALESCE(supply_rate, last_idx_scan)";
    let failed = "COALESCE(failed_rate, failed_count)";
    let spr = SCANS_PER_REQUEST as f64;
    let avg = avg_cost_expr();
    let gain = format!("(1 + {latency_weight} * LN({}))", gain_expr(compensation));
    let counts = format!(
        "(1 + {demand_weight} * {demand} + {supply_weight} * ({supply}::float8 / {spr}) \
         + {failure_weight} * {failed})",
    );
    Some((avg, gain, counts))
}

/// Average cost per request in microseconds; a ratio, so it is identical whether
/// measured over a window or over all time.
fn avg_cost_expr() -> String {
    "total_cost_us::float8 / GREATEST(demand_count, 1)".to_string()
}

/// Ratio of the (compensated) without-index average cost to the with-index
/// average — how many times faster the pattern is served with the index (`> 1`
/// helps, `< 1` hurts). Returns `1` (neutral) until the pattern has served
/// requests both with and without the index, so a ratio can actually be formed.
/// Averages are floored at 1µs so the ratio (and the `LN` taken of it in
/// [`score_expr`]) stays finite regardless of how cheap a side became.
///
/// `compensation` (`without-index-compensation-factor`) multiplies the
/// without-index average before the ratio is formed, so a without-index scan
/// whose wall-clock time is deflated by parallel workers is credited with the
/// cost it really incurred. `1.0` leaves the raw averages untouched.
pub fn gain_expr(compensation: f64) -> String {
    format!(
        "CASE WHEN cost_with_index_count > 0 AND cost_without_index_count > 0 \
              THEN GREATEST({without}, 1) \
                 / GREATEST(cost_with_index_us::float8 / GREATEST(cost_with_index_count, 1), 1) \
              ELSE 1 END",
        without = compensated_without_avg_expr(compensation),
    )
}

/// SQL for the without-index average cost per request, scaled by
/// `without-index-compensation-factor`. Shared by the latency [`gain_expr`] and
/// the regression guard so both judge the with/without comparison identically.
pub fn compensated_without_avg_expr(compensation: f64) -> String {
    format!(
        "(cost_without_index_us::float8 / GREATEST(cost_without_index_count, 1)) * {compensation}"
    )
}
