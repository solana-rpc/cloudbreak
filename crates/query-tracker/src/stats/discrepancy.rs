// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Discrepancy — where demand (API) and supply (Postgres) disagree.
//!
//! Demand is what the API asked for; supply is what the planner actually did
//! (`idx_scan`). A served `getProgramAccounts` is a `UNION ALL` over `accounts`
//! and `snapshot_accounts`, so it scans **both** indexes of the pair — raw
//! `idx_scan` grows ~2x the request count. The caller normalizes supply by
//! `SCANS_PER_REQUEST` before calling [`evaluate`], so once an index exists the
//! normalized supply should track request growth ~1:1. When it does not, the two
//! signals disagree and that disagreement is itself the most useful output:
//!
//! - **Starved** (supply ≪ demand): Postgres is *ignoring* an index the workload
//!   still wants (stale stats, predicate mismatch, cost misconfig). Do **not**
//!   evict — surface it for investigation instead.
//! - **OverScanned** (supply ≫ demand): more scans than requests (background
//!   jobs, ANALYZE, multi-partition scans). Informational.
//!
//! Both are measured *since creation* (`supply` vs `demand - demand_at_create`)
//! and only when demand is significant, so tiny counts do not create noise. The
//! band width is `discrepancy-delta` (default 10%).

/// Verdict for one created pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscrepancyState {
    Ok,
    Starved,
    OverScanned,
}

impl DiscrepancyState {
    /// Text stored in `index_patterns.discrepancy_state` (`None` for `Ok`).
    pub fn as_stored(self) -> Option<&'static str> {
        match self {
            DiscrepancyState::Ok => None,
            DiscrepancyState::Starved => Some("starved"),
            DiscrepancyState::OverScanned => Some("over_scanned"),
        }
    }
}

/// Evaluate demand-vs-supply for a created pattern. `supply` must already be
/// normalized by `SCANS_PER_REQUEST` so it is comparable to `demand_since_create`.
///
/// Returns the verdict and the `supply / demand_since_create` ratio (`None`
/// ratio when there is not enough demand to judge, in which case the state is
/// `Ok`).
pub fn evaluate(
    demand_since_create: i64,
    supply: i64,
    delta: f64,
    min_demand: i64,
) -> (DiscrepancyState, Option<f64>) {
    if demand_since_create < min_demand.max(1) {
        return (DiscrepancyState::Ok, None);
    }

    let ratio = supply as f64 / demand_since_create as f64;
    let state = if ratio < 1.0 - delta {
        DiscrepancyState::Starved
    } else if ratio > 1.0 + delta {
        DiscrepancyState::OverScanned
    } else {
        DiscrepancyState::Ok
    };
    (state, Some(ratio))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_when_supply_tracks_demand() {
        let (s, r) = evaluate(1000, 980, 0.10, 100);
        assert_eq!(s, DiscrepancyState::Ok);
        assert!(r.unwrap() > 0.9);
    }

    #[test]
    fn starved_when_pg_ignores_index() {
        let (s, _) = evaluate(1000, 100, 0.10, 100);
        assert_eq!(s, DiscrepancyState::Starved);
    }

    #[test]
    fn over_scanned_when_more_scans_than_requests() {
        let (s, _) = evaluate(1000, 5000, 0.10, 100);
        assert_eq!(s, DiscrepancyState::OverScanned);
    }

    #[test]
    fn ignores_low_demand() {
        let (s, r) = evaluate(5, 0, 0.10, 100);
        assert_eq!(s, DiscrepancyState::Ok);
        assert!(r.is_none());
    }
}
