// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{collections::BTreeSet, convert::Infallible, sync::OnceLock};

use http_body_util::Full;
use hyper::{Response, body::Bytes};
use serde::Serialize;

use super::params::{DebugDetail, DebugParams};
use super::{json_error, json_ok};
use crate::db_queries::{self, ChainTips};
use crate::modules::self_healing::SelfHealingState;

/// Set once during startup so the debug endpoint can inspect the live self-healing state.
pub static SELF_HEALING: OnceLock<SelfHealingState> = OnceLock::new();

/// HTTP debug endpoint for inspecting the self-healing gap state
/// (`crate::modules::self_healing::SelfHealingState`).
///
/// Always read-only. Groups the flat list of still-missing slots into contiguous gaps, each
/// anchored on its boundary (`gap_start - 1`) and annotated with how far behind the confirmed
/// tip it is.
///
/// # Route
///
/// `GET /debug/modules/self_healing`
///
/// # Query parameters
///
/// All parameters are optional.
///
/// | Name       | Type | Default   | Description |
/// |------------|------|-----------|-------------|
/// | `detail`   | enum | `summary` | `summary` (chain tips + `stats` counts only, no gap list), `slots` (adds the grouped `gaps`), or `full` (additionally dumps the raw `missing_slots`). |
/// | `min_slot` | u64  | unset     | Restricts the missing-slot view (gaps, counts, listings) to slots `>= min_slot`. |
/// | `max_slot` | u64  | unset     | Restricts the missing-slot view (gaps, counts, listings) to slots `<= max_slot`. |
/// | `limit`    | usize| unset     | Caps the number of returned `gaps` / `missing_slots` entries (stats counts are unaffected). |
///
/// ### `detail` values
///
/// - `summary` — chain tips (`db`), `last_slot_received` and `stats` only. Use this when the gap
///   list could be large; it returns the counts (`gap_count`, `missing_slots_total`,
///   `gap_boundaries_count`) without enumerating anything.
/// - `slots` — adds the grouped `gaps` list.
/// - `full` — same as `slots`, plus the raw flat `missing_slots` list.
///
/// ### Gap fields
///
/// - `boundary_slot` — `gap_start - 1` when recorded in `gap_boundaries`, else `null`.
/// - `start` / `end` — first/last still-missing slot of the run.
/// - `len` — number of slots still pending in the run (not necessarily the original gap size).
/// - `slots_behind_confirmed` — `db.confirmed_slot - start` (distance from the oldest edge of the
///   gap to the confirmed tip).
///
/// # Response
///
/// `Content-Type: application/json`. `200 OK` on success, `400 Bad Request` on an invalid
/// parameter, `503 Service Unavailable` if self-healing is not yet initialized.
///
/// ```json
/// {
///   "db": { "confirmed_slot": 300100200, "finalized_slot": 300100168, "finalized_behind_confirmed": 32 },
///   "last_slot_received": 300100200,
///   "stats": { "gap_count": 1, "missing_slots_total": 40, "gap_boundaries_count": 1 },
///   "gaps": [
///     { "boundary_slot": 300100059, "start": 300100060, "end": 300100099, "len": 40, "slots_behind_confirmed": 140 }
///   ]
/// }
/// ```
///
/// # Examples
///
/// ```text
/// curl http://localhost:8875/debug/modules/self_healing
/// curl 'http://localhost:8875/debug/modules/self_healing?detail=slots'
/// curl 'http://localhost:8875/debug/modules/self_healing?detail=full&min_slot=300100060&max_slot=300100099'
/// ```
pub(crate) async fn handle(query: Option<&str>) -> Result<Response<Full<Bytes>>, Infallible> {
    let params = match DebugParams::from_query(query) {
        Ok(p) => p,
        Err(msg) => return Ok(json_error(400, &msg)),
    };

    let Some(self_healing) = SELF_HEALING.get() else {
        return Ok(json_error(503, "self-healing not initialized"));
    };

    let tips = db_queries::get_chain_tips(&self_healing.finalizer.db).await;

    // Copy the state out from under the locks, then shape it here.
    let last_slot_received = *self_healing
        .last_slot_received
        .lock()
        .expect("Failed to lock last_slot_received");
    let mut missing = self_healing
        .gaps_list
        .lock()
        .expect("Failed to lock gaps_list")
        .clone();
    let boundaries = self_healing
        .gap_boundaries
        .lock()
        .expect("Failed to lock gap_boundaries")
        .clone();

    missing.sort_unstable();
    missing.dedup();
    missing.retain(|slot| params.in_range(*slot));
    let missing_slots_total = missing.len();

    // Group the (range-filtered) missing slots into contiguous runs. `gap_count` reflects every
    // run; the listed `gaps` may be capped by `limit`.
    let all_gaps = group_gaps(&missing, &boundaries, tips.confirmed_slot);
    let stats = SelfHealingStats {
        gap_count: all_gaps.len(),
        missing_slots_total,
        gap_boundaries_count: boundaries.len(),
    };

    let gaps = (params.detail >= DebugDetail::Slots).then(|| {
        let mut gaps = all_gaps;
        if let Some(limit) = params.limit {
            gaps.truncate(limit);
        }
        gaps
    });

    let missing_slots = (params.detail >= DebugDetail::Full).then(|| {
        if let Some(limit) = params.limit {
            missing.truncate(limit);
        }
        missing
    });

    let state = SelfHealingDebug {
        last_slot_received,
        stats,
        gaps,
        missing_slots,
    };

    json_ok(&SelfHealingResponse { db: tips, state })
}

/// Groups an ascending, de-duplicated list of missing slots into contiguous runs.
fn group_gaps(
    missing: &[u64],
    boundaries: &BTreeSet<u64>,
    confirmed: Option<u64>,
) -> Vec<GapDebug> {
    let mut gaps: Vec<GapDebug> = Vec::new();
    let mut iter = missing.iter().copied();
    if let Some(first) = iter.next() {
        let mut start = first;
        let mut end = first;
        for slot in iter {
            if slot == end + 1 {
                end = slot;
            } else {
                gaps.push(GapDebug::new(start, end, boundaries, confirmed));
                start = slot;
                end = slot;
            }
        }
        gaps.push(GapDebug::new(start, end, boundaries, confirmed));
    }
    gaps
}

#[derive(Serialize)]
struct SelfHealingResponse {
    db: ChainTips,
    #[serde(flatten)]
    state: SelfHealingDebug,
}

/// Owned snapshot of the self-healing state for the debug endpoint.
#[derive(Serialize)]
struct SelfHealingDebug {
    /// Latest live slot seen on the gRPC stream (the in-memory confirmed frontier).
    last_slot_received: u64,
    stats: SelfHealingStats,
    /// Contiguous runs of still-missing slots, ascending. Present only when `detail >= slots`.
    #[serde(skip_serializing_if = "Option::is_none")]
    gaps: Option<Vec<GapDebug>>,
    /// Raw flat list of every still-missing slot. Present only when `detail >= full`.
    #[serde(skip_serializing_if = "Option::is_none")]
    missing_slots: Option<Vec<u64>>,
}

#[derive(Serialize)]
struct SelfHealingStats {
    gap_count: usize,
    missing_slots_total: usize,
    gap_boundaries_count: usize,
}

#[derive(Serialize)]
struct GapDebug {
    /// `gap_start - 1` when the boundary is recorded in `gap_boundaries`, else `None`.
    boundary_slot: Option<u64>,
    start: u64,
    end: u64,
    /// Number of slots still pending in this run (not necessarily the original gap size).
    len: usize,
    /// `confirmed_slot - start` (how far the oldest edge of the gap lags confirmed).
    slots_behind_confirmed: Option<u64>,
}

impl GapDebug {
    fn new(start: u64, end: u64, boundaries: &BTreeSet<u64>, confirmed: Option<u64>) -> Self {
        let boundary_slot = start.checked_sub(1).filter(|b| boundaries.contains(b));
        Self {
            boundary_slot,
            start,
            end,
            len: (end - start + 1) as usize,
            slots_behind_confirmed: confirmed.map(|c| c.saturating_sub(start)),
        }
    }
}
