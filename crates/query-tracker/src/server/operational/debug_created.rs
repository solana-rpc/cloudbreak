// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/created` — currently active auto-created indexes.
//!
//! Read-only JSON view of every `created` pattern with its demand (API) vs.
//! supply (`compensated_idx_scan`) figures, on-disk size (`index_mb`), latency
//! with/without the index and their `with_without_idx_ratio`, the latest `explain_state`
//! verdict, a human-readable `created_at`, and its `score` — the same
//! [`PriorityMode`](cloudbreak_core::PriorityMode) ranking value the
//! `/debug/candidates` queue shows (creation builds highest, eviction drops
//! lowest) — so operators can see what is built and how it is being used.
//!
//! ## Query parameters
//!
//! - `order` — `created_at` (default, newest first), `index_mb`, `demand_count`,
//!   `idx_scan` (compensated), `avg_cost_with_index_ms`, `with_without_idx_ratio`,
//!   `variety_estimate`, `score` (the shared `PriorityMode` ranking, present on
//!   every row; under `filter=eviction_candidates` it is the default and sorts
//!   ascending = least useful first, otherwise descending = most valuable first).
//! - `dir` — `asc` | `desc` (default `desc`, except `score` defaults to `asc`).
//! - `min_ratio`, `max_ratio` — keep only rows whose `with_without_idx_ratio`
//!   (without-index ÷ with-index latency) is in the range (inclusive). Rows
//!   without both latency buckets have no ratio and are dropped when either bound
//!   is set. Since `> 1` means the index helps, `max_ratio=1` surfaces indexes
//!   that are **not** helping (candidates for the regression guard).
//! - `limit` — max rows (default: all).
//! - `example`, `pattern_id`, `verbose` — include the heavier fields.
//!
//! Examples:
//! - `GET /debug/created`
//! - `GET /debug/created?order=index_mb&limit=10`
//! - `GET /debug/created?order=variety_estimate&dir=desc`
//! - `GET /debug/created?max_ratio=1&order=with_without_idx_ratio` (index not helping)
//!
//! ## Eviction candidates (`filter=eviction_candidates`)
//!
//! Restricts the view to the created indexes the eviction pass would
//! *consider*: those past the idle + age-grace gates (`index-min-idle` /
//! `index-min-age-grace`), ordered least-useful-first by default (`order=score`,
//! ascending — the same `score` shown on every row, just sorted the drop way).
//!
//! This is the **eligible queue, not a guarantee**: the actual drop additionally
//! depends on the table being above the fill target (`eviction-fill-threshold`)
//! at runtime — eviction only trims the buffer band back down to the target — so
//! an index listed here may still be kept. Omit the filter for the full created
//! set.
//!
//! Examples:
//! - `GET /debug/created?filter=eviction_candidates` (the eviction queue)
//! - `GET /debug/created?filter=eviction_candidates&order=index_mb&verbose=true`
//!
//! ## EXPLAIN-verdict filters (`filter=explain_*`)
//!
//! Narrow the full created set by the latest `explain_state` verdict — the
//! detailed, per-index source of truth behind the `query_tracker_explain_state`
//! gauge and the EXPLAIN pass's summary log. Rows with no verdict yet
//! (`explain_state = null`, e.g. `explain-enabled` off) are excluded.
//!
//! - `filter=explain_none` — planner would use the index on **neither** table.
//! - `filter=explain_partial` — used on exactly **one** table (`accounts_table`
//!   xor `snapshot_accounts_table`).
//! - `filter=explain_incomplete` — `none` **or** partial: everything not fully
//!   used on both tables (the go-to detailed view).
//!
//! Examples:
//! - `GET /debug/created?filter=explain_incomplete&verbose=true`
//! - `GET /debug/created?filter=explain_none&order=idx_scan` (unused yet scanned first)

use super::{
    DebugQuery, Dir, avg_ms, bad_request, bytes_to_mib, compensated_idx_scan, created_view,
    db_error, envelope, order_and_limit, score_field, with_without_idx_ratio,
};
use crate::modules::store::ScoredPattern;
use crate::modules::store::patterns::explain_state;
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json as jval};
use std::sync::Arc;

/// A created row plus its [`PriorityMode`](cloudbreak_core::PriorityMode) score
/// (and, for `Weighted`, the score's component factors) — the single shared
/// ranking value (creation builds highest, eviction drops lowest). Always present
/// now, so `order=score` works for every filter.
type Row = ScoredPattern;

/// The `?filter=` row-subset selector for `/debug/created`.
enum CreatedFilter {
    /// Every `created` pattern (no filter).
    All,
    /// Only eviction-eligible rows (still scored by the shared `score`).
    Eviction,
    /// EXPLAIN verdict `none` — planner would use the index on neither table.
    ExplainNone,
    /// EXPLAIN verdict on exactly one table (`accounts_table` xor
    /// `snapshot_accounts_table`).
    ExplainPartial,
    /// `none` **or** partial — every index not fully used on both tables (rows
    /// with no verdict yet, i.e. `explain_state = null`, are excluded).
    ExplainIncomplete,
}

impl CreatedFilter {
    /// Drop rows that don't match an EXPLAIN-verdict filter, in place. A no-op
    /// for [`All`](Self::All)/[`Eviction`](Self::Eviction), whose row set is
    /// already decided by the query above.
    fn retain_explain(&self, rows: &mut Vec<Row>) {
        let keep = match self {
            CreatedFilter::ExplainNone => |s: Option<&str>| s == Some(explain_state::NONE),
            CreatedFilter::ExplainPartial => |s: Option<&str>| {
                matches!(s, Some(explain_state::ACCOUNTS | explain_state::SNAPSHOT))
            },
            CreatedFilter::ExplainIncomplete => |s: Option<&str>| {
                matches!(
                    s,
                    Some(explain_state::NONE | explain_state::ACCOUNTS | explain_state::SNAPSHOT)
                )
            },
            CreatedFilter::All | CreatedFilter::Eviction => return,
        };
        rows.retain(|s| keep(s.row.explain_state.as_deref()));
    }
}

pub async fn handle(state: &Arc<AppState>, query: Option<&str>) -> Response<Full<Bytes>> {
    let q = match DebugQuery::parse(query) {
        Ok(q) => q,
        Err(e) => return bad_request(e),
    };

    let filter = match q.filter.as_deref() {
        None => CreatedFilter::All,
        Some("eviction_candidates") => CreatedFilter::Eviction,
        Some("explain_none") => CreatedFilter::ExplainNone,
        Some("explain_partial") => CreatedFilter::ExplainPartial,
        Some("explain_incomplete") => CreatedFilter::ExplainIncomplete,
        Some(other) => {
            return bad_request(format!(
                "invalid filter '{other}' (created: eviction_candidates, explain_none, \
                 explain_partial, explain_incomplete)"
            ));
        }
    };
    let eviction = matches!(filter, CreatedFilter::Eviction);

    let cfg = &state.config;
    let mut rows: Vec<Row> = if eviction {
        match state
            .store
            .eviction_candidates(
                cfg.priority_mode,
                cfg.without_index_compensation_factor,
                cfg.index_min_idle.as_secs() as i64,
                cfg.index_min_age_grace.as_secs() as i64,
                cfg.use_supply_for_eviction,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return db_error("created", e),
        }
    } else {
        match state
            .store
            .list_created_scored(cfg.priority_mode, cfg.without_index_compensation_factor)
            .await
        {
            Ok(v) => v,
            Err(e) => return db_error("created", e),
        }
    };

    // EXPLAIN-verdict filters narrow the full created set to the indexes whose
    // last probe found them not fully used — the detailed per-index source of
    // truth behind the `query_tracker_explain_state` gauge / summary log.
    filter.retain_explain(&mut rows);

    // Optional bounds on the with/without-index latency ratio. A row without both
    // latency buckets has no ratio, so any set bound excludes it.
    if q.min_ratio.is_some() || q.max_ratio.is_some() {
        rows.retain(|s| match with_without_idx_ratio(&s.row) {
            Some(ratio) => {
                q.min_ratio.is_none_or(|min| ratio >= min)
                    && q.max_ratio.is_none_or(|max| ratio <= max)
            }
            None => false,
        });
    }

    let order = q
        .order
        .as_deref()
        .unwrap_or(if eviction { "score" } else { "created_at" });
    let key: fn(&Row) -> f64 = match order {
        "created_at" => |x| x.row.created_at_epoch.unwrap_or(0.0),
        "index_mb" => |x| bytes_to_mib(x.row.index_bytes),
        "demand_count" => |x| x.row.demand_count as f64,
        "idx_scan" => |x| compensated_idx_scan(x.row.last_idx_scan) as f64,
        "avg_cost_with_index_ms" => {
            |x| avg_ms(x.row.cost_with_index_us, x.row.cost_with_index_count).unwrap_or(0.0)
        }
        "with_without_idx_ratio" => |x| with_without_idx_ratio(&x.row).unwrap_or(0.0),
        "variety_estimate" => |x| x.row.variety_estimate as f64,
        "score" => |x| x.score,
        other => {
            return bad_request(format!(
                "invalid order '{other}' (created: created_at, index_mb, demand_count, idx_scan, \
                 avg_cost_with_index_ms, with_without_idx_ratio, variety_estimate, score)"
            ));
        }
    };
    // Under the eviction filter, `score` defaults ascending (least useful first,
    // matching the drop order); everywhere else the default is descending.
    let dir = q.dir.unwrap_or(if order == "score" && eviction {
        Dir::Asc
    } else {
        Dir::Desc
    });

    let (total, rows) = order_and_limit(rows, key, dir, q.limit);
    let items: Vec<Value> = rows
        .iter()
        .map(|s| {
            let mut view = created_view(&s.row, &q);
            view.as_object_mut()
                .expect("created_view built an object")
                .insert("score".into(), jval!(score_field(s.score, s.components)));
            view
        })
        .collect();

    json(StatusCode::OK, &envelope("created", total, q.limit, items))
}
