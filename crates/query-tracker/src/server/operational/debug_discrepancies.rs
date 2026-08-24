// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/discrepancies` — created indexes where demand and supply disagree.
//!
//! Read-only JSON view restricted to `created` patterns that carry a
//! discrepancy state (demand ≫ supply = `starved`, or the reverse =
//! `over_scanned`), for quickly spotting indexes the planner is ignoring or that
//! are no longer demanded. Rows use the same shape as `/debug/created`.
//!
//! The `discrepancy_ratio` is `compensated_idx_scan ÷ demand_since_create`
//! (`< 1` starved, `> 1` over-scanned).
//!
//! ## Query parameters
//!
//! - `order` — `discrepancy_ratio` (only key; default).
//! - `dir` — `asc` (default, most-starved first) | `desc`.
//! - `min_ratio`, `max_ratio` — keep only rows whose `discrepancy_ratio` is in
//!   the range (inclusive). Since the ratio is `< 1` for `starved` and `> 1` for
//!   `over_scanned`, a bound of exactly `1` cleanly splits the two: `max_ratio=1`
//!   shows **only starved** indexes, `min_ratio=1` shows **only over-scanned**
//!   ones.
//! - `limit` — max rows (default: all).
//! - `example`, `pattern_id`, `verbose` — include the heavier fields.
//!
//! Examples:
//! - `GET /debug/discrepancies`
//! - `GET /debug/discrepancies?max_ratio=1` (starved only)
//! - `GET /debug/discrepancies?min_ratio=1&dir=desc` (over-scanned only, worst first)
//! - `GET /debug/discrepancies?max_ratio=0.5&limit=10` (worst starvation only)

use super::{
    DebugQuery, Dir, bad_request, created_view, db_error, envelope, order_and_limit, score_field,
};
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json as jval};
use std::sync::Arc;

pub async fn handle(state: &Arc<AppState>, query: Option<&str>) -> Response<Full<Bytes>> {
    let q = match DebugQuery::parse(query) {
        Ok(q) => q,
        Err(e) => return bad_request(e),
    };
    if let Some(order) = q.order.as_deref()
        && order != "discrepancy_ratio"
    {
        return bad_request(format!(
            "invalid order '{order}' (discrepancies: discrepancy_ratio)"
        ));
    }

    let cfg = &state.config;
    let rows = match state
        .store
        .list_created_scored(cfg.priority_mode, cfg.without_index_compensation_factor)
        .await
    {
        Ok(rows) => rows,
        Err(e) => return db_error("discrepancies", e),
    };

    // Only discrepant rows (they always carry a ratio), then apply the optional
    // ratio bounds.
    let filtered: Vec<_> = rows
        .into_iter()
        .filter(|s| s.row.discrepancy_state.is_some())
        .filter(|s| {
            let ratio = s.row.discrepancy_ratio.unwrap_or(0.0);
            q.min_ratio.is_none_or(|min| ratio >= min) && q.max_ratio.is_none_or(|max| ratio <= max)
        })
        .collect();

    let dir = q.dir.unwrap_or(Dir::Asc);
    let (total, rows) = order_and_limit(
        filtered,
        |s| s.row.discrepancy_ratio.unwrap_or(0.0),
        dir,
        q.limit,
    );
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

    json(
        StatusCode::OK,
        &envelope("discrepancies", total, q.limit, items),
    )
}
