// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/candidates` — the ranked creation queue.
//!
//! Read-only JSON view of the candidates the creation loop would build, each
//! annotated with its priority score under the active
//! [`PriorityMode`](cloudbreak_core::PriorityMode), so operators can see *why*
//! an index will (or will not) be built without querying Postgres by hand.
//!
//! Alongside the `score` and its raw inputs (`demand_count`, `failed_count`,
//! `variety_estimate`) it surfaces `avg_cost_ms` (total cost ÷ demand) and
//! `avg_cost_without_index_ms` plus the measured `with_without_idx_ratio`
//! (without-index ÷ with-index) — the otherwise-opaque inputs to the score's
//! `gain` term. Usually a candidate has never had an index, so `avg_cost_ms`
//! and `avg_cost_without_index_ms` coincide and `with_without_idx_ratio` is
//! `null`; for a
//! pattern that was previously created (then evicted/recovered) they diverge and
//! carry real signal.
//!
//! ## Query parameters
//!
//! - `order` — `score` (default), `demand_count`, `avg_cost_ms`, `variety_estimate`.
//! - `dir` — `asc` | `desc` (default `desc`).
//! - `limit` — max rows (default: all).
//! - `example`, `pattern_id`, `verbose` — include the heavier fields.
//!
//! Examples:
//! - `GET /debug/candidates`
//! - `GET /debug/candidates?order=demand_count&limit=20`
//! - `GET /debug/candidates?order=avg_cost_ms&dir=desc&example=true&pattern_id=true`

use super::{
    DebugQuery, Dir, avg_ms, bad_request, db_error, docs, order_and_limit, score_field,
    with_without_idx_ratio,
};
use crate::modules::store::ScoredPattern;
use crate::modules::store::patterns::PatternRow;
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json as jval};
use std::sync::Arc;

/// Upper bound on candidates pulled for a debug listing before ordering/limit.
/// The queue is demand-gated so this is generous; `total` is capped by it.
const CANDIDATE_FETCH_CAP: u64 = 10_000;

pub async fn handle(state: &Arc<AppState>, query: Option<&str>) -> Response<Full<Bytes>> {
    let q = match DebugQuery::parse(query) {
        Ok(q) => q,
        Err(e) => return bad_request(e),
    };
    let cfg = &state.config;

    let scored = match state
        .store
        .top_candidates_scored(
            cfg.priority_mode,
            cfg.without_index_compensation_factor,
            cfg.index_generation_threshold,
            cfg.cost_eligibility_threshold_us,
            CANDIDATE_FETCH_CAP,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return db_error("candidates", e),
    };

    let order = q.order.as_deref().unwrap_or("score");
    let key: fn(&ScoredPattern) -> f64 = match order {
        "score" => |x| x.score,
        "demand_count" => |x| x.row.demand_count as f64,
        "avg_cost_ms" => |x| avg_ms(x.row.total_cost_us, x.row.demand_count).unwrap_or(0.0),
        "variety_estimate" => |x| x.row.variety_estimate as f64,
        other => {
            return bad_request(format!(
                "invalid order '{other}' (candidates: score, demand_count, avg_cost_ms, variety_estimate)"
            ));
        }
    };
    let dir = q.dir.unwrap_or(Dir::Desc);

    let (total, rows) = order_and_limit(scored, key, dir, q.limit);
    let items: Vec<Value> = rows
        .iter()
        .map(|s| candidate_view(&s.row, s.score, s.components, &q))
        .collect();

    json(
        StatusCode::OK,
        &jval!({
            "priority_mode": format!("{:?}", cfg.priority_mode),
            "total": total,
            "count": items.len(),
            "limit": q.limit,
            "docs": docs(),
            "candidates": items,
        }),
    )
}

fn candidate_view(
    r: &PatternRow,
    score: f64,
    components: Option<(f64, f64, f64)>,
    q: &DebugQuery,
) -> Value {
    let mut obj = jval!({
        "index": r.human_name,
        "score": score_field(score, components),
        "demand_count": r.demand_count,
        "failed_count": r.failed_count,
        "variety_estimate": r.variety_estimate,
        "avg_cost_ms": avg_ms(r.total_cost_us, r.demand_count),
        "avg_cost_without_index_ms": avg_ms(r.cost_without_index_us, r.cost_without_index_count),
        "with_without_idx_ratio": with_without_idx_ratio(r),
    });
    let map = obj.as_object_mut().expect("jval! built an object");
    if q.show_pattern_id {
        map.insert("pattern_id".into(), jval!(r.pattern_id));
    }
    if q.show_example {
        map.insert("example_request".into(), r.example_request.clone().into());
    }
    obj
}
