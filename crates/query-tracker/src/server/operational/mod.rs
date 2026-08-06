// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Operational endpoints — introspection and ops, one file per endpoint
//! (filename = endpoint path). Served on the same port as the functional
//! endpoints (see `server::serve`).
//!
//! - `debug_candidates`    — `GET /debug/candidates`    (ranked creation queue).
//! - `debug_created`       — `GET /debug/created`       (active indexes).
//! - `debug_discrepancies` — `GET /debug/discrepancies` (demand/supply divergence).
//! - `metrics`             — `GET /metrics`             (Prometheus text).
//! - `health`             — `GET /health`             (liveness probe).
//!
//! ## Shared debug conventions
//!
//! The three `debug_*` endpoints share query-string handling, number formatting
//! and the response envelope, all defined here:
//!
//! - Latencies are rendered in **milliseconds, 2 decimals** (`*_ms`); index size
//!   in **MiB, 2 decimals** (`index_mb`). Raw `idx_scan` is never shown — only
//!   `compensated_idx_scan` (÷ [`SCANS_PER_REQUEST`], since a served GPA scans
//!   both tables of the pair), so supply lines up ~1:1 with demand.
//! - `pattern_id` and `example_request` are **omitted unless explicitly
//!   requested** via `?pattern_id=true` / `?example=true` (or `?verbose=true`
//!   for both), keeping the default view compact.
//! - `score` (the shared `priority-mode` ranking) is rendered in **thousands,
//!   1 decimal** (e.g. `"37.2K"`): the raw score derives from microsecond costs,
//!   so ÷ 1000 makes it proportional to the `*_ms` figures. The `*_ms` averages
//!   and `with_without_idx_ratio` are the *raw* measured values (the compensation
//!   factor is not applied to them). Each response spells this out in a top-level
//!   `docs` object ([`docs`]).
//! - Every response is `{ "total": <matching rows>, "count": <rows returned>,
//!   "limit": <n|null>, "docs": <reading notes>, "<items>": [...] }`. `?limit=N`
//!   caps the returned rows (default: all); `?order=<key>` and `?dir=asc|desc`
//!   sort them.
//! - Unknown query keys or malformed values are rejected with `400`.

pub mod debug_candidates;
pub mod debug_created;
pub mod debug_discrepancies;
pub mod health;
pub mod metrics;

use crate::modules::store::patterns::PatternRow;
use crate::modules::store::prioritization::SCANS_PER_REQUEST;
use crate::server::text;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json as jval};
use tracing::error;

/// Sort direction for the `?dir=` query parameter.
#[derive(Debug, Clone, Copy)]
pub enum Dir {
    Asc,
    Desc,
}

/// Parsed common query parameters shared by the debug endpoints. Each endpoint
/// validates `order` against its own whitelist; the rest are generic.
#[derive(Debug, Default)]
pub struct DebugQuery {
    /// Max rows to return (`None` = all).
    pub limit: Option<usize>,
    /// Sort key (endpoint-specific whitelist); `None` = endpoint default.
    pub order: Option<String>,
    /// Sort direction; `None` = endpoint default.
    pub dir: Option<Dir>,
    /// Include `example_request` in each row.
    pub show_example: bool,
    /// Include `pattern_id` in each row.
    pub show_pattern_id: bool,
    /// Lower bound on the endpoint's ratio: `discrepancy_ratio` on
    /// `/discrepancies`, `with_without_idx_ratio` on `/created`.
    pub min_ratio: Option<f64>,
    /// Upper bound on the endpoint's ratio: `discrepancy_ratio` on
    /// `/discrepancies`, `with_without_idx_ratio` on `/created`.
    pub max_ratio: Option<f64>,
    /// Row subset selector (endpoint-specific; e.g. `/created` accepts
    /// `eviction_candidates`). `None` = the endpoint's full set.
    pub filter: Option<String>,
}

impl DebugQuery {
    /// Parse a raw query string (the part after `?`). Returns a human-readable
    /// message on any unknown key or malformed value, which the handler turns
    /// into a `400`.
    pub fn parse(query: Option<&str>) -> Result<Self, String> {
        let mut q = DebugQuery::default();
        let Some(raw) = query else { return Ok(q) };
        for pair in raw.split('&').filter(|s| !s.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "limit" => {
                    q.limit = Some(
                        value
                            .parse::<usize>()
                            .map_err(|_| format!("invalid limit '{value}' (expected a number)"))?,
                    )
                }
                "order" => q.order = Some(value.to_string()),
                "dir" => q.dir = Some(parse_dir(value)?),
                "example" => q.show_example = parse_bool("example", value)?,
                "pattern_id" => q.show_pattern_id = parse_bool("pattern_id", value)?,
                "verbose" => {
                    if parse_bool("verbose", value)? {
                        q.show_example = true;
                        q.show_pattern_id = true;
                    }
                }
                "min_ratio" => q.min_ratio = Some(parse_f64("min_ratio", value)?),
                "max_ratio" => q.max_ratio = Some(parse_f64("max_ratio", value)?),
                "filter" => q.filter = Some(value.to_string()),
                other => return Err(format!("unknown query parameter '{other}'")),
            }
        }
        Ok(q)
    }
}

fn parse_dir(value: &str) -> Result<Dir, String> {
    match value {
        "asc" => Ok(Dir::Asc),
        "desc" => Ok(Dir::Desc),
        _ => Err(format!("invalid dir '{value}' (expected 'asc' or 'desc')")),
    }
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(format!("invalid {key} '{value}' (expected true/false)")),
    }
}

fn parse_f64(key: &str, value: &str) -> Result<f64, String> {
    value
        .parse::<f64>()
        .map_err(|_| format!("invalid {key} '{value}' (expected a number)"))
}

/// Round to 2 decimals — the precision used for every rendered `*_ms` / `_mb` /
/// ratio value on the debug endpoints.
pub fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// The `docs` object attached to every debug endpoint: field-by-field notes on
/// how to read the rendered values (units, and where the compensation factor
/// does / does not apply) so operators don't have to consult the source.
pub fn docs() -> Value {
    jval!({
        "score": "shown in thousands (raw score ÷ 1000, e.g. \"37.2K\"); the raw score derives \
             from per-request costs in microseconds, so dividing by 1000 makes it proportional to \
             the millisecond (ms) figures shown alongside. For the `weighted` mode the value in \
             parentheses breaks it into the three factors it multiplies together — \
             avg (mean cost per request, in ms) × gain (latency multiplier, 1 = neutral) × \
             counts (windowed demand/supply/failure volume multiplier, shown in K like the score)",
        "score_units": "underlying units: the raw score is in microseconds and counts is a raw count",
        "latency": "avg_cost_*_ms and with_without_idx_ratio are the raw measured values; \
             without-index-compensation-factor is applied only inside the weighted score's gain \
             term and the latency regression guard, never to these displayed numbers",
    })
}

/// Render a raw score in **thousands**, one decimal, e.g. `37234.0` → `"37.2K"`.
/// See [`docs`] for why the score is shown ÷ 1000.
pub fn score_k(score: f64) -> String {
    format!("{:.1}K", score / 1000.0)
}

/// Render the `score` field: the score in thousands ([`score_k`]) plus, for the
/// `weighted` mode, the three factors whose product **is** that score — average
/// cost (ms), the latency `gain` multiplier, and the `counts` volume multiplier
/// (also in K) — e.g. `"37.2K (avg 12.34ms × gain 1.05 × counts 13.0K)"`. The
/// single-quantity modes have no such decomposition and render the bare score.
pub fn score_field(score: f64, components: Option<(f64, f64, f64)>) -> String {
    match components {
        Some((avg_us, gain, counts)) => format!(
            "{} (avg {:.2}ms × gain {:.2} × counts {})",
            score_k(score),
            avg_us / 1000.0,
            gain,
            score_k(counts),
        ),
        None => score_k(score),
    }
}

/// Average cost per request in **milliseconds** (2 dp), or `None` when the
/// bucket has no requests.
pub fn avg_ms(sum_us: i64, count: i64) -> Option<f64> {
    (count > 0).then(|| round2(sum_us as f64 / count as f64 / 1000.0))
}

/// Bytes rendered as **MiB** (base-2), 2 dp.
pub fn bytes_to_mib(bytes: i64) -> f64 {
    round2(bytes as f64 / (1024.0 * 1024.0))
}

/// A Unix-epoch-seconds timestamp (as stored in `created_at_epoch`) rendered as a
/// human-readable UTC string (`YYYY-MM-DDTHH:MM:SSZ`); empty on an out-of-range
/// value. Ordering still uses the raw epoch, so this is display-only.
pub fn epoch_to_iso(epoch: f64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(epoch as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

/// Supply compensated for the fact that a served GPA scans both tables of the
/// pair (raw `idx_scan` ≈ 2× requests). This is the only form ever surfaced.
pub fn compensated_idx_scan(last_idx_scan: i64) -> i64 {
    last_idx_scan / SCANS_PER_REQUEST
}

/// Raw measured latency ratio for the index: without-index avg ÷ with-index avg
/// (`> 1` means the index helps). `None` until the pattern has served requests
/// both with and without the index. This is the *uncompensated* ratio — the two
/// averages exactly as measured — so it demystifies the `gain` term the
/// `weighted` score derives from it (which additionally applies
/// `without-index-compensation-factor`).
pub fn with_without_idx_ratio(r: &PatternRow) -> Option<f64> {
    (r.cost_with_index_count > 0 && r.cost_without_index_count > 0).then(|| {
        let with = (r.cost_with_index_us as f64 / r.cost_with_index_count as f64).max(1.0);
        let without = (r.cost_without_index_us as f64 / r.cost_without_index_count as f64).max(1.0);
        round2(without / with)
    })
}

/// Sort `items` by `key` in `dir` and truncate to `limit`, returning the total
/// count *before* limiting alongside the trimmed list — the two numbers the
/// response envelope reports as `total` and `count`.
pub fn order_and_limit<T>(
    mut items: Vec<T>,
    key: impl Fn(&T) -> f64,
    dir: Dir,
    limit: Option<usize>,
) -> (usize, Vec<T>) {
    let total = items.len();
    items.sort_by(|a, b| {
        let (ka, kb) = (key(a), key(b));
        match dir {
            Dir::Asc => ka.total_cmp(&kb),
            Dir::Desc => kb.total_cmp(&ka),
        }
    });
    if let Some(l) = limit {
        items.truncate(l);
    }
    (total, items)
}

/// Wrap rendered `items` in the shared envelope, including the [`docs`] object
/// (field-by-field reading notes).
pub fn envelope(items_key: &str, total: usize, limit: Option<usize>, items: Vec<Value>) -> Value {
    jval!({
        "total": total,
        "count": items.len(),
        "limit": limit,
        "docs": docs(),
        items_key: items,
    })
}

/// JSON view of a created pattern's demand vs. supply, shared by the
/// `debug_created` and `debug_discrepancies` endpoints. `pattern_id` and
/// `example_request` are only included when `q` requests them.
pub fn created_view(r: &PatternRow, q: &DebugQuery) -> Value {
    let mut obj = jval!({
        "index": r.human_name,
        "explain_state": r.explain_state,
        "created_at": r.created_at_epoch.map(epoch_to_iso),
        "demand_count": r.demand_count,
        "demand_since_create": (r.demand_count - r.demand_at_create).max(0),
        "compensated_idx_scan": compensated_idx_scan(r.last_idx_scan),
        "index_mb": bytes_to_mib(r.index_bytes),
        "variety_estimate": r.variety_estimate,
        "avg_cost_with_index_ms": avg_ms(r.cost_with_index_us, r.cost_with_index_count),
        "avg_cost_without_index_ms": avg_ms(r.cost_without_index_us, r.cost_without_index_count),
        "with_without_idx_ratio": with_without_idx_ratio(r),
        "discrepancy_state": r.discrepancy_state,
        "discrepancy_ratio": r.discrepancy_ratio.map(round2),
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

/// Turn a query-parse error message into a `400`.
pub fn bad_request(msg: String) -> Response<Full<Bytes>> {
    text(StatusCode::BAD_REQUEST, &msg)
}

/// Log a debug-endpoint DB failure (with target) and return a 500.
pub fn db_error(what: &str, e: sea_orm::DbErr) -> Response<Full<Bytes>> {
    error!(target: "query_tracker_server", "debug /{what} query failed: {e:?}");
    text(StatusCode::INTERNAL_SERVER_ERROR, "database error")
}
