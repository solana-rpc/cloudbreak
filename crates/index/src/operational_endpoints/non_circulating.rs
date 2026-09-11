// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{convert::Infallible, sync::OnceLock};

use cloudbreak_core::modules::non_circulating::{
    NonCirculatingSummary, NonCirculatingTracker, StakeEntryView,
};
use http_body_util::Full;
use hyper::{Response, body::Bytes};
use serde::Serialize;

use super::params::{DebugDetail, DebugParams};
use super::{json_error, json_ok};

/// Set once during startup so the debug endpoint can inspect the live tracker.
pub static NON_CIRCULATING: OnceLock<NonCirculatingTracker> = OnceLock::new();

/// Expiry-set heads returned when `limit` is not given.
const DEFAULT_HEADS: usize = 10;

/// HTTP debug endpoint for inspecting the live non-circulating tracker
/// (`cloudbreak_core::modules::non_circulating::NonCirculatingTracker`).
///
/// Always read-only. It takes the tracker's state mutex, which is the block path's
/// mutex, only for an O(1) copy of the counters, the bounded heads of the two
/// expiry sets, or one map lookup. It never walks the stake map.
///
/// # Route
///
/// `GET /debug/modules/non_circulating`
///
/// # Query parameters
///
/// All parameters are optional.
///
/// | Name     | Type   | Default   | Description |
/// |----------|--------|-----------|-------------|
/// | `detail` | enum   | `summary` | `summary` returns the counters and heads. `full` also looks up `pubkey`. |
/// | `limit`  | usize  | `10`      | Heads returned from each expiry set. |
/// | `pubkey` | base58 | unset     | The one map entry to return. Requires `detail=full`. |
///
/// # Response
///
/// `Content-Type: application/json`. `200 OK` on success, `400 Bad Request` on an invalid
/// parameter, `503 Service Unavailable` if the tracker is not yet initialized. A node
/// without `[largest-accounts]` or `[supply]` returns `{"enabled": false}`.
///
/// ```json
/// {
///   "enabled": true,
///   "status": "live",
///   "slot": 300100200,
///   "clock": { "slot": 300100200, "epoch": 694, "unix_timestamp": 1730000000 },
///   "members": 3134,
///   "non_circulating_lamports": 137000000000000000,
///   "stake_accounts": 1435120,
///   "by_epoch": 1200,
///   "by_timestamp": 1800,
///   "epoch_heads": [[695, "…"]],
///   "timestamp_heads": [[1730001000, "…"]],
///   "entry": { "lamports": 1000000, "lockup_unix_timestamp": 0, "lockup_epoch": 700, "slot": 300100199,
///              "stake_owned": true, "lockup": true, "listed_withdrawer": false, "pinned": false, "member": true }
/// }
/// ```
///
/// `entry` is present only when `pubkey` is given, and is `null` when the account is not
/// in the map.
///
/// # Examples
///
/// ```text
/// curl http://localhost:8875/debug/modules/non_circulating
/// curl 'http://localhost:8875/debug/modules/non_circulating?limit=50'
/// curl 'http://localhost:8875/debug/modules/non_circulating?detail=full&pubkey=<base58>'
/// ```
pub(crate) fn handle(query: Option<&str>) -> Result<Response<Full<Bytes>>, Infallible> {
    let params = match DebugParams::from_query(query) {
        Ok(p) => p,
        Err(msg) => return Ok(json_error(400, &msg)),
    };
    if params.pubkey.is_some() && params.detail != DebugDetail::Full {
        return Ok(json_error(400, "`pubkey` requires `detail=full`"));
    }

    let Some(tracker) = NON_CIRCULATING.get() else {
        return Ok(json_error(503, "non-circulating tracker not initialized"));
    };
    let Some(summary) = tracker.summary(params.limit.unwrap_or(DEFAULT_HEADS)) else {
        return json_ok(&serde_json::json!({ "enabled": false }));
    };

    let entry = params.pubkey.map(|pubkey| tracker.entry(&pubkey));

    json_ok(&NonCirculatingResponse {
        enabled: true,
        summary,
        entry,
    })
}

#[derive(Serialize)]
struct NonCirculatingResponse {
    enabled: bool,
    #[serde(flatten)]
    summary: NonCirculatingSummary,
    /// The requested map entry. Absent without `pubkey`, `null` when not in the map.
    #[serde(skip_serializing_if = "Option::is_none")]
    entry: Option<Option<StakeEntryView>>,
}
