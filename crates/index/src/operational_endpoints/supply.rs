// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{convert::Infallible, sync::OnceLock};

use cloudbreak_core::modules::supply::{SupplySummary, SupplyTracker, cache::Entry};
use http_body_util::Full;
use hyper::{Response, body::Bytes};
use serde::Serialize;

use super::params::{DebugDetail, DebugParams};
use super::{json_error, json_ok};

/// Set once during startup so the debug endpoint can inspect the live supply tracker.
pub static SUPPLY: OnceLock<SupplyTracker> = OnceLock::new();

/// HTTP debug endpoint for inspecting the live state of the supply tracker
/// (`cloudbreak_core::modules::supply::SupplyTracker`).
///
/// Always read-only. It takes the tracker's state mutex, which is the block path's
/// mutex, only for an O(1) copy of the counters or one map lookup. It never walks
/// the hot-accounts map.
///
/// # Route
///
/// `GET /debug/modules/supply`
///
/// # Query parameters
///
/// All parameters are optional.
///
/// | Name     | Type   | Default   | Description |
/// |----------|--------|-----------|-------------|
/// | `detail` | enum   | `summary` | `summary` returns the counters. `full` also looks up `pubkey`. |
/// | `pubkey` | base58 | unset     | The one cache entry to return. Requires `detail=full`. |
///
/// # Response
///
/// `Content-Type: application/json`. `200 OK` on success, `400 Bad Request` on an invalid
/// parameter, `503 Service Unavailable` if the tracker is not yet initialized. A node
/// without the `[supply]` section returns `{"enabled": false}`.
///
/// ```json
/// {
///   "enabled": true,
///   "status": "live",
///   "bootstrap_failed": false,
///   "total": 606238391817583060,
///   "slot": 300100200,
///   "startup_slot": 300099000,
///   "startup_touched": 0,
///   "gap_closes": 0,
///   "hot_entries": 1017342,
///   "pinned_entries": 0,
///   "capacity": 1835008,
///   "cap": 1000000,
///   "last_sweep_slot": 300099500,
///   "entry": { "pubkey": "…", "lamports": 1000000, "slot": 300100199, "pinned": false }
/// }
/// ```
///
/// `entry` is present only when `pubkey` is given, and is `null` when the account is not
/// cached.
///
/// # Examples
///
/// ```text
/// curl http://localhost:8875/debug/modules/supply
/// curl 'http://localhost:8875/debug/modules/supply?detail=full&pubkey=SysvarC1ock11111111111111111111111111111111'
/// ```
pub(crate) fn handle(query: Option<&str>) -> Result<Response<Full<Bytes>>, Infallible> {
    let params = match DebugParams::from_query(query) {
        Ok(p) => p,
        Err(msg) => return Ok(json_error(400, &msg)),
    };
    if params.pubkey.is_some() && params.detail != DebugDetail::Full {
        return Ok(json_error(400, "`pubkey` requires `detail=full`"));
    }

    let Some(tracker) = SUPPLY.get() else {
        return Ok(json_error(503, "supply tracker not initialized"));
    };
    let Some(summary) = tracker.summary() else {
        return json_ok(&serde_json::json!({ "enabled": false }));
    };

    let entry = params.pubkey.map(|pubkey| {
        tracker.entry(&pubkey).map(|entry| EntryDebug {
            pubkey: pubkey.to_string(),
            entry,
        })
    });

    json_ok(&SupplyResponse {
        enabled: true,
        summary,
        entry,
    })
}

#[derive(Serialize)]
struct SupplyResponse {
    enabled: bool,
    #[serde(flatten)]
    summary: SupplySummary,
    /// The requested cache entry. Absent without `pubkey`, `null` when not cached.
    #[serde(skip_serializing_if = "Option::is_none")]
    entry: Option<Option<EntryDebug>>,
}

#[derive(Serialize)]
struct EntryDebug {
    pubkey: String,
    #[serde(flatten)]
    entry: Entry,
}
