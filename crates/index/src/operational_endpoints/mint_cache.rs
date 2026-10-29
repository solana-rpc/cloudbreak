// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{convert::Infallible, sync::OnceLock};

use cloudbreak_core::modules::mint_cache::{MintCache, MintCacheIndexerSummary, MintEntryDebug};
use http_body_util::Full;
use hyper::{Response, body::Bytes};
use serde::Serialize;

use super::params::DebugParams;
use super::{json_error, json_ok};

/// Set once during startup so the debug endpoint can inspect the live mint cache.
pub static MINT_CACHE: OnceLock<MintCache> = OnceLock::new();

/// HTTP debug endpoint for the indexer side of the mint cache
/// (`cloudbreak_core::modules::mint_cache::MintCacheIndexer`). Read-only: it takes the indexer mutex for
/// an O(1) copy of the counters or one map lookup.
///
/// # Route
///
/// `GET /debug/modules/mint_cache`
///
/// # Query parameters
///
/// | Name     | Type   | Default | Description |
/// |----------|--------|---------|-------------|
/// | `pubkey` | base58 | unset   | One mint to look up in the in-memory copy. |
///
/// # Response
///
/// `Content-Type: application/json`. `200 OK`, `400 Bad Request` on an invalid parameter,
/// `503 Service Unavailable` before startup sets the mint cache. Without `[mint-cache]`:
/// `{"enabled": false}`.
///
/// ```json
/// {
///   "enabled": true,
///   "last_db_slot": 454305900,
///   "last_slot": 454306183,
///   "immutable": 88213004,
///   "mutable": 1398112,
///   "tombstones": 2210,
///   "closed_pending": null,
///   "pending_to_db": 412,
///   "db_write_running": false,
///   "last_block": { "slot": 454306183, "written": { "create": 3, "update": 0, "close": 0, "reinit": 0 }, "skipped": 41 },
///   "entry": { "pubkey": "…", "map": "immutable", "slot": null, "closed": false, "decimals": 6, "has_config": false }
/// }
/// ```
///
/// `last_db_slot` is `null` until the bootstrap wrote `mints`; `closed_pending` is `null` after it.
/// `entry` is present only with `pubkey`. `map` is `immutable`, `mutable`, `tombstone` or
/// `missing`; `slot` is `null` for an immutable entry, which stores none.
///
/// # Examples
///
/// ```text
/// curl http://localhost:8875/debug/modules/mint_cache
/// curl 'http://localhost:8875/debug/modules/mint_cache?pubkey=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
/// ```
pub(crate) fn handle(query: Option<&str>) -> Result<Response<Full<Bytes>>, Infallible> {
    let params = match DebugParams::from_query(query) {
        Ok(p) => p,
        Err(msg) => return Ok(json_error(400, &msg)),
    };
    let Some(mint_cache) = MINT_CACHE.get() else {
        return Ok(json_error(503, "mint cache not initialized"));
    };
    let indexer = mint_cache.indexer();
    let Some(summary) = indexer.summary() else {
        return json_ok(&serde_json::json!({ "enabled": false }));
    };
    json_ok(&MintCacheResponse {
        enabled: true,
        summary,
        entry: params.pubkey.and_then(|pubkey| indexer.entry(&pubkey)),
    })
}

#[derive(Serialize)]
struct MintCacheResponse {
    enabled: bool,
    #[serde(flatten)]
    summary: MintCacheIndexerSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    entry: Option<MintEntryDebug>,
}
