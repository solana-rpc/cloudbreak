// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{convert::Infallible, sync::OnceLock};

use http_body_util::Full;
use hyper::{Response, body::Bytes};
use serde::Serialize;

use super::params::{BlockKindFilter, DebugDetail, DebugParams};
use super::{json_error, json_ok};
use crate::db_queries::{self, ChainTips};
use crate::modules::finalize_slot::SlotFinalizer;

/// Set once during startup so the debug endpoint can inspect the live finalizer.
pub static FINALIZER: OnceLock<SlotFinalizer> = OnceLock::new();

/// HTTP debug endpoint for inspecting the live state of the slot finalizer
/// (`crate::modules::finalize_slot::SlotFinalizer`).
///
/// Always read-only. It briefly takes the finalizer's internal mutex to copy the requested
/// metadata into owned structures, then releases it before base58-encoding pubkeys and
/// serializing JSON, so it does not hold the hot-path lock across the expensive work.
///
/// # Route
///
/// `GET /debug/modules/finalizer`
///
/// # Query parameters
///
/// All parameters are optional.
///
/// | Name           | Type   | Default   | Description |
/// |----------------|--------|-----------|-------------|
/// | `detail`       | enum   | `summary` | Verbosity. One of `summary`, `slots`, `full`. |
/// | `kind`         | enum   | `all`     | Filters the blocks listing: `all`, `live` (gRPC, has chain data), `repaired` (from snapshot). Ignored when `detail=summary`. |
/// | `min_slot`     | u64    | unset     | Restricts the blocks/pending listings to slots `>= min_slot`. |
/// | `max_slot`     | u64    | unset     | Restricts the blocks/pending listings to slots `<= max_slot`. |
/// | `limit`        | usize  | unset     | Caps the blocks/pending listings (after the ascending-slot sort). |
/// | `with_pubkeys` | bool   | `false`   | Adds per-block `account_pubkeys`/`closed_pubkeys`. Requires `detail=full` and a bounded selection (`limit`, or both `min_slot` and `max_slot`). |
///
/// ### `detail` values
///
/// - `summary` — chain tips (`db`), pause state, `bound`, and `stats` only; no per-slot listings.
/// - `slots` — adds the `pending` slot list and per-block metadata (`blocks`), without pubkeys.
/// - `full` — same as `slots`, plus `account_pubkeys`/`closed_pubkeys` when `with_pubkeys=true`.
///
/// # Response
///
/// `Content-Type: application/json`. `200 OK` on success, `400 Bad Request` on an invalid
/// parameter, `503 Service Unavailable` if the finalizer is not yet initialized.
///
/// ```json
/// {
///   "db": { "confirmed_slot": 300100200, "finalized_slot": 300100168, "finalized_behind_confirmed": 32 },
///   "paused": false,
///   "pause_reasons": [],
///   "bound": 1000,
///   "stats": {
///     "blocks_count": 37, "blocks_live": 30, "blocks_repaired": 7,
///     "oldest_block_slot": 300100164, "newest_block_slot": 300100200,
///     "pending_count": 5, "pending_oldest": 300100196, "pending_newest": 300100200,
///     "pending_oldest_behind_confirmed": 4
///   },
///   "pending": [300100196, 300100197],
///   "blocks": [
///     { "slot": 300100200, "blockhash": "…", "parent_slot": 300100199, "parent_blockhash": "…",
///       "account_count": 128, "closed_account_count": 4, "block_time": 1730000000,
///       "is_repaired": false, "in_pending": true }
///   ]
/// }
/// ```
///
/// # Examples
///
/// ```text
/// curl http://localhost:8875/debug/modules/finalizer
/// curl 'http://localhost:8875/debug/modules/finalizer?detail=slots&kind=repaired'
/// curl 'http://localhost:8875/debug/modules/finalizer?detail=full&min_slot=300100190&max_slot=300100200&with_pubkeys=true'
/// ```
pub(crate) async fn handle(query: Option<&str>) -> Result<Response<Full<Bytes>>, Infallible> {
    let params = match DebugParams::from_query(query) {
        Ok(p) => p,
        Err(msg) => return Ok(json_error(400, &msg)),
    };

    let Some(finalizer) = FINALIZER.get() else {
        return Ok(json_error(503, "finalizer not initialized"));
    };

    let tips = db_queries::get_chain_tips(&finalizer.db).await;
    let include_listings = params.detail >= DebugDetail::Slots;

    // Phase 1: copy what we need while holding the hot-path lock (pubkey bytes only for the
    // already-filtered, bounded selection); base58 encoding happens after the lock is released.
    struct RawBlock {
        slot: u64,
        blockhash: String,
        parent_slot: u64,
        parent_blockhash: String,
        account_count: usize,
        closed_account_count: usize,
        block_time: Option<i64>,
        is_repaired: bool,
        in_pending: bool,
        account_pubkeys: Option<Vec<Vec<u8>>>,
        closed_pubkeys: Option<Vec<Vec<u8>>>,
    }

    let (stats, pause_reasons, pending, raw_blocks) = {
        let inner = finalizer.inner.lock().expect("Failed to lock finalizer");

        let blocks_count = inner.blocks.len();
        let blocks_repaired = inner
            .blocks
            .values()
            .filter(|e| e.blockhash.is_empty())
            .count();
        let pending_oldest = inner.pending.iter().next().copied();
        let stats = FinalizerStats {
            blocks_count,
            blocks_live: blocks_count - blocks_repaired,
            blocks_repaired,
            oldest_block_slot: inner.blocks.keys().min().copied(),
            newest_block_slot: inner.blocks.keys().max().copied(),
            pending_count: inner.pending.len(),
            pending_oldest,
            pending_newest: inner.pending.iter().next_back().copied(),
            pending_oldest_behind_confirmed: match (tips.confirmed_slot, pending_oldest) {
                (Some(confirmed), Some(pending)) => Some(confirmed.saturating_sub(pending)),
                _ => None,
            },
        };

        let pause_reasons: Vec<String> = inner
            .pause_reasons
            .iter()
            .map(|r| r.as_str().to_string())
            .collect();

        let (pending, raw_blocks) = if include_listings {
            let mut pending: Vec<u64> = inner
                .pending
                .iter()
                .copied()
                .filter(|slot| params.in_range(*slot))
                .collect();
            if let Some(limit) = params.limit {
                pending.truncate(limit);
            }

            // Filter by kind/range, sort ascending, cap with `limit`, then copy (so pubkey bytes
            // are cloned only for the bounded selection).
            let mut selected: Vec<_> = inner
                .blocks
                .iter()
                .filter(|(slot, entry)| {
                    let is_repaired = entry.blockhash.is_empty();
                    params.in_range(**slot)
                        && match params.kind {
                            BlockKindFilter::All => true,
                            BlockKindFilter::Live => !is_repaired,
                            BlockKindFilter::Repaired => is_repaired,
                        }
                })
                .collect();
            selected.sort_by_key(|(slot, _)| **slot);
            if let Some(limit) = params.limit {
                selected.truncate(limit);
            }

            let raw_blocks: Vec<RawBlock> = selected
                .into_iter()
                .map(|(slot, entry)| RawBlock {
                    slot: *slot,
                    blockhash: entry.blockhash.clone(),
                    parent_slot: entry.parent_slot,
                    parent_blockhash: entry.parent_blockhash.clone(),
                    account_count: entry.accounts.accounts.len(),
                    closed_account_count: entry.accounts.closed_accounts.len(),
                    block_time: entry.accounts.block_time.as_ref().map(|t| t.timestamp),
                    is_repaired: entry.blockhash.is_empty(),
                    in_pending: inner.pending.contains(slot),
                    account_pubkeys: params.with_pubkeys.then(|| entry.accounts.accounts.clone()),
                    closed_pubkeys: params
                        .with_pubkeys
                        .then(|| entry.accounts.closed_accounts.clone()),
                })
                .collect();

            (Some(pending), Some(raw_blocks))
        } else {
            (None, None)
        };

        (stats, pause_reasons, pending, raw_blocks)
    };

    // Phase 2 (lock released): base58-encode pubkeys and build the response blocks.
    let blocks = raw_blocks.map(|raw_blocks| {
        raw_blocks
            .into_iter()
            .map(|b| BlockDebug {
                slot: b.slot,
                blockhash: b.blockhash,
                parent_slot: b.parent_slot,
                parent_blockhash: b.parent_blockhash,
                account_count: b.account_count,
                closed_account_count: b.closed_account_count,
                block_time: b.block_time,
                is_repaired: b.is_repaired,
                in_pending: b.in_pending,
                account_pubkeys: b.account_pubkeys.map(|keys| encode_pubkeys(&keys)),
                closed_pubkeys: b.closed_pubkeys.map(|keys| encode_pubkeys(&keys)),
            })
            .collect::<Vec<_>>()
    });

    let state = FinalizerDebug {
        paused: !pause_reasons.is_empty(),
        pause_reasons,
        bound: finalizer.bound,
        stats,
        pending,
        blocks,
    };

    json_ok(&FinalizerResponse { db: tips, state })
}

/// Base58-encodes raw 32-byte account pubkeys, falling back to hex for unexpected lengths.
fn encode_pubkeys(keys: &[Vec<u8>]) -> Vec<String> {
    keys.iter()
        .map(|bytes| match <[u8; 32]>::try_from(bytes.as_slice()) {
            Ok(arr) => solana_pubkey::Pubkey::new_from_array(arr).to_string(),
            Err(_) => hex::encode(bytes),
        })
        .collect()
}

#[derive(Serialize)]
struct FinalizerResponse {
    db: ChainTips,
    #[serde(flatten)]
    state: FinalizerDebug,
}

/// Owned snapshot of the finalizer state for the debug endpoint.
#[derive(Serialize)]
struct FinalizerDebug {
    /// Whether the worker is currently paused (i.e. there is at least one pause reason).
    paused: bool,
    /// Active pause reasons (e.g. `"gap_fill"`).
    pause_reasons: Vec<String>,
    /// Back-pressure bound: max live pending slots before `note_finalized` blocks.
    bound: usize,
    stats: FinalizerStats,
    /// Slots awaiting finalization, ascending. Present only when `detail >= slots`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pending: Option<Vec<u64>>,
    /// Per-block metadata for the confirmed-but-not-finalized blocks map. Present only when
    /// `detail >= slots`.
    #[serde(skip_serializing_if = "Option::is_none")]
    blocks: Option<Vec<BlockDebug>>,
}

#[derive(Serialize)]
struct FinalizerStats {
    blocks_count: usize,
    blocks_live: usize,
    blocks_repaired: usize,
    oldest_block_slot: Option<u64>,
    newest_block_slot: Option<u64>,
    pending_count: usize,
    /// Smallest pending slot (the next one to finalize).
    pending_oldest: Option<u64>,
    pending_newest: Option<u64>,
    /// `confirmed_slot - pending_oldest` (how far the next slot to finalize lags confirmed).
    pending_oldest_behind_confirmed: Option<u64>,
}

#[derive(Serialize)]
struct BlockDebug {
    slot: u64,
    blockhash: String,
    parent_slot: u64,
    parent_blockhash: String,
    account_count: usize,
    closed_account_count: usize,
    block_time: Option<i64>,
    is_repaired: bool,
    /// Whether this slot is also queued in `pending` (a finalized notification was received).
    in_pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_pubkeys: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_pubkeys: Option<Vec<String>>,
}
