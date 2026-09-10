// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! getSupply with the owner map off, served from a bounded hot-accounts cache.
//!
//! # What it does
//!
//! getSupply keeps a running total: anchor on the snapshot bank capitalization,
//! then per confirmed block add the sum of `new - previous` lamports over the
//! accounts the block touched. The only hard part is the previous balance. For
//! a stake pubkey it comes from the non-circulating stake map, which owns every
//! stake account and reports the summed stake delta per block. For every other
//! account this module reads it from a bounded in-memory map keyed by pubkey. A
//! hit is a memory read. A miss goes to one batched by-pubkey DB read with no
//! owner routing.
//!
//! # Layout
//!
//! - [`tracker`]: the running total, the status machine, the bootstrap window,
//!   the gap-close set, and the cache. The block entry points
//!   `build_block_pending`, `apply_block` and `finish_block`.
//! - [`cache`]: `HotAccounts`, the map, the entry rule, the sweep. No async.
//! - [`prev`]: the batched miss read and the by-pubkey bootstrap resolve read.
//! - [`persist`]: `from_config`, `persist_supply_row`, the prior-run cleanup.
//! - [`read`]: the one read path the API shares.
//! - [`bootstrap`]: the anchor seed and the two-pass resolve that flips Live.
//! - [`maintain`]: the slot-driven, health-gated cache sweeper.
//!
//! # Runtime model
//!
//! The indexer builds and persists the total. The API only reads the ring. A
//! disabled tracker is a cheap-clone no-op handle. The block path folds the
//! block's accounts with `build_block_pending`, hands them, the stake map's
//! outcome and the block's row writes to `apply_block`, then `finish_block`
//! commits or fails closed and persists the row off the block path. Each
//! commit carries the stake map's running non-circulating sum.
//!
//! # One balance source per pubkey
//!
//! The stake map reports the pubkeys it handled, and the tracker skips them.
//! A stake-owned account never enters the hot cache: a write-back for one
//! drops the entry instead, so a pubkey that moves from system to stake
//! ownership is counted through the cache once and then belongs to the map.
//! While the tracker is Bootstrapping it records every touch and ignores the
//! stake outcome, so the window delta is resolved against the snapshot alone
//! whichever tracker flips Live first.
//!
//! # Lock protocol
//!
//! `apply_block` holds the block-writes lock from its first probe to its last
//! miss write-back, so a live block, a repaired block, and the bootstrap
//! resolve's second pass never interleave on the cache. A live block's miss read
//! excludes the block's own slot, so it can never see the block's own row and
//! runs alongside the block's row writes. A repaired block's read is unbounded,
//! because a gap write already absorbed by a later live block must come back
//! and count zero, so its row writes wait for the read.
//!
//! # Newest slot wins
//!
//! Every cache entry carries the slot of its last write. A write-back keeps the
//! newer slot, and a block at or below the tracker's applied slot contributes
//! nothing, so a replayed or out-of-order block never counts a delta twice.
//!
//! The feed carries one version per pubkey per block, the newest one. The
//! yellowstone plugin enforces it: `ProcessingSlot::seal` drops every account
//! entry whose write version is not the per-pubkey maximum. Nothing here dedupes
//! a block. If that guarantee went away, an older version arriving after the
//! newest in one block would be a same-slot hit and count zero, which is right,
//! but one arriving before it would count a stale delta.
//!
//! # Node requirements (enforced in [`persist::from_config`])
//!
//! - Owner map off: the cache replaces it, and a map-on run would write same-slot
//!   mask rows that violate the `(pubkey, slot)` key.
//! - Owner partitioning off: a pubkey-only read on owner partitions fans out.
//!   Checked against the catalog (`pg_class.relkind = 'r'`).
//! - `idx_accounts_pubkey_slot`, `idx_snapshot_accounts_pubkey_slot`: the miss
//!   read and the resolve ride them.
//! - Empty `[programs]` filter and a `[snapshot]` section.
//! - Unfiltered geyser feed and a supply-only instance: documented, not checked.
//!
//! # Footprint
//!
//! The cache is pre-sized for the cap, its 10% sweep slack, and the failure-pin
//! budget: 1.3M items at the default cap, which rounds to 2^21 buckets of 65
//! bytes, about 136 MB [est]. The stake map beside it is about 153 MB [est].
//! Steady miss rate ~150 per block at a 1M cap [measured], one ~7-10 ms read.
//!
//! # Residuals
//!
//! - Filtered geyser feed: `compr6` credits never arrive, ~2,000 lamports per
//!   slot of drift. Infrastructure, fixed by an unfiltered feed.
//! - A close inside a gap the incremental snapshot does not carry, for an account
//!   never written again, is over-counted until touched. A DB audit bounds it.
//! - One indexer per database: an out-of-band write to `accounts` breaks the
//!   eviction-consistency invariant silently.
//! - A write failure on a block whose stake pubkeys the map handled is not
//!   pinned for those pubkeys. The map holds their balance, so the DB is not
//!   consulted for them until they leave stake ownership.

pub mod bootstrap;
pub mod cache;
pub mod maintain;
pub mod persist;
pub mod prev;
pub mod read;
pub mod tracker;

use std::time::Duration;

pub use read::{SupplyRow, SupplySnapshot, load_latest_supply};
pub use tracker::{
    BlockOutcome, Pending, SUPPLY_RING_SLOTS, SupplyCommit, SupplySummary, SupplyTracker,
};

/// Timeout for the seed and bootstrap queries: the anchor upsert, the startup
/// balance resolve, and the Live row.
pub const SEED_TIMEOUT: Duration = Duration::from_secs(60);
