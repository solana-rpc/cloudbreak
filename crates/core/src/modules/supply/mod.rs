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
//! accounts the block touched. The only hard part is the previous balance. This
//! module reads it from a bounded in-memory map keyed by pubkey. A hit is a
//! memory read. A miss goes to one batched by-pubkey DB read with no owner
//! routing. Every stake account is pinned so the epoch-reward burst is all hits.
//!
//! # Layout
//!
//! - [`tracker`]: the running total, the status machine, the bootstrap window,
//!   the gap-close set, the non-circulating balances, and the cache. The block
//!   entry points `apply_block` and `finish_block`.
//! - [`cache`]: `HotAccounts`, the map, the entry rule, the sweep. No async.
//! - [`prev`]: the batched miss read and the by-pubkey bootstrap resolve read.
//! - [`persist`]: `from_config`, `persist_supply_row`, the prior-run cleanup.
//! - [`read`]: the one read path the API shares.
//! - [`bootstrap`]: the two-pass resolve that flips the tracker Live.
//! - [`maintain`]: the slot-driven, health-gated cache sweeper.
//!
//! # Runtime model
//!
//! The indexer builds and persists the total; the API only reads the ring. A
//! disabled tracker is a cheap-clone no-op handle. The block path calls
//! `apply_block` under the block-writes lock, releases it before the inserts,
//! then `finish_block` commits or fails closed.
//!
//! # Lock protocol
//!
//! The miss read runs strictly before the block's own inserts, or it would read
//! the block's own row as the previous balance. `apply_block` returns before the
//! inserts are spawned. The block-writes lock serializes the bootstrap resolve's
//! second pass against the block path. It no longer spans the inserts: nothing
//! the resolve reads is written by a live block.
//!
//! # Newest state wins
//!
//! Every cache entry carries `(slot, write_version)`. A write-back, a seed, and
//! the scan refresh all keep the newer stamp, so an older update never clobbers
//! newer state at ingest or at seed.
//!
//! # Node requirements (enforced in [`persist::from_config`])
//!
//! - Owner map off: the cache replaces it, and a map-on run would write same-slot
//!   mask rows that violate the `(pubkey, slot)` key.
//! - Owner partitioning off: a pubkey-only read on owner partitions fans out.
//!   Checked against the catalog (`pg_class.relkind = 'r'`).
//! - `idx_accounts_pubkey_slot`, `idx_snapshot_accounts_pubkey_slot`: the miss
//!   read and the resolve ride them.
//! - `idx_accounts_stake_owner`, `idx_snapshot_accounts_stake_owner`: the
//!   recomputer's stake scan rides them, else it is a full ~1.15B-row scan.
//! - Empty `[programs]` filter and a `[snapshot]` section.
//! - Unfiltered geyser feed and a supply-only instance: documented, not checked.
//!
//! # Measured footprint
//!
//! The cache is ~272 MB RSS pre-sized to 2^22 buckets [est], against the ~185 GiB
//! of the owner-map delta [measured]. Steady miss rate ~150 per block at a 1M cap,
//! one ~7-10 ms read. See `local-docs/GETSUPPLY-METRICS-MATRIX.md`.
//!
//! # Residuals
//!
//! - Filtered geyser feed: `compr6` credits never arrive, ~2,000 lamports per
//!   slot of drift. Infrastructure, fixed by an unfiltered feed.
//! - A close inside a gap the incremental snapshot does not carry, for an account
//!   never written again, is over-counted until touched. A DB audit bounds it.
//! - One indexer per database: an out-of-band write to `accounts` breaks the
//!   eviction-consistency invariant silently.

pub mod bootstrap;
pub mod cache;
pub mod maintain;
pub mod persist;
pub mod prev;
pub mod read;
pub mod tracker;

pub use read::{SupplyRow, SupplySnapshot, load_latest_supply};
pub use tracker::{
    BlockOutcome, NonCirculatingBalance, Pending, SUPPLY_RING_SLOTS, SupplyCommit, SupplyTracker,
};
