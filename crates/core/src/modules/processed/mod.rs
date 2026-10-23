// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Processed commitment for getAccountInfo, getMultipleAccounts, getBalance,
//! getTokenAccountBalance, getTokenSupply and getSlot.
//!
//! Postgres holds confirmed data only. This module subscribes to Yellowstone
//! blocks with accounts at processed commitment and keeps the blocks around the
//! Postgres confirmed slot in memory. Each request takes one [`ProcessedBlocks`]:
//! the highest stored block and its parents down through the confirmed slot,
//! linked by parent slot and parent blockhash. A key written in the blocks is
//! answered from memory, and an unknown key reads Postgres at `slot <= anchor_slot`.
//! When no chain can be proven, [`ProcessedAccounts::blocks`] returns `None` and
//! the request takes the confirmed path. The module has no Postgres dependency.
//! The confirmed slot and its blockhash arrive as an [`Anchor`] on a watch
//! channel from the API slot syncronizer.
//!
//! # Layout
//!
//! - `mod.rs`: the public surface and the constants.
//! - `ingest.rs`: builds a `SlotBlock` and classifies each account as live or
//!   closed. A write by an owner outside the API program filter is a close.
//! - `store.rs`: the block store. One block per slot, the anchor, conflicts,
//!   retention, the slot cap and the latest chained blocks.
//! - `read.rs`: [`ProcessedBlocks::get_account`], the one read function the API calls,
//!   and the `processed_read` span with the store size.
//! - `writer.rs`: [`ProcessedWriter`], the single writer, which owns the store
//!   without a lock.
//!
//! # Runtime model
//!
//! The API gRPC feed (`modules::grpc` at processed commitment) hands each block
//! to the [`ProcessedWriter`], and the API slot syncronizer hands it each
//! [`Anchor`]. After every block or anchor change the writer prunes, selects the
//! latest chained blocks and publishes them behind an `RwLock`. A request clones
//! one `Arc` and holds no lock while it reads. A block leaves memory when the
//! last [`ProcessedBlocks`] that pins it drops, in the writer after the publish
//! lock or in a request. While the feed is down, requests take the confirmed path.
//!
//! # Latest chained blocks
//!
//! The head is the highest stored slot. Its walk follows parent links, each
//! checked by blockhash, down to the confirmed slot, whose blockhash comes from
//! the `slots` row. The walk fails, and no blocks are served, when a link is
//! missing, a blockhash differs, or the head has no `block_time`. A second blockhash
//! for a stored slot, or a parent blockhash that does not match the stored
//! parent, deletes the conflicting block and its descendants and serves nothing
//! until the confirmed slot passes that slot.
//!
//! # Retention
//!
//! Only the Postgres confirmed slot prunes. Blocks below it stay for
//! `RETAINED_SLOTS_BELOW_CONFIRMED` slots and serve hot keys from memory. The
//! Postgres bound stays at the confirmed slot, so a retained write is the
//! newest one whenever the chain is linked. Above the confirmed slot at most
//! `MAX_SLOTS_ABOVE_CONFIRMED` slots are kept. Past that the lowest goes, which
//! breaks the walk until Postgres catches up.
//!
//! # Enable rules
//!
//! The API `[processed-accounts]` section with `enabled = true` turns it on.
//! Otherwise [`ProcessedAccounts::default()`] is a no-op handle and `blocks`
//! returns `None`. [`ProcessedAccounts::writer`] gives the API feed the writer.
//!
//! # Node requirements
//!
//! - A `[grpc]` Yellowstone endpoint that allows processed commitment, plus its x-token.
//! - `[slot-syncronizer]` enabled, which publishes the [`Anchor`].
//! - No owner map, no program filter change, no indexer change, no
//!   `replay_stored_slots`. Each API instance carries a full block feed.
//!
//! # Upstream invariants
//!
//! Correctness leans on yellowstone-grpc plugin and Agave behaviour. Recheck
//! each one on every plugin major version bump.
//!
//! - At most one entry per pubkey per block, the one with the highest
//!   `write_version`. The plugin's `ProcessingSlot::seal()` enforces it. Ingest
//!   inserts each pubkey without dedup, so if it breaks, a repeated pubkey keeps
//!   an arbitrary version from that block.
//! - One block per slot per stream. The plugin's block assembly enforces it. A
//!   second version is a conflict.
//! - Account writes are sent before the block seals. The plugin's message
//!   ordering enforces it. If it breaks, a block misses writes and serves stale
//!   data.
//! - A `Block` message holds exactly one bank. The plugin buffers by `bank_id`
//!   and seals a bank only after its freeze-time sysvar writes
//!   (`block_reconstruction_v2.rs`), so a dead bank sends no block. If it
//!   breaks, a block that mixes two attempts at a slot can be served.
//! - `parent_blockhash` equals the parent block's `blockhash`. Agave bank
//!   construction enforces it. If it breaks, nothing links and no blocks are served.

mod ingest;
mod read;
mod store;
mod writer;

pub use read::{ProcessedAccount, ProcessedBlocks};
pub use writer::ProcessedWriter;

use std::sync::{Arc, RwLock};

use solana_pubkey::Pubkey;

use crate::config::{AccountSelectorConfig, ProcessedAccountsConfig};
/// Stored slots above the confirmed slot.
pub(crate) const MAX_SLOTS_ABOVE_CONFIRMED: usize = 32;
/// Slots kept below the confirmed slot.
pub(crate) const RETAINED_SLOTS_BELOW_CONFIRMED: u64 = 4;

/// The Postgres confirmed slot and its blockhash, published by the API slot syncronizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub confirmed_slot: u64,
    /// Base58, as in `slots.blockhash` and `SubscribeUpdateBlock.blockhash`.
    pub confirmed_blockhash: String,
}

/// One account version held in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAccount {
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Arc<Vec<u8>>,
}

/// An account write in one block.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AccountEntry {
    Live(LiveAccount),
    /// Zero lamports, or an owner outside the program filter. Shadows every older version.
    Closed,
}

struct Shared {
    program_filter: Arc<AccountSelectorConfig>,
    latest: RwLock<Option<Arc<ProcessedBlocks>>>,
}

impl Shared {
    /// Swaps in the latest blocks. The old ones drop after the write lock is released.
    fn set_latest(&self, next: Option<Arc<ProcessedBlocks>>) {
        let _previous = {
            let mut guard = self.latest.write().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *guard, next)
        };
    }
}

/// Cheap-clone handle to the latest processed blocks. `None` is the disabled handle.
#[derive(Clone, Default)]
pub struct ProcessedAccounts(Option<Arc<Shared>>);

impl ProcessedAccounts {
    /// Returns the disabled handle when the section is absent or disabled.
    pub fn from_config(
        config: Option<&ProcessedAccountsConfig>,
        program_filter: Arc<AccountSelectorConfig>,
    ) -> Self {
        if !config.is_some_and(|c| c.enabled) {
            return Self::default();
        }
        Self(Some(Arc::new(Shared {
            program_filter,
            latest: RwLock::new(None),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// The single writer for the API feed; the disabled writer when disabled. Call it once.
    pub fn writer(&self) -> ProcessedWriter {
        self.0.clone().map(ProcessedWriter::new).unwrap_or_default()
    }

    /// The blocks for one request, or `None` when no chain can be proven.
    pub fn blocks(&self) -> Option<Arc<ProcessedBlocks>> {
        self.0
            .as_ref()?
            .latest
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::store::tests::{TestChain, anchor_at};
    use super::*;

    pub(super) fn config() -> ProcessedAccountsConfig {
        ProcessedAccountsConfig { enabled: true }
    }

    pub(super) fn handle(config: &ProcessedAccountsConfig) -> ProcessedAccounts {
        ProcessedAccounts::from_config(Some(config), Arc::new(AccountSelectorConfig::default()))
    }

    #[test]
    fn from_config_gates_on_enabled() {
        let disabled = ProcessedAccounts::default();
        assert!(!disabled.is_enabled());
        assert!(disabled.blocks().is_none());

        let mut off = config();
        off.enabled = false;
        assert!(!handle(&off).is_enabled());
        let none = ProcessedAccounts::from_config(None, Arc::new(AccountSelectorConfig::default()));
        assert!(!none.is_enabled());

        let enabled = handle(&config());
        assert!(enabled.is_enabled());
        assert!(enabled.blocks().is_none());
    }

    #[test]
    fn blocks_returns_the_latest_blocks() {
        let handle = handle(&config());
        let shared = handle.0.as_ref().unwrap();
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.store.set_anchor(anchor_at(100));
        shared.set_latest(chain.event().map(Arc::new));
        assert_eq!(handle.blocks().unwrap().slot, 101);
        shared.set_latest(None);
        assert!(handle.blocks().is_none());
    }
}
