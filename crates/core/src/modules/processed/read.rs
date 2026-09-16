// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The processed blocks a request reads and their one read function.

use std::sync::Arc;

use solana_pubkey::Pubkey;

use super::ingest::SlotBlock;
use super::{AccountEntry, LiveAccount};

/// The newest state of a key along the processed blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessedAccount<'a> {
    Live(&'a LiveAccount),
    /// Closed, or written by an owner outside the program filter.
    Closed,
    /// Not written in the blocks. Read Postgres at `slot <= anchor_slot`.
    Unknown,
}

/// The immutable chain of blocks from the head down through the confirmed slot
/// and the retained slots below it. Every read in one request uses the same blocks.
pub struct ProcessedBlocks {
    /// Head slot, the response `context.slot`.
    pub slot: u64,
    /// Head block time in unix seconds.
    pub block_time: i64,
    /// Postgres bound for unknown keys: `slot <= anchor_slot`.
    pub anchor_slot: u64,
    /// Newest first.
    pub(super) blocks: Vec<Arc<SlotBlock>>,
}

impl ProcessedBlocks {
    /// The newest write of `pubkey` along the blocks, or [`ProcessedAccount::Unknown`].
    pub fn get_account(&self, pubkey: &Pubkey) -> ProcessedAccount<'_> {
        for block in &self.blocks {
            if let Some(entry) = block.accounts.get(pubkey) {
                return match entry {
                    AccountEntry::Live(account) => ProcessedAccount::Live(account),
                    AccountEntry::Closed => ProcessedAccount::Closed,
                };
            }
        }
        ProcessedAccount::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::{TestChain, anchor_at};
    use super::*;
    use crate::config::{AccountSelectorConfig, PubkeyDef};

    fn lamports(account: ProcessedAccount<'_>) -> Option<u64> {
        match account {
            ProcessedAccount::Live(LiveAccount { lamports, .. }) => Some(*lamports),
            _ => None,
        }
    }

    #[test]
    fn newest_version_wins_across_blocks() {
        let key = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let mut chain = TestChain::new();
        chain.block_with(101, 100, vec![(key, 1), (other, 7)]);
        chain.block_with(102, 101, vec![(key, 2)]);
        chain.block_with(103, 102, vec![]);
        chain.store.set_anchor(anchor_at(100));
        let blocks = chain.event().unwrap();
        assert_eq!(blocks.slot, 103);
        assert_eq!(lamports(blocks.get_account(&key)), Some(2));
        assert_eq!(lamports(blocks.get_account(&other)), Some(7));
        assert_eq!(
            blocks.get_account(&Pubkey::new_unique()),
            ProcessedAccount::Unknown
        );
    }

    #[test]
    fn closed_and_excluded_shadow_older_versions() {
        let closed = Pubkey::new_unique();
        let moved = Pubkey::new_unique();
        let selected = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let mut chain = TestChain::new();
        chain.filter = AccountSelectorConfig {
            include: vec![PubkeyDef(selected)],
            exclude: vec![],
        };
        chain.block_owned(101, 100, vec![(closed, selected, 5), (moved, selected, 5)]);
        chain.block_owned(102, 101, vec![(closed, selected, 0), (moved, other, 6)]);
        chain.store.set_anchor(anchor_at(100));
        let blocks = chain.event().unwrap();
        // Closed, not Unknown, so the caller never falls back to a Postgres row.
        assert_eq!(blocks.get_account(&closed), ProcessedAccount::Closed);
        assert_eq!(blocks.get_account(&moved), ProcessedAccount::Closed);

        chain.block_owned(103, 102, vec![(moved, selected, 7)]);
        let blocks = chain.event().unwrap();
        assert_eq!(blocks.slot, 103);
        assert_eq!(lamports(blocks.get_account(&moved)), Some(7));
    }

    #[test]
    fn retained_blocks_below_the_confirmed_slot_answer_from_memory() {
        let key = Pubkey::new_unique();
        let mut chain = TestChain::new();
        chain.block_with(98, 97, vec![(key, 3)]);
        chain.linear(99, 103);
        chain.store.set_anchor(anchor_at(100));
        let blocks = chain.event().unwrap();
        assert_eq!(blocks.anchor_slot, 100);
        assert_eq!(lamports(blocks.get_account(&key)), Some(3));
    }
}
