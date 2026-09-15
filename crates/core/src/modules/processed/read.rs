// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The request snapshot and its one read function.

use std::sync::Arc;

use solana_pubkey::Pubkey;

use super::ingest::SlotBlock;
use super::{AccountEntry, LiveAccount};

/// The newest state of a key along a view's chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup<'a> {
    Live(&'a LiveAccount),
    /// Closed, or written by an owner outside the program filter.
    Closed,
    /// Not written in the chain. Read Postgres at `slot <= anchor_slot`.
    Miss,
}

/// An immutable chain from the head block down through the confirmed slot and
/// the retained slots below it. Every lookup in one request uses the same view.
pub struct ProcessedView {
    /// Head slot, the response `context.slot`.
    pub slot: u64,
    /// Head block time in unix seconds.
    pub block_time: i64,
    /// Postgres bound for misses: `slot <= anchor_slot`.
    pub anchor_slot: u64,
    /// Newest first.
    pub(super) chain: Vec<Arc<SlotBlock>>,
}

impl ProcessedView {
    /// The newest write of `pubkey` along the chain, or [`Lookup::Miss`].
    pub fn lookup(&self, pubkey: &Pubkey) -> Lookup<'_> {
        for block in &self.chain {
            if let Some(entry) = block.accounts.get(pubkey) {
                return match entry {
                    AccountEntry::Live(account) => Lookup::Live(account),
                    AccountEntry::Closed => Lookup::Closed,
                };
            }
        }
        Lookup::Miss
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::{TestChain, anchor_at};
    use super::*;
    use crate::config::{AccountSelectorConfig, PubkeyDef};

    fn lamports(lookup: Lookup<'_>) -> Option<u64> {
        match lookup {
            Lookup::Live(LiveAccount { lamports, .. }) => Some(*lamports),
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
        let view = chain.event().unwrap();
        assert_eq!(view.slot, 103);
        assert_eq!(lamports(view.lookup(&key)), Some(2));
        assert_eq!(lamports(view.lookup(&other)), Some(7));
        assert_eq!(view.lookup(&Pubkey::new_unique()), Lookup::Miss);
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
        let view = chain.event().unwrap();
        // Closed, not Miss, so the caller never falls back to a Postgres row.
        assert_eq!(view.lookup(&closed), Lookup::Closed);
        assert_eq!(view.lookup(&moved), Lookup::Closed);

        chain.block_owned(103, 102, vec![(moved, selected, 7)]);
        let view = chain.event().unwrap();
        assert_eq!(view.slot, 103);
        assert_eq!(lamports(view.lookup(&moved)), Some(7));
    }

    #[test]
    fn retained_blocks_below_the_confirmed_slot_answer_lookups() {
        let key = Pubkey::new_unique();
        let mut chain = TestChain::new();
        chain.block_with(98, 97, vec![(key, 3)]);
        chain.linear(99, 103);
        chain.store.set_anchor(anchor_at(100));
        let view = chain.event().unwrap();
        assert_eq!(view.anchor_slot, 100);
        assert_eq!(lamports(view.lookup(&key)), Some(3));
    }
}
