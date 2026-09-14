// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The request snapshot and its one read function.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use solana_pubkey::Pubkey;

use super::ingest::SlotBlock;
use super::{AccountEntry, Lookup};

/// An immutable chain from the head block down to the block above the anchor.
/// Every lookup in one request uses the same view.
pub struct ProcessedView {
    /// Head slot, the response `context.slot`.
    pub slot: u64,
    /// Head block time in unix seconds.
    pub block_time: i64,
    /// Postgres bound for misses: `slot <= anchor_slot`.
    pub anchor_slot: u64,
    /// Newest first.
    pub(super) chain: Vec<Arc<SlotBlock>>,
    pub(super) anchor_polled_at: Instant,
}

impl ProcessedView {
    /// The newest write of `pubkey` along the chain, or [`Lookup::Miss`].
    pub fn lookup(&self, pubkey: &Pubkey) -> Lookup<'_> {
        for block in &self.chain {
            if let Some(entry) = block.accounts.get(pubkey) {
                return match entry {
                    AccountEntry::Live(account) => Lookup::Live(account),
                    AccountEntry::Closed => Lookup::Closed,
                    AccountEntry::Excluded { owner } => Lookup::Excluded(*owner),
                };
            }
        }
        Lookup::Miss
    }
}

impl fmt::Debug for ProcessedView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessedView")
            .field("slot", &self.slot)
            .field("block_time", &self.block_time)
            .field("anchor_slot", &self.anchor_slot)
            .field("chain_len", &self.chain.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::LiveAccount;
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
        // Closed and Excluded, not Miss, so the caller never falls back to a Postgres row.
        assert_eq!(view.lookup(&closed), Lookup::Closed);
        assert_eq!(view.lookup(&moved), Lookup::Excluded(other));

        chain.block_owned(103, 102, vec![(moved, selected, 7)]);
        let view = chain.event().unwrap();
        assert_eq!(view.slot, 103);
        assert_eq!(lamports(view.lookup(&moved)), Some(7));
    }
}
