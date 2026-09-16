// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The block store state machine. Pure and synchronous: it takes events, keeps
//! one block per slot, prunes, and selects the latest chained blocks.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Instant;

use super::ingest::SlotBlock;
use super::read::ProcessedBlocks;
use super::{Anchor, MAX_SLOTS_ABOVE_CONFIRMED, RETAINED_SLOTS_BELOW_CONFIRMED};
use crate::metrics;

pub(crate) struct BlockStore {
    pub(super) blocks: BTreeMap<u64, Arc<SlotBlock>>,
    pub(super) dead: BTreeSet<u64>,
    /// Slots with a `SLOT_CREATED_BANK` in this session.
    pub(super) created: HashSet<u64>,
    /// No blocks are served until the confirmed slot passes this slot.
    pub(super) conflict_until: Option<u64>,
    pub(super) session_floor: u64,
    pub(super) max_slot_seen: u64,
    pub(super) anchor: Option<Anchor>,
}

impl BlockStore {
    pub(crate) fn new() -> Self {
        Self {
            blocks: BTreeMap::new(),
            dead: BTreeSet::new(),
            created: HashSet::new(),
            conflict_until: None,
            session_floor: 0,
            max_slot_seen: 0,
            anchor: None,
        }
    }

    pub(crate) fn anchor_slot(&self) -> Option<u64> {
        self.anchor.as_ref().map(|a| a.confirmed_slot)
    }

    fn find(&self, slot: u64, hash: &str) -> Option<&Arc<SlotBlock>> {
        self.blocks
            .get(&slot)
            .filter(|block| block.blockhash == hash)
    }

    fn parent_of(&self, block: &SlotBlock) -> Option<&Arc<SlotBlock>> {
        if block.parent_slot >= block.slot {
            return None;
        }
        self.find(block.parent_slot, &block.parent_blockhash)
    }

    /// Starts a subscription session. Slots seen before it are ineligible until
    /// the confirmed slot passes them, and births are forgotten.
    pub(crate) fn new_session(&mut self) {
        self.session_floor = self.max_slot_seen;
        self.created.clear();
    }

    pub(crate) fn on_created_bank(&mut self, slot: u64, parent: Option<u64>) {
        self.max_slot_seen = self.max_slot_seen.max(slot);
        if !self.created.insert(slot) && self.dead.insert(slot) {
            tracing::info!(slot, ?parent, "processed slot restarted, marked dead");
        }
    }

    pub(crate) fn on_dead(&mut self, slot: u64) {
        self.max_slot_seen = self.max_slot_seen.max(slot);
        self.dead.insert(slot);
    }

    /// Stores a block. A replay of a stored block is ignored. A second
    /// blockhash for a stored slot, or a parent blockhash that does not match
    /// the stored parent, is a conflict above the confirmed slot. At or below
    /// it the stored block is the confirmed one and the newcomer is dropped.
    pub(crate) fn on_block(&mut self, block: SlotBlock) {
        self.max_slot_seen = self.max_slot_seen.max(block.slot);
        let confirmed = |slot: u64| self.anchor_slot().is_some_and(|a| slot <= a);
        if let Some(stored) = self.blocks.get(&block.slot) {
            if stored.blockhash != block.blockhash && !confirmed(block.slot) {
                self.conflict(block.slot, "second blockhash for slot");
            }
            return;
        }
        if let Some(parent) = self.blocks.get(&block.parent_slot)
            && parent.blockhash != block.parent_blockhash
        {
            if !confirmed(block.parent_slot) {
                self.conflict(block.parent_slot, "parent blockhash mismatch");
            }
            return;
        }
        self.blocks.insert(block.slot, Arc::new(block));
    }

    /// Deletes the block at `slot` and its descendants, and serves nothing
    /// until the confirmed slot passes `slot`.
    fn conflict(&mut self, slot: u64, cause: &str) {
        let mut doomed = HashSet::from([slot]);
        for (&child_slot, child) in self.blocks.range(slot + 1..) {
            if doomed.contains(&child.parent_slot) && self.parent_of(child).is_some() {
                doomed.insert(child_slot);
            }
        }
        for doomed_slot in &doomed {
            self.blocks.remove(doomed_slot);
        }
        self.conflict_until = Some(self.conflict_until.map_or(slot, |c| c.max(slot)));
        tracing::warn!(slot, cause, deleted = doomed.len(), "processed conflict");
    }

    /// Applies the Postgres confirmed slot and blockhash.
    pub(crate) fn set_anchor(&mut self, anchor: Anchor) {
        if let Some(previous) = self.anchor_slot()
            && let Some(confirmed) = self.find(anchor.confirmed_slot, &anchor.confirmed_blockhash)
        {
            let now = Instant::now();
            let newly_confirmed = std::iter::successors(Some(confirmed), |b| self.parent_of(b))
                .take_while(|block| block.slot > previous);
            for block in newly_confirmed {
                let latency = now.saturating_duration_since(block.received_at);
                metrics::PROCESSED_CONFIRM_LATENCY_MS.observe(latency.as_secs_f64() * 1_000.0);
            }
        }
        if self
            .conflict_until
            .is_some_and(|until| anchor.confirmed_slot > until)
        {
            self.conflict_until = None;
        }
        self.anchor = Some(anchor);
    }

    /// Drops slots below the retained window under the confirmed slot, poison
    /// entries at or below it, and the lowest slots over the cap above it.
    /// Before the first anchor the cap below the highest slot seen is the floor.
    pub(crate) fn prune(&mut self) {
        if let Some(a) = self.anchor_slot() {
            let floor = a.saturating_sub(RETAINED_SLOTS_BELOW_CONFIRMED);
            while self
                .blocks
                .first_key_value()
                .is_some_and(|(s, _)| *s < floor)
            {
                self.blocks.pop_first();
            }
        }
        let poison_floor = self.anchor_slot().unwrap_or_else(|| {
            self.max_slot_seen
                .saturating_sub(MAX_SLOTS_ABOVE_CONFIRMED as u64)
        });
        self.dead.retain(|slot| *slot > poison_floor);
        self.created.retain(|slot| *slot > poison_floor);
        let above = self.anchor_slot().map_or(0, |a| a + 1);
        while self.blocks.range(above..).count() > MAX_SLOTS_ABOVE_CONFIRMED {
            let lowest = *self.blocks.range(above..).next().map(|(s, _)| s).unwrap();
            self.blocks.remove(&lowest);
        }
    }

    /// A slot above the confirmed slot that must not be served: dead,
    /// restarted, without a birth in this session, or seen before this session.
    fn poisoned(&self, slot: u64) -> bool {
        self.dead.contains(&slot) || !self.created.contains(&slot) || slot <= self.session_floor
    }

    /// The blocks from the highest stored slot, when its walk reaches the
    /// confirmed slot and blockhash with no poisoned slot on the way.
    pub(crate) fn latest_chained_blocks(&self) -> Option<ProcessedBlocks> {
        let anchor = self.anchor.as_ref()?;
        let a = anchor.confirmed_slot;
        if self.conflict_until.is_some_and(|until| a <= until) {
            return None;
        }
        let head = self.blocks.values().next_back()?;
        let block_time = head.block_time?;

        let mut blocks = Vec::new();
        let mut block = head;
        while block.slot > a {
            if self.poisoned(block.slot) {
                return None;
            }
            blocks.push(block.clone());
            if block.parent_slot == a {
                if block.parent_blockhash != anchor.confirmed_blockhash {
                    return None;
                }
                break;
            }
            block = self.parent_of(block)?;
        }
        // The confirmed block and its stored parents extend the chain below the anchor.
        let at_anchor = match block.slot.cmp(&a) {
            Ordering::Greater => self.parent_of(block),
            Ordering::Equal => Some(self.find(a, &anchor.confirmed_blockhash)?),
            Ordering::Less => return None,
        };
        blocks.extend(std::iter::successors(at_anchor, |b| self.parent_of(b)).cloned());

        Some(ProcessedBlocks {
            slot: head.slot,
            block_time,
            anchor_slot: a,
            blocks,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::ingest::tests::{account_info, update};
    use super::*;
    use crate::config::AccountSelectorConfig;
    use solana_pubkey::Pubkey;

    pub(crate) fn hash(slot: u64) -> String {
        format!("h{slot}")
    }

    pub(crate) fn anchor_at(slot: u64) -> Anchor {
        anchor_with(slot, &hash(slot))
    }

    pub(crate) fn anchor_with(slot: u64, blockhash: &str) -> Anchor {
        Anchor {
            confirmed_slot: slot,
            confirmed_blockhash: blockhash.to_string(),
        }
    }

    /// Drives a store with blocks built through ingest. Hashes are named `h<slot>`.
    pub(crate) struct TestChain {
        pub store: BlockStore,
        pub filter: AccountSelectorConfig,
        owner: Pubkey,
    }

    impl TestChain {
        pub fn new() -> Self {
            let mut store = BlockStore::new();
            store.new_session();
            Self {
                store,
                filter: AccountSelectorConfig::default(),
                owner: Pubkey::new_unique(),
            }
        }

        fn birth(&mut self, slot: u64, parent: u64) {
            if !self.store.created.contains(&slot) {
                self.store.on_created_bank(slot, Some(parent));
            }
        }

        /// Birth plus block, every account owned by the chain's owner.
        pub fn block_with(&mut self, slot: u64, parent: u64, accounts: Vec<(Pubkey, u64)>) {
            let owner = self.owner;
            let accounts = accounts.into_iter().map(|(k, l)| (k, owner, l)).collect();
            self.block_owned(slot, parent, accounts);
        }

        pub fn block_owned(
            &mut self,
            slot: u64,
            parent: u64,
            accounts: Vec<(Pubkey, Pubkey, u64)>,
        ) {
            self.birth(slot, parent);
            let infos = accounts
                .into_iter()
                .map(|(key, owner, lamports)| account_info(key, owner, lamports, vec![0; 8]))
                .collect();
            self.raw(update(slot, &hash(slot), parent, &hash(parent), infos));
        }

        pub fn block_without_time(&mut self, slot: u64, parent: u64) {
            self.birth(slot, parent);
            let mut block = update(slot, &hash(slot), parent, &hash(parent), vec![]);
            block.block_time = None;
            self.raw(block);
        }

        pub fn raw(&mut self, block: yellowstone_grpc_proto::prelude::SubscribeUpdateBlock) {
            let built = SlotBlock::from_update(block, &self.filter, Instant::now());
            self.store.on_block(built);
        }

        /// Birth plus an empty block for every slot in `from..=to`, each on the previous slot.
        pub fn linear(&mut self, from: u64, to: u64) {
            for slot in from..=to {
                self.block_with(slot, slot - 1, vec![]);
            }
        }

        /// Birth plus an empty block with explicit hashes.
        pub fn fork(&mut self, slot: u64, hash: &str, parent: u64, parent_hash: &str) {
            self.birth(slot, parent);
            self.raw(update(slot, hash, parent, parent_hash, vec![]));
        }

        pub fn event(&mut self) -> Option<ProcessedBlocks> {
            self.store.prune();
            self.store.latest_chained_blocks()
        }

        pub fn head(&mut self) -> Option<u64> {
            self.event().map(|blocks| blocks.slot)
        }

        pub fn slots(&self) -> Vec<u64> {
            self.store.blocks.keys().copied().collect()
        }
    }

    #[test]
    fn linear_chain_serves_the_highest_slot_down_to_the_confirmed_block() {
        let mut chain = TestChain::new();
        chain.linear(100, 105);
        chain.store.set_anchor(anchor_at(100));
        let latest = chain.event().unwrap();
        assert_eq!(latest.slot, 105);
        assert_eq!(latest.anchor_slot, 100);
        assert_eq!(latest.block_time, 1_700_000_105);
        // Five blocks above the anchor plus the confirmed block itself.
        assert_eq!(latest.blocks.len(), 6);
        assert_eq!(latest.blocks.last().unwrap().slot, 100);
    }

    #[test]
    fn chain_links_to_the_anchor_by_parent_hash_when_the_confirmed_block_is_absent() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        chain.store.set_anchor(anchor_at(100));
        let latest = chain.event().unwrap();
        assert_eq!(latest.slot, 103);
        assert_eq!(latest.blocks.len(), 3);

        // A different confirmed blockhash breaks the walk.
        chain.store.set_anchor(anchor_with(100, "other100"));
        assert_eq!(chain.head(), None);
    }

    #[test]
    fn broken_walk_serves_nothing_until_the_confirmed_slot_passes_the_gap() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.linear(105, 106);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(104));
        assert_eq!(chain.head(), Some(106));
    }

    #[test]
    fn highest_slot_on_a_losing_fork_serves_nothing() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.fork(103, "x103", 100, "other100");
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), None);
        // Once a higher block on the confirmed chain arrives it is the head again.
        chain.block_with(104, 102, vec![]);
        assert_eq!(chain.head(), Some(104));
    }

    #[test]
    fn head_at_or_below_the_confirmed_slot() {
        let mut chain = TestChain::new();
        chain.linear(100, 101);
        chain.store.set_anchor(anchor_at(101));
        assert_eq!(chain.head(), Some(101));
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_with(101, "other101"));
        assert_eq!(chain.head(), None);
    }

    #[test]
    fn no_blocks_before_the_first_anchor() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Some(103));
    }

    #[test]
    fn second_blockhash_deletes_the_slot_and_descendants_until_confirmed_passes_it() {
        let mut chain = TestChain::new();
        chain.linear(101, 104);
        chain.fork(105, "y105", 103, "h103");
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Some(105));

        chain.fork(102, "other102", 101, "h101");
        assert_eq!(chain.store.conflict_until, Some(102));
        // 102 and its descendants 103, 104 and 105 are gone. 101 stays.
        assert_eq!(chain.slots(), vec![101]);
        assert_eq!(chain.head(), None);

        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.store.conflict_until, Some(102));
        assert_eq!(chain.head(), None);
        chain.linear(103, 104);
        chain.store.set_anchor(anchor_at(103));
        assert_eq!(chain.store.conflict_until, None);
        assert_eq!(chain.head(), Some(104));
    }

    #[test]
    fn parent_mismatch_deletes_the_stored_parent_and_its_descendants() {
        let mut chain = TestChain::new();
        chain.linear(101, 104);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Some(104));

        chain.fork(105, "h105", 103, "r103");
        assert_eq!(chain.store.conflict_until, Some(103));
        assert_eq!(chain.slots(), vec![101, 102]);
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(103));
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(104));
        assert_eq!(chain.store.conflict_until, None);
    }

    #[test]
    fn newcomer_conflicting_at_or_below_the_confirmed_slot_is_dropped() {
        let mut chain = TestChain::new();
        chain.linear(99, 103);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Some(103));

        chain.fork(100, "other100", 99, "h99");
        chain.fork(99, "other99", 98, "h98");
        chain.fork(104, "x104", 100, "other100");
        assert_eq!(chain.store.conflict_until, None);
        assert_eq!(chain.slots(), vec![99, 100, 101, 102, 103]);
        assert_eq!(chain.head(), Some(103));
    }

    #[test]
    fn replay_of_a_stored_block_is_not_a_conflict() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.store.set_anchor(anchor_at(100));
        chain.linear(102, 102);
        assert_eq!(chain.store.conflict_until, None);
        assert_eq!(chain.head(), Some(102));
    }

    #[test]
    fn retention_keeps_a_window_below_the_confirmed_slot() {
        let mut chain = TestChain::new();
        chain.linear(90, 105);
        chain.store.on_dead(93);
        chain.store.set_anchor(anchor_at(100));
        chain.store.prune();
        let floor = 100 - RETAINED_SLOTS_BELOW_CONFIRMED;
        assert_eq!(chain.slots(), (floor..=105).collect::<Vec<_>>());
        assert!(chain.store.dead.is_empty());
        assert!(chain.store.created.iter().all(|s| *s > 100));
        let latest = chain.event().unwrap();
        assert_eq!(latest.blocks.last().unwrap().slot, floor);
    }

    #[test]
    fn slot_cap_evicts_the_lowest_slot_above_the_confirmed_slot() {
        let cap = MAX_SLOTS_ABOVE_CONFIRMED as u64;
        let mut chain = TestChain::new();
        chain.linear(101, 100 + cap + 2);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), None);
        assert_eq!(chain.slots().first(), Some(&103));
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Some(100 + cap + 2));

        // Before the first anchor the cap bounds the whole store and the poison sets.
        let mut chain = TestChain::new();
        chain.linear(101, 100 + cap + 1);
        chain.store.on_dead(101);
        chain.store.prune();
        assert_eq!(chain.slots().len(), MAX_SLOTS_ABOVE_CONFIRMED);
        assert!(chain.store.dead.is_empty());
        assert!(chain.store.created.iter().all(|s| *s > 101));
    }

    #[test]
    fn head_without_block_time_serves_nothing() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.block_without_time(103, 102);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), None);
        chain.linear(104, 104);
        assert_eq!(chain.head(), Some(104));
    }

    #[test]
    fn dead_slot_on_the_walk_serves_nothing_until_confirmed_passes_it() {
        let mut chain = TestChain::new();
        chain.linear(101, 104);
        chain.store.set_anchor(anchor_at(100));
        chain.store.on_dead(102);
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Some(104));
    }

    #[test]
    fn repeated_bank_creation_marks_the_slot_dead() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Some(103));
        chain.store.on_created_bank(103, Some(102));
        assert!(chain.store.dead.contains(&103));
        assert_eq!(chain.head(), None);
    }

    #[test]
    fn missing_birth_and_session_floor_are_ineligible() {
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.raw(update(102, "h102", 101, "h101", vec![]));
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), None);
        chain.store.on_created_bank(102, Some(101));
        assert_eq!(chain.head(), Some(102));

        chain.store.new_session();
        assert_eq!(chain.store.session_floor, 102);
        chain.block_with(103, 102, vec![]);
        chain.store.on_created_bank(101, Some(100));
        chain.store.on_created_bank(102, Some(101));
        assert_eq!(chain.head(), None);
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Some(103));
    }
}
