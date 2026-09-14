// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The block store state machine. Pure and synchronous: it takes events, keeps
//! blocks per slot with one entry per blockhash, and selects the view. The only
//! clocks it reads are the `Instant`s stored on blocks and anchors.
//!
//! Within one event a block is identified by its `Arc` address, so walks follow
//! parents without cloning blockhashes. `prune` computes the links once per
//! event and passes them to [`BlockStore::select_view_with`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::ingest::SlotBlock;
use super::prune::Evicted;
use super::read::ProcessedView;
use super::{Anchor, DegradeReason, Published};
use crate::metrics;

/// How a stored block relates to the anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Link {
    /// Walks through stored parents to the anchor slot and blockhash.
    Linked,
    /// Proven not to descend from the anchor.
    OffRoot,
    /// A parent above the anchor is not stored yet.
    Unlinked,
}

/// Link state per block id, valid for the blocks stored when it was computed.
pub(super) type Links = HashMap<usize, Link>;

/// Per block id: the walk down to the anchor has no poisoned slot, and it passes the root.
pub(super) type ChainMemo = HashMap<usize, (bool, bool)>;

pub(super) fn block_id(block: &Arc<SlotBlock>) -> usize {
    Arc::as_ptr(block) as usize
}

/// The stream's confirmed block and the ids of its chain down to the anchor.
pub(super) struct RootLine<'a> {
    root: &'a Arc<SlotBlock>,
    chain: HashSet<usize>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StoreLimits {
    pub max_servable_depth: u64,
    pub max_overlay_slots: u64,
    pub max_memory_bytes: usize,
}

pub(crate) struct BlockStore {
    pub(super) limits: StoreLimits,
    pub(super) live_bytes: Arc<AtomicUsize>,
    /// Bytes of evicted blocks that free once the dropper releases them.
    pub(super) pending_drop_bytes: Arc<AtomicUsize>,
    /// Stored blocks per slot, one per blockhash, in arrival order. Never an empty `Vec`.
    pub(super) by_slot: BTreeMap<u64, Vec<Arc<SlotBlock>>>,
    pub(super) dead: BTreeSet<u64>,
    /// Slots with a `SLOT_CREATED_BANK` in this session.
    pub(super) created: HashSet<u64>,
    pub(super) conflict_until: Option<u64>,
    pub(super) session_floor: u64,
    pub(super) max_slot_seen: u64,
    pub(super) grpc_confirmed: u64,
    pub(super) anchor: Option<Anchor>,
    /// The last published view. Evictions use it to tell which blocks only it pins.
    pub(super) last_view: Option<Arc<ProcessedView>>,
    /// Removed blocks, drained by the caller into the dropper.
    pub(super) evicted: Vec<Evicted>,
}

impl BlockStore {
    pub(crate) fn new(
        limits: StoreLimits,
        live_bytes: Arc<AtomicUsize>,
        pending_drop_bytes: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            limits,
            live_bytes,
            pending_drop_bytes,
            by_slot: BTreeMap::new(),
            dead: BTreeSet::new(),
            created: HashSet::new(),
            conflict_until: None,
            session_floor: 0,
            max_slot_seen: 0,
            grpc_confirmed: 0,
            anchor: None,
            last_view: None,
            evicted: Vec::new(),
        }
    }

    pub(crate) fn anchor_slot(&self) -> Option<u64> {
        self.anchor.as_ref().map(|a| a.confirmed_slot)
    }

    pub(crate) fn block_count(&self) -> usize {
        self.by_slot.values().map(Vec::len).sum()
    }

    pub(crate) fn take_evicted(&mut self) -> Vec<Evicted> {
        std::mem::take(&mut self.evicted)
    }

    pub(super) fn blocks(&self) -> impl Iterator<Item = &Arc<SlotBlock>> {
        self.by_slot.values().flatten()
    }

    pub(super) fn find(&self, slot: u64, hash: &str) -> Option<&Arc<SlotBlock>> {
        self.by_slot
            .get(&slot)?
            .iter()
            .find(|block| block.blockhash == hash)
    }

    fn parent_of(&self, block: &SlotBlock) -> Option<&Arc<SlotBlock>> {
        if block.parent_slot >= block.slot {
            return None;
        }
        self.find(block.parent_slot, &block.parent_blockhash)
    }

    /// `head` and its stored ancestors above slot `above`, newest first.
    fn ancestors<'a>(
        &'a self,
        head: &'a Arc<SlotBlock>,
        above: u64,
    ) -> impl Iterator<Item = &'a Arc<SlotBlock>> {
        std::iter::successors(Some(head), |block| self.parent_of(block))
            .take_while(move |block| block.slot > above)
    }

    /// Starts a subscription session. Slots seen before it are ineligible until
    /// the anchor passes them, and births are forgotten.
    pub(crate) fn new_session(&mut self) {
        self.session_floor = self.max_slot_seen;
        self.created.clear();
    }

    pub(crate) fn on_created_bank(&mut self, slot: u64, parent: Option<u64>) {
        self.max_slot_seen = self.max_slot_seen.max(slot);
        if !self.created.insert(slot) && self.dead.insert(slot) {
            metrics::PROCESSED_RESTARTED_SLOTS_TOTAL.inc();
            tracing::info!(slot, ?parent, "processed slot restarted, marked dead");
        }
    }

    pub(crate) fn on_dead(&mut self, slot: u64) {
        self.max_slot_seen = self.max_slot_seen.max(slot);
        if self.dead.insert(slot) {
            metrics::PROCESSED_DEAD_SLOTS_TOTAL.inc();
        }
    }

    pub(crate) fn on_confirmed(&mut self, slot: u64) {
        self.grpc_confirmed = self.grpc_confirmed.max(slot);
    }

    pub(crate) fn on_block(&mut self, block: SlotBlock) {
        self.max_slot_seen = self.max_slot_seen.max(block.slot);
        let block = Arc::new(block);

        let at_or_below_anchor = self.anchor_slot().is_some_and(|a| block.slot <= a);
        if at_or_below_anchor || self.find(block.slot, &block.blockhash).is_some() {
            self.push_evicted(block, None);
            return;
        }

        if self.by_slot.contains_key(&block.slot) {
            self.latch(block.slot, "second blockhash for slot");
        }
        if self.by_slot.get(&block.parent_slot).is_some_and(|parents| {
            !parents
                .iter()
                .any(|p| p.blockhash == block.parent_blockhash)
        }) {
            self.latch(block.parent_slot, "parent blockhash mismatch");
        }
        let mismatched_child = self
            .by_slot
            .range((Bound::Excluded(block.slot), Bound::Unbounded))
            .flat_map(|(_, children)| children)
            .any(|child| {
                child.parent_slot == block.slot && child.parent_blockhash != block.blockhash
            });
        if mismatched_child {
            self.latch(block.slot, "stored child names another parent blockhash");
        }

        if block.bytes > self.limits.max_memory_bytes {
            tracing::warn!(
                slot = block.slot,
                bytes = block.bytes,
                "processed block exceeds the memory cap, not stored"
            );
            self.push_evicted(block, Some("memory_cap"));
            return;
        }

        self.by_slot.entry(block.slot).or_default().push(block);
    }

    fn latch(&mut self, slot: u64, cause: &str) {
        self.conflict_until = Some(self.conflict_until.map_or(slot, |c| c.max(slot)));
        metrics::PROCESSED_CONFLICTS_TOTAL.inc();
        tracing::warn!(slot, cause, "processed conflict latch set");
    }

    /// Applies an anchor. A lower confirmed slot applies only its unhealthy flag.
    pub(crate) fn set_anchor(&mut self, anchor: Anchor) {
        if let Some(current) = self.anchor.as_mut()
            && anchor.confirmed_slot < current.confirmed_slot
        {
            tracing::warn!(
                current = current.confirmed_slot,
                received = anchor.confirmed_slot,
                healthy = anchor.healthy,
                "processed anchor regressed, only an unhealthy flag applies"
            );
            if !anchor.healthy {
                current.healthy = false;
                current.polled_at = anchor.polled_at;
            }
            return;
        }

        if let Some(previous_slot) = self.anchor_slot()
            && let Some(confirmed) = self.find(anchor.confirmed_slot, &anchor.confirmed_blockhash)
        {
            for block in self.ancestors(confirmed, previous_slot) {
                let latency = anchor
                    .polled_at
                    .saturating_duration_since(block.received_at);
                metrics::PROCESSED_CONFIRM_LATENCY_MS.observe(latency.as_secs_f64() * 1_000.0);
            }
        }

        if self
            .conflict_until
            .is_some_and(|until| anchor.confirmed_slot >= until)
        {
            self.conflict_until = None;
        }
        self.anchor = Some(anchor);
    }

    /// Link state of every stored block, memoized along each walk.
    pub(super) fn links(&self) -> Links {
        let Some(anchor) = &self.anchor else {
            return self
                .blocks()
                .map(|block| (block_id(block), Link::Unlinked))
                .collect();
        };
        let a = anchor.confirmed_slot;
        let mut memo = Links::with_capacity(self.block_count());

        for start in self.blocks() {
            let mut path = Vec::new();
            let mut block = start;
            let result = loop {
                if let Some(link) = memo.get(&block_id(block)) {
                    break *link;
                }
                path.push(block_id(block));
                if block.parent_slot >= block.slot || block.parent_slot < a {
                    break Link::OffRoot;
                }
                if block.parent_slot == a {
                    break if block.parent_blockhash == anchor.confirmed_blockhash {
                        Link::Linked
                    } else {
                        Link::OffRoot
                    };
                }
                match self.find(block.parent_slot, &block.parent_blockhash) {
                    Some(parent) => block = parent,
                    None => break Link::Unlinked,
                }
            };
            for id in path {
                memo.insert(id, result);
            }
        }
        memo
    }

    /// The stream's confirmed block, when it is above the anchor, the only
    /// stored version of its slot, linked, and no latch is set.
    pub(super) fn root_line(&self, links: &Links) -> Option<RootLine<'_>> {
        let a = self.anchor_slot()?;
        if self.grpc_confirmed <= a || self.conflict_until.is_some() {
            return None;
        }
        let [root] = self.by_slot.get(&self.grpc_confirmed)?.as_slice() else {
            return None;
        };
        (links.get(&block_id(root)) == Some(&Link::Linked)).then(|| RootLine {
            root,
            chain: self.ancestors(root, a).map(block_id).collect(),
        })
    }

    /// Whether the walk from `head` to the anchor is free of poisoned slots, and
    /// whether `head` is on the root line: the root, a descendant or an ancestor.
    /// With no root every head is on the line. `memo` must belong to one `line`.
    pub(super) fn chain_status(
        &self,
        head: &Arc<SlotBlock>,
        line: Option<&RootLine<'_>>,
        a: u64,
        memo: &mut ChainMemo,
    ) -> (bool, bool) {
        let mut path = Vec::new();
        let mut state = (true, false);
        for block in self.ancestors(head, a) {
            if let Some(known) = memo.get(&block_id(block)) {
                state = *known;
                break;
            }
            path.push(block);
        }
        for block in path.into_iter().rev() {
            let clean = state.0
                && !self.dead.contains(&block.slot)
                && self.created.contains(&block.slot)
                && block.slot > self.session_floor;
            let through_root = state.1 || line.is_some_and(|l| Arc::ptr_eq(l.root, block));
            state = (clean, through_root);
            memo.insert(block_id(block), state);
        }
        let on_line = line.is_none_or(|l| state.1 || l.chain.contains(&block_id(head)));
        (state.0, on_line)
    }

    #[cfg(test)]
    pub(crate) fn select_view(&self) -> Result<ProcessedView, DegradeReason> {
        self.select_view_with(&self.links())
    }

    /// Selects the head. `links` must match the blocks currently stored.
    pub(crate) fn select_view_with(&self, links: &Links) -> Result<ProcessedView, DegradeReason> {
        let anchor = self.anchor.as_ref().ok_or(DegradeReason::NotWarm)?;
        let a = anchor.confirmed_slot;
        if !anchor.healthy {
            return Err(DegradeReason::Unhealthy);
        }
        if anchor.finalized_slot > a {
            return Err(DegradeReason::FinalizedAboveAnchor);
        }
        if self.conflict_until.is_some_and(|until| a < until) {
            return Err(DegradeReason::Conflict);
        }
        let above = (Bound::Excluded(a), Bound::Unbounded);
        if self.by_slot.range(above).next().is_none() {
            return Err(DegradeReason::HeadNotAhead);
        }

        let line = self.root_line(links);
        let mut memo = ChainMemo::new();
        let mut beyond_depth = false;

        for (&slot, blocks) in self.by_slot.range(above).rev() {
            let too_deep = slot - a > self.limits.max_servable_depth;
            if too_deep && beyond_depth {
                continue;
            }
            for head in blocks {
                if links.get(&block_id(head)) != Some(&Link::Linked) {
                    continue;
                }
                let Some(block_time) = head.block_time else {
                    continue;
                };
                let (clean, on_line) = self.chain_status(head, line.as_ref(), a, &mut memo);
                if !clean || !on_line {
                    continue;
                }
                if too_deep {
                    beyond_depth = true;
                    break;
                }
                return Ok(ProcessedView {
                    slot,
                    block_time,
                    anchor_slot: a,
                    chain: self.ancestors(head, a).cloned().collect(),
                    anchor_polled_at: anchor.polled_at,
                });
            }
        }

        Err(if beyond_depth {
            DegradeReason::TooDeep
        } else {
            DegradeReason::Unlinked
        })
    }

    /// Records a publish, updates the store metrics and returns the regression cause, if any.
    pub(crate) fn observe_publish(&mut self, result: &Published) -> Option<&'static str> {
        let previous = match result {
            Ok(view) => self.last_view.replace(view.clone()),
            Err(_) => self.last_view.take(),
        };
        let cause = match (result, &previous) {
            (Ok(view), Some(previous)) if view.slot < previous.slot => Some("fork_switch"),
            (Err(_), Some(_)) => Some("degrade"),
            _ => None,
        };
        if let Ok(view) = result {
            let depth_key = |v: &ProcessedView| (v.slot, v.anchor_slot);
            if previous.as_deref().map(depth_key) != Some(depth_key(view)) {
                metrics::PROCESSED_DEPTH_SLOTS.observe((view.slot - view.anchor_slot) as f64);
            }
            if let Some(head) = view.chain.first() {
                metrics::PROCESSED_HEAD_AGE_MS.set(head.received_at.elapsed().as_millis() as i64);
            }
            metrics::PROCESSED_HEAD_SLOT.set(view.slot as i64);
        }
        if let Some(cause) = cause {
            metrics::PROCESSED_HEAD_REGRESSIONS_TOTAL
                .with_label_values(&[cause])
                .inc();
        }
        metrics::PROCESSED_LIVE_BYTES.set(self.live_bytes.load(Ordering::Relaxed) as i64);
        metrics::PROCESSED_STORE_BLOCKS.set(self.block_count() as i64);
        if let Some(a) = self.anchor_slot() {
            metrics::PROCESSED_ANCHOR_SLOT.set(a as i64);
        }
        cause
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::Lookup;
    use super::super::ingest::tests::{account_info, update};
    use super::*;
    use crate::config::AccountSelectorConfig;
    use solana_pubkey::Pubkey;
    use std::time::Instant;

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
            finalized_slot: slot.saturating_sub(32),
            healthy: true,
            polled_at: Instant::now(),
        }
    }

    pub(crate) const TEST_LIMITS: StoreLimits = StoreLimits {
        max_servable_depth: 16,
        max_overlay_slots: 64,
        max_memory_bytes: 1 << 30,
    };

    /// Drives a store with blocks built through ingest. Hashes are named `h<slot>`.
    pub(crate) struct TestChain {
        pub store: BlockStore,
        pub live_bytes: Arc<AtomicUsize>,
        pub filter: AccountSelectorConfig,
        owner: Pubkey,
    }

    impl TestChain {
        pub fn new() -> Self {
            Self::with_live_bytes(Arc::new(AtomicUsize::new(0)))
        }

        pub fn with_limits(limits: StoreLimits) -> Self {
            let mut chain = Self::new();
            chain.store.limits = limits;
            chain
        }

        pub fn with_live_bytes(live_bytes: Arc<AtomicUsize>) -> Self {
            let mut store = BlockStore::new(
                TEST_LIMITS,
                live_bytes.clone(),
                Arc::new(AtomicUsize::new(0)),
            );
            store.new_session();
            Self {
                store,
                live_bytes,
                filter: AccountSelectorConfig::default(),
                owner: Pubkey::new_unique(),
            }
        }

        pub fn live(&self) -> usize {
            self.live_bytes.load(Ordering::Relaxed)
        }

        pub fn pending(&self) -> usize {
            self.store.pending_drop_bytes.load(Ordering::Relaxed)
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
            let built =
                SlotBlock::from_update(block, &self.filter, &self.live_bytes, Instant::now());
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

        pub fn event(&mut self) -> Result<ProcessedView, DegradeReason> {
            let links = self.store.prune();
            let result = self.store.select_view_with(&links);
            drop(self.store.take_evicted());
            result
        }

        pub fn head(&mut self) -> Result<u64, DegradeReason> {
            self.event().map(|view| view.slot)
        }

        pub fn stored(&self, slot: u64, hash: &str) -> bool {
            self.store.find(slot, hash).is_some()
        }
    }

    fn lamports(lookup: Lookup<'_>) -> Option<u64> {
        match lookup {
            Lookup::Live(account) => Some(account.lamports),
            _ => None,
        }
    }

    #[test]
    fn linear_chain_serves_highest_block() {
        let mut chain = TestChain::new();
        chain.linear(101, 105);
        chain.store.set_anchor(anchor_at(100));
        let view = chain.event().unwrap();
        assert_eq!(view.slot, 105);
        assert_eq!(view.anchor_slot, 100);
        assert_eq!(view.block_time, 1_700_000_105);
        assert_eq!(view.chain.len(), 5);
    }

    #[test]
    fn confirmed_fork_wins_and_the_losing_tip_is_pruned() {
        let key = Pubkey::new_unique();
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.block_with(102, 101, vec![(key, 9)]);
        chain.block_with(103, 101, vec![(key, 1)]);
        chain.block_with(104, 102, vec![]);
        chain.store.set_anchor(anchor_at(100));
        let view = chain.event().unwrap();
        assert_eq!(view.slot, 104);
        assert_eq!(lamports(view.lookup(&key)), Some(9));

        chain.store.on_confirmed(103);
        let view = chain.event().unwrap();
        assert_eq!(view.slot, 103);
        assert_eq!(lamports(view.lookup(&key)), Some(1));
        assert!(!chain.stored(102, "h102"));
        assert!(!chain.stored(104, "h104"));
        assert!(chain.stored(103, "h103"));
    }

    #[test]
    fn branch_off_the_anchor_is_never_head_and_is_pruned() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.fork(103, "x103", 100, "other100");
        chain.fork(104, "x104", 103, "x103");
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Ok(102));
        assert!(!chain.stored(103, "x103"));
        assert!(!chain.stored(104, "x104"));

        let mut chain = TestChain::new();
        chain.fork(101, "x101", 99, "h99");
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Err(DegradeReason::HeadNotAhead));
        assert_eq!(chain.store.block_count(), 0);
    }

    #[test]
    fn warm_start_keeps_unlinked_blocks_until_anchor_reaches_parent() {
        let mut chain = TestChain::new();
        chain.linear(103, 106);
        assert_eq!(chain.head(), Err(DegradeReason::NotWarm));
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Err(DegradeReason::Unlinked));
        assert_eq!(chain.store.block_count(), 4);
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Ok(106));
    }

    #[test]
    fn gap_is_unlinked_until_anchor_passes_it() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.linear(105, 106);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Ok(102));
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Err(DegradeReason::Unlinked));
        chain.store.set_anchor(anchor_at(104));
        assert_eq!(chain.head(), Ok(106));
    }

    #[test]
    fn parent_blockhash_mismatch_latches_until_anchor_passes() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Ok(103));
        // 104 descends from a replacement 103, so stored 103 and 102 may be dumped.
        chain.fork(104, "h104", 103, "r103");
        assert_eq!(chain.store.conflict_until, Some(103));
        assert_eq!(chain.head(), Err(DegradeReason::Conflict));
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Err(DegradeReason::Conflict));

        chain.store.set_anchor(anchor_with(103, "r103"));
        assert_eq!(chain.store.conflict_until, None);
        assert_eq!(chain.head(), Ok(104));
        assert!(!chain.stored(103, "h103"));

        // A child stored before its mismatching parent latches too.
        let mut chain = TestChain::new();
        chain.fork(103, "h103", 102, "r102");
        chain.linear(101, 102);
        assert_eq!(chain.store.conflict_until, Some(102));
    }

    #[test]
    fn two_hashes_for_one_slot_latch_and_replay_is_idempotent() {
        let mut chain = TestChain::new();
        chain.linear(101, 102);
        chain.store.set_anchor(anchor_at(100));
        chain.linear(102, 102);
        assert_eq!(chain.store.conflict_until, None);
        assert_eq!(chain.store.by_slot[&102].len(), 1);
        assert_eq!(chain.head(), Ok(102));

        chain.fork(102, "other102", 101, "h101");
        assert_eq!(chain.store.conflict_until, Some(102));
        assert_eq!(chain.head(), Err(DegradeReason::Conflict));
    }

    #[test]
    fn stream_confirmed_root_needs_a_single_version() {
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.fork(102, "a102", 101, "h101");
        chain.store.set_anchor(anchor_at(100));
        chain.store.on_confirmed(102);
        let links = chain.store.links();
        let line = chain.store.root_line(&links);
        assert!(line.is_some_and(|line| line.root.blockhash == "a102"));

        chain.fork(102, "b102", 101, "h101");
        assert_eq!(chain.store.conflict_until, Some(102));
        // Clear the latch so only the single-version rule is under test.
        chain.store.conflict_until = None;
        let links = chain.store.links();
        assert!(chain.store.root_line(&links).is_none());
    }

    #[test]
    fn block_without_block_time_links_but_is_not_head() {
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.block_without_time(102, 101);
        chain.linear(103, 103);
        chain.store.set_anchor(anchor_at(100));
        chain.store.on_confirmed(102);
        assert!(chain.store.root_line(&chain.store.links()).is_some());
        assert_eq!(chain.head(), Ok(103));
        // With 103 dead and the root 102 untimed, its ancestor 101 is served.
        chain.store.on_dead(103);
        assert_eq!(chain.head(), Ok(101));
    }

    #[test]
    fn dead_slot_poisons_descendants() {
        let mut chain = TestChain::new();
        chain.linear(101, 104);
        chain.store.set_anchor(anchor_at(100));
        chain.store.on_dead(102);
        assert_eq!(chain.head(), Ok(101));
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Ok(104));
    }

    #[test]
    fn repeated_bank_creation_marks_dead_with_same_or_changed_parent() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Ok(103));

        chain.store.on_created_bank(103, Some(102));
        assert!(chain.store.dead.contains(&103));
        assert_eq!(chain.head(), Ok(102));

        chain.store.on_created_bank(102, Some(100));
        assert!(chain.store.dead.contains(&102));
        assert_eq!(chain.head(), Ok(101));
    }

    #[test]
    fn missing_birth_and_session_floor_are_ineligible() {
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.raw(update(102, "h102", 101, "h101", vec![]));
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Ok(101));

        chain.store.new_session();
        assert_eq!(chain.store.session_floor, 102);
        chain.block_with(103, 102, vec![]);
        chain.store.on_created_bank(101, Some(100));
        chain.store.on_created_bank(102, Some(101));
        assert_eq!(chain.head(), Err(DegradeReason::Unlinked));
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Ok(103));
    }

    #[test]
    fn degrade_reasons() {
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.store.set_anchor(anchor_at(101));
        assert_eq!(chain.head(), Err(DegradeReason::HeadNotAhead));

        let mut chain = TestChain::with_limits(StoreLimits {
            max_servable_depth: 2,
            ..TEST_LIMITS
        });
        chain.block_without_time(101, 100);
        chain.block_without_time(102, 101);
        chain.linear(103, 104);
        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Err(DegradeReason::TooDeep));
        chain.store.set_anchor(anchor_at(101));
        assert_eq!(chain.head(), Ok(103));

        let mut chain = TestChain::new();
        chain.linear(101, 101);
        let mut unhealthy = anchor_at(100);
        unhealthy.healthy = false;
        chain.store.set_anchor(unhealthy);
        assert_eq!(chain.head(), Err(DegradeReason::Unhealthy));

        let mut above = anchor_at(100);
        above.finalized_slot = 101;
        chain.store.set_anchor(above);
        assert_eq!(chain.head(), Err(DegradeReason::FinalizedAboveAnchor));
    }

    #[test]
    fn anchor_regression_applies_only_unhealthy() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        chain.store.set_anchor(anchor_at(101));
        assert_eq!(chain.head(), Ok(103));

        let mut regressed = anchor_at(100);
        regressed.healthy = false;
        chain.store.set_anchor(regressed.clone());
        let current = chain.store.anchor.as_ref().unwrap();
        assert_eq!(current.confirmed_slot, 101);
        assert_eq!(current.polled_at, regressed.polled_at);
        assert_eq!(chain.head(), Err(DegradeReason::Unhealthy));

        chain.store.set_anchor(anchor_at(100));
        assert_eq!(chain.head(), Err(DegradeReason::Unhealthy));
        chain.store.set_anchor(anchor_at(101));
        assert_eq!(chain.head(), Ok(103));
    }

    #[test]
    fn same_slot_anchor_refreshes_polled_at_and_keeps_triple() {
        let mut chain = TestChain::new();
        chain.linear(101, 103);
        let first = anchor_at(100);
        chain.store.set_anchor(first.clone());
        assert_eq!(chain.head(), Ok(103));
        let republished = Anchor {
            polled_at: Instant::now(),
            ..first.clone()
        };
        chain.store.set_anchor(republished.clone());
        let view = chain.event().unwrap();
        assert_eq!(view.anchor_slot, 100);
        assert_eq!(
            chain.store.anchor.as_ref().unwrap().finalized_slot,
            first.finalized_slot
        );
        assert_eq!(view.anchor_polled_at, republished.polled_at);
    }

    #[test]
    fn head_regression_causes() {
        fn publish(chain: &mut TestChain) -> Option<&'static str> {
            let result = chain.event().map(Arc::new);
            chain.store.observe_publish(&result)
        }
        let forked = || {
            let mut chain = TestChain::new();
            chain.linear(101, 101);
            chain.fork(102, "a102", 101, "h101");
            chain.fork(103, "b103", 101, "h101");
            chain.fork(104, "a104", 102, "a102");
            chain.store.set_anchor(anchor_at(100));
            chain
        };

        let mut chain = forked();
        assert_eq!(publish(&mut chain), None);
        chain.store.on_confirmed(103);
        assert_eq!(publish(&mut chain), Some("fork_switch"));
        assert_eq!(publish(&mut chain), None);

        // A degrade counts once, and the next lower view is not a fork switch.
        let mut chain = forked();
        assert_eq!(publish(&mut chain), None);
        let mut unhealthy = anchor_at(100);
        unhealthy.healthy = false;
        chain.store.set_anchor(unhealthy);
        assert_eq!(publish(&mut chain), Some("degrade"));
        assert_eq!(publish(&mut chain), None);
        chain.store.set_anchor(anchor_at(100));
        chain.store.on_confirmed(103);
        assert_eq!(publish(&mut chain), None);
        assert_eq!(chain.store.last_view.as_ref().map(|v| v.slot), Some(103));
    }
}
