// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Block tree built from the processed feed. Blocks are keyed by blockhash and linked by
//! `(parent_slot, parent_blockhash)`, so every chain walk proves ancestry by hash, not by slot.
//!
//! Each block keeps its touched accounts as a key-sorted slice of `(pubkey, lamports, owner id)`,
//! vote accounts included. Owners are interned: ids 0 and 1 are the token programs, 2 is the vote
//! program. A touch is about 48 bytes. At an estimated 4,000 touched keys per mainnet block, the
//! default 1,500 slots hold about 290 MB (estimate, see the RSS row of the coverage report).
//!
//! A dead slot counts as repaired once it is confirmed or has a confirmed descendant through a
//! hash-matched chain. A slot with two stored blockhashes makes every chain through it ambiguous,
//! so key queries on such a chain answer uncovered.

use rand::seq::SliceRandom;
use solana_pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const TOKEN_PROGRAMS: [Pubkey; 2] = [
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"),
];
pub const VOTE_PROGRAM: Pubkey =
    Pubkey::from_str_const("Vote111111111111111111111111111111111111111");
const VOTE_ID: u32 = 2;
/// Confirmed slots tried by `canonical` before it answers unknown.
const CANONICAL_PROBES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Touch {
    pub key: Pubkey,
    pub lamports: u64,
    pub owner: u32,
}

impl Touch {
    pub fn is_token(&self) -> bool {
        (self.owner as usize) < TOKEN_PROGRAMS.len()
    }

    pub fn is_vote(&self) -> bool {
        self.owner == VOTE_ID
    }
}

#[derive(Clone, Debug)]
pub struct Block {
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub received_at: Instant,
    /// Sorted by key, one entry per key.
    pub touched: Arc<[Touch]>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Block {
        slot: u64,
        received_at: Instant,
        touched: Arc<[Touch]>,
    },
    Fork {
        fork_point: u64,
    },
    Dead {
        slot: u64,
    },
    Confirmed {
        slot: u64,
    },
}

pub struct BlockTree {
    blocks: HashMap<String, Block>,
    by_slot: BTreeMap<u64, Vec<String>>,
    children: HashMap<String, Vec<String>>,
    confirmed: BTreeSet<u64>,
    dead: BTreeSet<u64>,
    created: BTreeSet<u64>,
    fork_points: BTreeSet<u64>,
    owners: Vec<Pubkey>,
    owner_ids: HashMap<Pubkey, u32>,
    highest: Option<String>,
    max_seen: u64,
}

impl Default for BlockTree {
    fn default() -> Self {
        let mut tree = Self {
            blocks: HashMap::new(),
            by_slot: BTreeMap::new(),
            children: HashMap::new(),
            confirmed: BTreeSet::new(),
            dead: BTreeSet::new(),
            created: BTreeSet::new(),
            fork_points: BTreeSet::new(),
            owners: Vec::new(),
            owner_ids: HashMap::new(),
            highest: None,
            max_seen: 0,
        };
        let fixed = TOKEN_PROGRAMS.iter().chain([&VOTE_PROGRAM]);
        fixed.for_each(|p| _ = tree.intern(*p));
        tree
    }
}

impl BlockTree {
    pub fn intern(&mut self, owner: Pubkey) -> u32 {
        let next = self.owners.len() as u32;
        let id = *self.owner_ids.entry(owner).or_insert(next);
        if id == next {
            self.owners.push(owner);
        }
        id
    }

    pub fn owner(&self, id: u32) -> Pubkey {
        self.owners[id as usize]
    }

    pub fn tip(&self) -> u64 {
        self.highest.as_ref().map_or(0, |h| self.blocks[h].slot)
    }

    pub fn lowest_slot(&self) -> Option<u64> {
        self.by_slot.keys().next().copied()
    }

    pub fn highest_confirmed(&self) -> Option<u64> {
        self.confirmed.last().copied()
    }

    pub fn slots_in(&self, lo: u64, hi: u64) -> Vec<u64> {
        self.by_slot.range(lo..=hi).map(|(s, _)| *s).collect()
    }

    pub fn first_at(&self, slot: u64) -> Option<&Block> {
        self.by_slot.get(&slot)?.first().map(|h| &self.blocks[h])
    }

    /// The stored parent, only when both its slot and blockhash match.
    pub fn parent(&self, block: &Block) -> Option<&Block> {
        let parent = self.blocks.get(&block.parent_blockhash)?;
        (parent.slot == block.parent_slot).then_some(parent)
    }

    /// Blocks from the first version at `slot` down through hash-matched parents.
    pub fn chain(&self, slot: u64) -> impl Iterator<Item = &Block> {
        std::iter::successors(self.first_at(slot), |b| self.parent(b))
    }

    fn duplicated(&self, slot: u64) -> bool {
        self.by_slot.get(&slot).is_some_and(|hs| hs.len() > 1)
    }

    /// Stores a block and returns its events. A repeated blockhash is a no-op.
    pub fn insert(&mut self, block: Block) -> Vec<Event> {
        if self.blocks.contains_key(&block.blockhash) {
            return Vec::new();
        }
        let mut events = vec![Event::Block {
            slot: block.slot,
            received_at: block.received_at,
            touched: block.touched.clone(),
        }];
        let parent = self.parent(&block).map(|p| p.blockhash.clone());
        let second_child = parent
            .as_ref()
            .is_some_and(|p| self.children.get(p).is_some_and(|c| !c.is_empty()));
        let off_tip = parent.is_some() && parent != self.highest;
        let rival = self.highest.clone();
        let (hash, slot) = (block.blockhash.clone(), block.slot);
        if let Some(p) = &parent {
            let siblings = self.children.entry(p.clone()).or_default();
            siblings.push(hash.clone());
        }
        self.by_slot.entry(slot).or_default().push(hash.clone());
        self.blocks.insert(hash.clone(), block);
        self.max_seen = self.max_seen.max(slot);
        if self.highest.is_none() || slot > self.tip() {
            self.highest = Some(hash.clone());
        }
        let fork_point = match (second_child, off_tip, rival) {
            (true, _, _) => parent.map(|p| self.blocks[&p].slot),
            (false, true, Some(rival)) => self.common_ancestor(&hash, &rival),
            _ => None,
        };
        if let Some(fork_point) = fork_point.filter(|f| self.fork_points.insert(*f)) {
            events.push(Event::Fork { fork_point });
        }
        events
    }

    pub fn confirm(&mut self, slot: u64) -> bool {
        self.max_seen = self.max_seen.max(slot);
        self.confirmed.insert(slot)
    }

    pub fn mark_dead(&mut self, slot: u64) -> bool {
        self.max_seen = self.max_seen.max(slot);
        self.dead.insert(slot)
    }

    /// Records a bank creation. True when it is a repeat that newly marks the slot restarted.
    pub fn on_created_bank(&mut self, slot: u64) -> bool {
        self.max_seen = self.max_seen.max(slot);
        !self.created.insert(slot) && self.dead.insert(slot)
    }

    /// Forgets bank creations, so a resubscription does not read as a restart.
    pub fn new_session(&mut self) {
        self.created.clear();
    }

    fn common_ancestor(&self, a: &str, b: &str) -> Option<u64> {
        let walk = |h: &str| std::iter::successors(self.blocks.get(h), |b| self.parent(b));
        let seen: HashSet<&str> = walk(a).map(|b| b.blockhash.as_str()).collect();
        walk(b)
            .find(|x| seen.contains(x.blockhash.as_str()))
            .map(|x| x.slot)
    }

    fn descends(&self, block: &Block, ancestor: u64) -> bool {
        let chain = std::iter::successors(Some(block), |b| self.parent(b));
        chain
            .take_while(|b| b.slot >= ancestor)
            .any(|b| b.slot == ancestor)
    }

    /// True when `a` is `b` or on its hash-matched parent chain.
    pub fn is_ancestor(&self, a: u64, b: u64) -> bool {
        self.first_at(b)
            .is_some_and(|block| self.descends(block, a))
    }

    fn probes(&self, slot: u64) -> Vec<u64> {
        let above = self.confirmed.range(slot + 1..);
        above.take(CANONICAL_PROBES).copied().collect()
    }

    /// True when `slot` is confirmed or a confirmed slot descends from it by hash.
    pub fn confirmed_through(&self, slot: u64) -> bool {
        self.confirmed.contains(&slot)
            || self.probes(slot).iter().any(|c| self.is_ancestor(slot, *c))
    }

    /// Some(true) when a confirmed slot is or descends from `slot`, Some(false) when it is dead
    /// or a confirmed slot at or above it provably does not descend from it, None while unknown.
    pub fn canonical(&self, slot: u64) -> Option<bool> {
        if self.confirmed_through(slot) {
            return Some(true);
        }
        if self.dead.contains(&slot) {
            return Some(false);
        }
        let probes = self.probes(slot);
        probes.into_iter().find_map(|c| self.chain_verdict(c, slot))
    }

    fn chain_verdict(&self, from: u64, slot: u64) -> Option<bool> {
        let last = self
            .chain(from)
            .find(|b| b.slot <= slot || self.parent(b).is_none())?;
        if last.slot <= slot {
            return Some(last.slot == slot);
        }
        match last.parent_slot.cmp(&slot) {
            std::cmp::Ordering::Less => Some(false),
            std::cmp::Ordering::Greater => None,
            std::cmp::Ordering::Equal => {
                let hashes = self.by_slot.get(&slot)?;
                Some(hashes.contains(&last.parent_blockhash))
            }
        }
    }

    /// True when `slot` or a block on its chain is dead or restarted and not repaired.
    pub fn dead_chain(&self, slot: u64) -> bool {
        if self.confirmed_through(slot) {
            return false;
        }
        let unrepaired = |s: u64| self.dead.contains(&s) && !self.confirmed_through(s);
        let mut last = None;
        for block in self.chain(slot) {
            if unrepaired(block.slot) {
                return true;
            }
            last = Some(block.parent_slot);
        }
        unrepaired(slot)
            || last.is_some_and(|p| self.dead.contains(&p) && !self.confirmed.contains(&p))
    }

    /// Newest touched value on the chain ending at `slot`. None when not touched in the window,
    /// or when the walk meets a slot with two stored blockhashes.
    pub fn lamports_at(&self, key: &Pubkey, slot: u64) -> Option<(u64, Pubkey)> {
        for block in self.chain(slot) {
            if self.duplicated(block.slot) {
                return None;
            }
            if let Some(t) = find(&block.touched, key) {
                return Some((t.lamports, self.owner(t.owner)));
            }
        }
        None
    }

    /// Newest write to `key` in `(s, c]` on the chain ending at `c`. Some(None) means no write,
    /// None means the window does not cover the range or a slot on it is duplicated.
    pub fn write_between(&self, key: &Pubkey, s: u64, c: u64) -> Option<Option<(u64, Pubkey)>> {
        if c <= s {
            return (c == s).then_some(None);
        }
        let mut block = self.first_at(c)?;
        loop {
            if self.duplicated(block.slot) {
                return None;
            }
            if let Some(t) = find(&block.touched, key) {
                return Some(Some((t.lamports, self.owner(t.owner))));
            }
            if block.parent_slot <= s {
                return (block.parent_slot == s).then_some(None);
            }
            block = self.parent(block)?;
        }
    }

    /// True when any stored block above `slot` touches `key`.
    pub fn touched_after(&self, key: &Pubkey, slot: u64) -> bool {
        let mut above = self.by_slot.range(slot + 1..).flat_map(|(_, hs)| hs);
        above.any(|h| find(&self.blocks[h].touched, key).is_some())
    }

    /// Random non-vote keys touched above `fork_point` on every branch, spread over blocks.
    pub fn branch_keys(&self, fork_point: u64, cap: usize) -> Vec<Pubkey> {
        let hashes = self.by_slot.range(fork_point + 1..).flat_map(|(_, hs)| hs);
        let blocks: Vec<&Block> = hashes.map(|h| &self.blocks[h]).collect();
        let per_block = (cap / blocks.len().max(1)).max(1);
        let rng = &mut rand::thread_rng();
        let mut keys: Vec<Pubkey> = Vec::new();
        for block in blocks {
            let eligible: Vec<&Touch> = block.touched.iter().filter(|t| !t.is_vote()).collect();
            keys.extend(eligible.choose_multiple(rng, per_block).map(|t| t.key));
        }
        keys.sort_unstable();
        keys.dedup();
        keys.shuffle(rng);
        keys.truncate(cap);
        keys
    }

    /// Drops the lowest slots while they are older than `window` or more than `max_slots` below
    /// the highest slot seen. Slot sets keep the same floor, so they stay bounded without blocks.
    pub fn prune(&mut self, now: Instant, window: Duration, max_slots: u64) {
        let floor = self.max_seen.saturating_sub(max_slots);
        while let Some((&slot, hashes)) = self.by_slot.first_key_value() {
            let old = |h: &String| now.duration_since(self.blocks[h].received_at) > window;
            let keeps_tip = self.highest.as_ref().is_some_and(|h| hashes.contains(h));
            if keeps_tip || (slot >= floor && !hashes.iter().all(old)) {
                break;
            }
            for hash in self
                .by_slot
                .pop_first()
                .map(|(_, hs)| hs)
                .unwrap_or_default()
            {
                self.blocks.remove(&hash);
                self.children.remove(&hash);
            }
        }
        let low = self.lowest_slot().unwrap_or(floor);
        for set in [
            &mut self.confirmed,
            &mut self.dead,
            &mut self.created,
            &mut self.fork_points,
        ] {
            *set = set.split_off(&low);
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.blocks.len()
    }
}

fn find<'a>(touched: &'a [Touch], key: &Pubkey) -> Option<&'a Touch> {
    let i = touched.binary_search_by(|t| t.key.cmp(key)).ok()?;
    Some(&touched[i])
}

#[cfg(test)]
pub mod tests;
