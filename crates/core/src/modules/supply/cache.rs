// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The bounded hot-accounts map. One entry per account touched recently, keyed
//! by the full pubkey. It holds the previous balance for the per-block supply
//! delta: a hit is a memory read, a miss goes to the DB. Stake-owned accounts
//! never enter it, because the non-circulating stake map owns their balance.
//!
//! No async, no DB, no lock. The tracker owns one `HotAccounts` behind its state
//! mutex and calls these methods under it. Fully unit-tested below.

use crate::STAKE_PROGRAM_ID;
use serde::Serialize;
use solana_pubkey::Pubkey;
use std::collections::HashMap;

/// One cached account. A zero-lamport entry is a tombstone kept like any other,
/// carrying the close slot so a later repaired block contributes nothing.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Entry {
    pub lamports: u64,
    pub slot: u64,
    /// Pinned by a live write failure: never swept, so the DB is never
    /// consulted for a balance it does not hold.
    pub pinned: bool,
}

/// The result of probing an account against the cache.
pub enum Probe {
    /// The previous balance was in memory. Carries the delta already folded and
    /// written back.
    Hit(i128),
    /// The account is absent. The caller must resolve it with a DB read and then
    /// call [`HotAccounts::apply_miss`].
    Miss,
}

/// The `(lamports, slot)` row a miss read returned for one account, or `None`
/// for no row.
pub type PrevRow = Option<(u64, u64)>;

pub struct HotAccounts {
    map: HashMap<Pubkey, Entry>,
    /// Cap on unpinned entries.
    cap: usize,
    /// Cap on pinned entries, so a stuck DB does not pin without bound. Beyond
    /// it the tracker fails closed.
    fail_pin_cap: usize,
    /// Maintained incrementally so the sweep and metrics never rescan the map.
    pinned_count: usize,
    last_sweep_slot: u64,
}

impl HotAccounts {
    /// Pre-sizes the map for the cap, the sweep slack above it, and the pin
    /// budget, so it never resizes in steady state.
    pub fn with_capacity(cap: usize, fail_pin_cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap + cap / 10 + fail_pin_cap),
            cap,
            fail_pin_cap,
            pinned_count: 0,
            last_sweep_slot: 0,
        }
    }

    pub fn pinned_len(&self) -> usize {
        self.pinned_count
    }

    pub fn hot_len(&self) -> usize {
        self.map.len() - self.pinned_count
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Items the map holds before it resizes.
    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn last_sweep_slot(&self) -> u64 {
        self.last_sweep_slot
    }

    pub fn get(&self, pubkey: &Pubkey) -> Option<Entry> {
        self.map.get(pubkey).copied()
    }

    /// Writes back a touched account. Never downgrades on an older slot. A
    /// stake-owned write leaves the cache instead: the stake map owns it now.
    fn write_back(&mut self, pubkey: Pubkey, lamports: u64, slot: u64, owner: &Pubkey) {
        if owner == &STAKE_PROGRAM_ID {
            self.remove(&pubkey);
            return;
        }
        match self.map.get_mut(&pubkey) {
            Some(entry) => {
                if slot >= entry.slot {
                    entry.lamports = lamports;
                    entry.slot = slot;
                }
            }
            None => {
                self.map.insert(
                    pubkey,
                    Entry {
                        lamports,
                        slot,
                        pinned: false,
                    },
                );
            }
        }
    }

    fn remove(&mut self, pubkey: &Pubkey) {
        if let Some(entry) = self.map.remove(pubkey)
            && entry.pinned
        {
            self.pinned_count -= 1;
        }
    }

    /// Probes a touched account. A hit folds the delta and writes back. A miss
    /// leaves the DB read to the caller. `zero_prev` forces a full count for an
    /// account that closed inside the bootstrap window.
    pub fn probe(
        &mut self,
        pubkey: Pubkey,
        lamports: u64,
        slot: u64,
        owner: &Pubkey,
        zero_prev: bool,
    ) -> Probe {
        if zero_prev {
            self.write_back(pubkey, lamports, slot, owner);
            return Probe::Hit(lamports as i128);
        }
        match self.map.get(&pubkey).copied() {
            Some(entry) if entry.slot < slot => {
                let delta = lamports as i128 - entry.lamports as i128;
                self.write_back(pubkey, lamports, slot, owner);
                Probe::Hit(delta)
            }
            // A replayed or out-of-order update: already counted, no delta.
            Some(_) => Probe::Hit(0),
            None => Probe::Miss,
        }
    }

    /// Applies the entry rule after a miss read. Inserts the newer of the block
    /// write and the DB row, and returns the delta.
    pub fn apply_miss(
        &mut self,
        pubkey: Pubkey,
        lamports: u64,
        slot: u64,
        owner: &Pubkey,
        prev: PrevRow,
    ) -> i128 {
        match prev {
            None => {
                self.write_back(pubkey, lamports, slot, owner);
                lamports as i128
            }
            Some((prev_lamports, prev_slot)) if prev_slot < slot => {
                self.write_back(pubkey, lamports, slot, owner);
                lamports as i128 - prev_lamports as i128
            }
            Some((prev_lamports, prev_slot)) => {
                // The DB row is newer than the block write. Cache it, count 0.
                self.write_back(pubkey, prev_lamports, prev_slot, owner);
                0
            }
        }
    }

    /// True when the unpinned population is over the cap plus its slack, or the
    /// slot floor has passed. Cheap, checked from the slot watch.
    pub fn sweep_due(&self, slot: u64) -> bool {
        self.hot_len() > self.cap + self.cap / 10
            || slot.saturating_sub(self.last_sweep_slot) >= SWEEP_SLOT_FLOOR
    }

    /// Evicts unpinned entries down toward 90% of the cap by dropping the oldest
    /// by slot. Pinned entries are always kept. Returns the number evicted.
    pub fn sweep(&mut self, slot: u64) -> usize {
        self.last_sweep_slot = slot;
        let target = self.cap - self.cap / 10;
        let hot = self.hot_len();
        if hot <= target {
            return 0;
        }
        let mut slots: Vec<u64> = self
            .map
            .values()
            .filter(|entry| !entry.pinned)
            .map(|entry| entry.slot)
            .collect();
        // The cutoff leaves `target` newest unpinned entries.
        let nth = slots.len() - target;
        let (_, cutoff, _) = slots.select_nth_unstable(nth);
        let cutoff = *cutoff;
        let before = self.map.len();
        self.map
            .retain(|_, entry| entry.pinned || entry.slot >= cutoff);
        before - self.map.len()
    }

    /// Pins a failed block's touched accounts so the DB is never consulted for them.
    /// Exact: each entry already holds this block's write, and nothing else writes
    /// it before its next touch. Returns false past the failure-pin cap.
    pub fn pin_failed(&mut self, pubkeys: &[Pubkey]) -> bool {
        let newly = pubkeys
            .iter()
            .filter(|pubkey| self.map.get(pubkey).is_some_and(|entry| !entry.pinned))
            .count();
        if self.pinned_count + newly > self.fail_pin_cap {
            return false;
        }
        self.pinned_count += newly;
        for pubkey in pubkeys {
            if let Some(entry) = self.map.get_mut(pubkey) {
                entry.pinned = true;
            }
        }
        true
    }
}

/// The sweep runs at least this often even when the cap is not exceeded.
const SWEEP_SLOT_FLOOR: u64 = 3_000;

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn other() -> Pubkey {
        pk(9)
    }

    fn cache() -> HotAccounts {
        HotAccounts::with_capacity(4, 8)
    }

    #[test]
    fn hit_delta_and_write_back() {
        let mut c = cache();
        assert!(matches!(
            c.probe(pk(1), 100, 10, &other(), false),
            Probe::Miss
        ));
        let d = c.apply_miss(pk(1), 100, 10, &other(), None);
        assert_eq!(d, 100);
        match c.probe(pk(1), 150, 12, &other(), false) {
            Probe::Hit(d) => assert_eq!(d, 50),
            Probe::Miss => panic!("expected hit"),
        }
        assert_eq!(c.get(&pk(1)).unwrap().lamports, 150);
    }

    #[test]
    fn older_or_same_slot_contributes_nothing() {
        let mut c = cache();
        c.apply_miss(pk(1), 100, 12, &other(), None);
        // An out-of-order older slot, then a replayed equal slot.
        for slot in [11, 12] {
            match c.probe(pk(1), 40, slot, &other(), false) {
                Probe::Hit(d) => assert_eq!(d, 0),
                Probe::Miss => panic!("expected hit"),
            }
            assert_eq!(c.get(&pk(1)).unwrap().lamports, 100);
        }
    }

    #[test]
    fn tombstone_hit_counts_full_new_balance() {
        let mut c = cache();
        // Close: zero-lamport tombstone kept with the close stamp.
        c.apply_miss(pk(1), 0, 12, &other(), None);
        assert_eq!(c.get(&pk(1)).unwrap().lamports, 0);
        match c.probe(pk(1), 130, 13, &other(), false) {
            Probe::Hit(d) => assert_eq!(d, 130),
            Probe::Miss => panic!("expected hit"),
        }
    }

    #[test]
    fn miss_newer_db_row_contributes_zero_and_caches_it() {
        let mut c = cache();
        let d = c.apply_miss(pk(5), 500, 30, &other(), Some((700, 31)));
        assert_eq!(d, 0);
        let e = c.get(&pk(5)).unwrap();
        assert_eq!((e.lamports, e.slot), (700, 31));
    }

    #[test]
    fn miss_older_db_row_counts_difference() {
        let mut c = cache();
        let d = c.apply_miss(pk(5), 500, 30, &other(), Some((300, 28)));
        assert_eq!(d, 200);
        assert_eq!(c.get(&pk(5)).unwrap().lamports, 500);
    }

    #[test]
    fn stake_owned_write_leaves_the_cache() {
        let mut c = cache();
        c.apply_miss(pk(1), 100, 10, &other(), None);
        assert!(c.pin_failed(&[pk(1)]));
        // The delta still counts, then the stake map owns the pubkey.
        match c.probe(pk(1), 100, 11, &STAKE_PROGRAM_ID, false) {
            Probe::Hit(d) => assert_eq!(d, 0),
            Probe::Miss => panic!("expected hit"),
        }
        assert!(c.get(&pk(1)).is_none());
        assert_eq!(c.pinned_len(), 0);
        assert_eq!(c.apply_miss(pk(2), 5, 11, &STAKE_PROGRAM_ID, None), 5);
        assert!(c.is_empty());
    }

    #[test]
    fn sweep_keeps_pinned_and_drops_below_cutoff() {
        let mut c = HotAccounts::with_capacity(2, 8);
        // Two pinned, four unpinned at rising slots.
        c.apply_miss(pk(1), 1, 5, &other(), None);
        c.apply_miss(pk(2), 1, 6, &other(), None);
        assert!(c.pin_failed(&[pk(1), pk(2)]));
        c.apply_miss(pk(10), 1, 10, &other(), None);
        c.apply_miss(pk(11), 1, 11, &other(), None);
        c.apply_miss(pk(12), 1, 12, &other(), None);
        c.apply_miss(pk(13), 1, 13, &other(), None);
        assert_eq!(c.hot_len(), 4);
        let evicted = c.sweep(100);
        assert!(evicted >= 1);
        assert!(c.get(&pk(1)).is_some());
        assert!(c.get(&pk(2)).is_some());
        assert_eq!(c.pinned_len(), 2);
        assert!(c.get(&pk(13)).is_some());
    }

    #[test]
    fn pin_failed_respects_cap() {
        let mut c = HotAccounts::with_capacity(4, 2);
        c.apply_miss(pk(1), 1, 5, &other(), None);
        c.apply_miss(pk(2), 1, 6, &other(), None);
        c.apply_miss(pk(3), 1, 7, &other(), None);
        assert!(c.pin_failed(&[pk(1), pk(2)]));
        // Third failure pin exceeds the cap of 2.
        assert!(!c.pin_failed(&[pk(3)]));
    }
}
