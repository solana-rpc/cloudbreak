// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The bounded hot-accounts map. One entry per account touched recently or
//! pinned, keyed by the full pubkey. It holds the previous balance for the
//! per-block supply delta: a hit is a memory read, a miss goes to the DB.
//!
//! No async, no DB, no lock. The tracker owns one `HotAccounts` behind its state
//! mutex and calls these methods under it. Fully unit-tested below.

use crate::STAKE_PROGRAM_ID;
use solana_pubkey::Pubkey;
use std::collections::HashMap;

/// One cached account. A zero-lamport entry is a tombstone kept like any other,
/// carrying the close stamp so a later repaired block contributes nothing.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    pub lamports: u64,
    pub slot: u64,
    pub write_version: u64,
    /// True while the account's last known owner is the Stake program. Pinned
    /// entries are never swept, so the epoch-reward burst is all hits.
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

/// The DB row a miss read returned for one account, or `None` for no row.
pub type PrevRow = Option<(u64, u64, u64)>;

pub struct HotAccounts {
    map: HashMap<Pubkey, Entry>,
    /// Cap on unpinned entries. Pinned stake accounts sit on top of it.
    cap: usize,
    /// Cap on entries pinned by a live write failure, so a stuck DB does not pin
    /// without bound. Beyond it the tracker fails closed.
    fail_pin_cap: usize,
    /// When false the stake-account pinning is off and stake rides the cap like
    /// any other account. The epoch burst then falls to the DB.
    pin_stake: bool,
    /// Maintained incrementally so the sweep and metrics never rescan the map.
    pinned_count: usize,
    /// Entries pinned by a write failure, counted against `fail_pin_cap`.
    fail_pinned: usize,
    /// Slot of the last sweep, for the "at least every N slots" trigger.
    last_sweep_slot: u64,
}

impl HotAccounts {
    pub fn with_capacity(buckets: usize, cap: usize, fail_pin_cap: usize, pin_stake: bool) -> Self {
        Self {
            map: HashMap::with_capacity(buckets),
            cap,
            fail_pin_cap,
            pin_stake,
            pinned_count: 0,
            fail_pinned: 0,
            last_sweep_slot: 0,
        }
    }

    fn is_stake(&self, owner: &Pubkey) -> bool {
        self.pin_stake && owner == &STAKE_PROGRAM_ID
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

    pub fn bucket_count(&self) -> usize {
        self.map.capacity()
    }

    /// Writes back `(lamports, slot, write_version)` for a touched account and
    /// updates its pinned flag from the owner. Adjusts the pinned count. Used for
    /// hits and for the miss write-back. Never downgrades on a stale stamp.
    fn write_back(&mut self, pubkey: Pubkey, lamports: u64, slot: u64, wv: u64, owner: &Pubkey) {
        let pinned = self.is_stake(owner);
        match self.map.get_mut(&pubkey) {
            Some(entry) => {
                if (slot, wv) >= (entry.slot, entry.write_version) {
                    entry.lamports = lamports;
                    entry.slot = slot;
                    entry.write_version = wv;
                    self.set_pinned(pubkey, pinned);
                }
            }
            None => {
                if pinned {
                    self.pinned_count += 1;
                }
                self.map.insert(
                    pubkey,
                    Entry {
                        lamports,
                        slot,
                        write_version: wv,
                        pinned,
                    },
                );
            }
        }
    }

    /// Flips an existing entry's pinned flag and keeps the pinned count exact.
    fn set_pinned(&mut self, pubkey: Pubkey, pinned: bool) {
        if let Some(entry) = self.map.get_mut(&pubkey) {
            if entry.pinned && !pinned {
                self.pinned_count -= 1;
            } else if !entry.pinned && pinned {
                self.pinned_count += 1;
            }
            entry.pinned = pinned;
        }
    }

    /// Probes a touched account. On a hit the delta is folded and the entry
    /// written back. On a miss the caller resolves it with a DB read.
    ///
    /// `zero_prev` forces a full count with no DB read, for an account that
    /// closed inside the bootstrap window: its previous balance is known to be 0.
    pub fn probe(
        &mut self,
        pubkey: Pubkey,
        lamports: u64,
        slot: u64,
        wv: u64,
        owner: &Pubkey,
        zero_prev: bool,
    ) -> Probe {
        if zero_prev {
            self.write_back(pubkey, lamports, slot, wv, owner);
            return Probe::Hit(lamports as i128);
        }
        match self.map.get(&pubkey).copied() {
            Some(entry) => {
                if entry.slot < slot {
                    let delta = lamports as i128 - entry.lamports as i128;
                    self.write_back(pubkey, lamports, slot, wv, owner);
                    Probe::Hit(delta)
                } else {
                    // A replayed or out-of-order update. Newest stamp still wins,
                    // but it contributes no delta.
                    if (slot, wv) > (entry.slot, entry.write_version) {
                        self.write_back(pubkey, lamports, slot, wv, owner);
                    }
                    Probe::Hit(0)
                }
            }
            None => Probe::Miss,
        }
    }

    /// Applies the entry rule after a miss read returned `prev` (or `None`).
    /// Inserts the newer of the block write and the DB row, and returns the
    /// delta.
    pub fn apply_miss(
        &mut self,
        pubkey: Pubkey,
        lamports: u64,
        slot: u64,
        wv: u64,
        owner: &Pubkey,
        prev: PrevRow,
    ) -> i128 {
        match prev {
            None => {
                self.write_back(pubkey, lamports, slot, wv, owner);
                lamports as i128
            }
            Some((prev_lamports, prev_slot, _prev_wv)) if prev_slot < slot => {
                self.write_back(pubkey, lamports, slot, wv, owner);
                lamports as i128 - prev_lamports as i128
            }
            Some((prev_lamports, prev_slot, prev_wv)) => {
                // The DB row is newer than the block write. Cache it, count 0.
                self.write_back(pubkey, prev_lamports, prev_slot, prev_wv, owner);
                0
            }
        }
    }

    /// Seeds one stake account from the snapshot. Newest `(slot, write_version)`
    /// wins across the concurrent full and incremental passes. Stored pinned,
    /// including a zero-lamport tombstone.
    pub fn seed_stake_account(&mut self, pubkey: Pubkey, lamports: u64, slot: u64, wv: u64) {
        match self.map.get_mut(&pubkey) {
            Some(entry) => {
                if (slot, wv) > (entry.slot, entry.write_version) {
                    entry.lamports = lamports;
                    entry.slot = slot;
                    entry.write_version = wv;
                }
                self.set_pinned(pubkey, true);
            }
            None => {
                self.pinned_count += 1;
                self.map.insert(
                    pubkey,
                    Entry {
                        lamports,
                        slot,
                        write_version: wv,
                        pinned: true,
                    },
                );
            }
        }
    }

    /// Folds the stake scan into the pinned set: inserts an absent account,
    /// upgrades an older entry, never downgrades. Drops pinned tombstones older
    /// than `scan_slot - PINNED_TOMBSTONE_RETENTION`, whose DB rows are
    /// finalize-cleaned by then so a miss read returns the same answer (nothing).
    pub fn refresh_pinned(
        &mut self,
        rows: impl IntoIterator<Item = (Pubkey, u64, u64, u64)>,
        scan_slot: u64,
    ) {
        for (pubkey, lamports, slot, wv) in rows {
            match self.map.get_mut(&pubkey) {
                Some(entry) => {
                    if (slot, wv) > (entry.slot, entry.write_version) {
                        entry.lamports = lamports;
                        entry.slot = slot;
                        entry.write_version = wv;
                    }
                    self.set_pinned(pubkey, true);
                }
                None => {
                    self.pinned_count += 1;
                    self.map.insert(
                        pubkey,
                        Entry {
                            lamports,
                            slot,
                            write_version: wv,
                            pinned: true,
                        },
                    );
                }
            }
        }

        let cutoff = scan_slot.saturating_sub(PINNED_TOMBSTONE_RETENTION);
        let mut dropped = 0usize;
        self.map.retain(|_, entry| {
            let drop = entry.pinned && entry.lamports == 0 && entry.slot < cutoff;
            if drop {
                dropped += 1;
            }
            !drop
        });
        self.pinned_count -= dropped;
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

    /// Pins a live-write-failure block's touched accounts so the DB is never
    /// consulted for them, keeping the cache authoritative. Returns false when
    /// the failure-pin cap would be exceeded, so the tracker fails closed.
    ///
    /// Exactness: each touched entry already holds this block's applied write, so
    /// pinning it keeps the correct balance resident. Nothing else writes the
    /// account before its next touch, so no later hit or miss ever needs the row
    /// the failed write never persisted.
    pub fn pin_failed(&mut self, pubkeys: &[Pubkey]) -> bool {
        let mut newly = 0usize;
        for pubkey in pubkeys {
            if self.map.get(pubkey).is_some_and(|entry| !entry.pinned) {
                newly += 1;
            }
        }
        if self.fail_pinned + newly > self.fail_pin_cap {
            return false;
        }
        self.fail_pinned += newly;
        for pubkey in pubkeys {
            self.set_pinned(*pubkey, true);
        }
        true
    }

    #[cfg(test)]
    pub fn get(&self, pubkey: &Pubkey) -> Option<Entry> {
        self.map.get(pubkey).copied()
    }
}

/// Pinned tombstones older than this many slots before the scan slot are
/// dropped: their DB rows are finalize-cleaned, so a miss returns no row (0).
const PINNED_TOMBSTONE_RETENTION: u64 = 1_000;

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
        HotAccounts::with_capacity(64, 4, 8, true)
    }

    #[test]
    fn hit_delta_and_write_back() {
        let mut c = cache();
        assert!(matches!(c.probe(pk(1), 100, 10, 1, &other(), false), Probe::Miss));
        let d = c.apply_miss(pk(1), 100, 10, 1, &other(), None);
        assert_eq!(d, 100);
        // Next touch is a hit with the delta from the stored balance.
        match c.probe(pk(1), 150, 12, 2, &other(), false) {
            Probe::Hit(d) => assert_eq!(d, 50),
            Probe::Miss => panic!("expected hit"),
        }
        assert_eq!(c.get(&pk(1)).unwrap().lamports, 150);
    }

    #[test]
    fn older_stamp_rejected() {
        let mut c = cache();
        c.apply_miss(pk(1), 100, 12, 5, &other(), None);
        // An update at an older slot contributes 0 and does not overwrite.
        match c.probe(pk(1), 40, 11, 1, &other(), false) {
            Probe::Hit(d) => assert_eq!(d, 0),
            Probe::Miss => panic!("expected hit"),
        }
        assert_eq!(c.get(&pk(1)).unwrap().lamports, 100);
    }

    #[test]
    fn equal_slot_replay_contributes_zero() {
        let mut c = cache();
        c.apply_miss(pk(1), 100, 12, 5, &other(), None);
        match c.probe(pk(1), 999, 12, 4, &other(), false) {
            Probe::Hit(d) => assert_eq!(d, 0),
            Probe::Miss => panic!("expected hit"),
        }
        // Same slot, lower write version: no overwrite.
        assert_eq!(c.get(&pk(1)).unwrap().lamports, 100);
    }

    #[test]
    fn tombstone_hit_counts_full_new_balance() {
        let mut c = cache();
        // Close: zero-lamport tombstone kept with the close stamp.
        c.apply_miss(pk(1), 0, 12, 5, &other(), None);
        assert_eq!(c.get(&pk(1)).unwrap().lamports, 0);
        match c.probe(pk(1), 130, 13, 1, &other(), false) {
            Probe::Hit(d) => assert_eq!(d, 130),
            Probe::Miss => panic!("expected hit"),
        }
    }

    #[test]
    fn miss_no_row_counts_full() {
        let mut c = cache();
        let d = c.apply_miss(pk(5), 100, 30, 1, &other(), None);
        assert_eq!(d, 100);
    }

    #[test]
    fn miss_newer_db_row_contributes_zero_and_caches_it() {
        let mut c = cache();
        // The DB already has a newer row than this block write.
        let d = c.apply_miss(pk(5), 500, 30, 1, &other(), Some((700, 31, 9)));
        assert_eq!(d, 0);
        let e = c.get(&pk(5)).unwrap();
        assert_eq!((e.lamports, e.slot), (700, 31));
    }

    #[test]
    fn miss_older_db_row_counts_difference() {
        let mut c = cache();
        let d = c.apply_miss(pk(5), 500, 30, 2, &other(), Some((300, 28, 1)));
        assert_eq!(d, 200);
        assert_eq!(c.get(&pk(5)).unwrap().lamports, 500);
    }

    #[test]
    fn pinned_routing_by_owner_and_unpin_on_close() {
        let mut c = cache();
        c.apply_miss(pk(1), 100, 10, 1, &STAKE_PROGRAM_ID, None);
        assert!(c.get(&pk(1)).unwrap().pinned);
        assert_eq!(c.pinned_len(), 1);
        // Close under the system program: becomes an unpinned tombstone.
        c.probe(pk(1), 0, 11, 2, &other(), false);
        assert!(!c.get(&pk(1)).unwrap().pinned);
        assert_eq!(c.pinned_len(), 0);
    }

    #[test]
    fn sweep_keeps_pinned_and_drops_below_cutoff() {
        let mut c = HotAccounts::with_capacity(64, 2, 8, true);
        // Two pinned, four unpinned at rising slots.
        c.apply_miss(pk(1), 1, 5, 1, &STAKE_PROGRAM_ID, None);
        c.apply_miss(pk(2), 1, 6, 1, &STAKE_PROGRAM_ID, None);
        c.apply_miss(pk(10), 1, 10, 1, &other(), None);
        c.apply_miss(pk(11), 1, 11, 1, &other(), None);
        c.apply_miss(pk(12), 1, 12, 1, &other(), None);
        c.apply_miss(pk(13), 1, 13, 1, &other(), None);
        assert_eq!(c.hot_len(), 4);
        let evicted = c.sweep(100);
        assert!(evicted >= 1);
        // Pinned entries survive.
        assert!(c.get(&pk(1)).is_some());
        assert!(c.get(&pk(2)).is_some());
        assert_eq!(c.pinned_len(), 2);
        // The newest unpinned entry survives.
        assert!(c.get(&pk(13)).is_some());
    }

    #[test]
    fn seed_newest_wins_across_out_of_order_passes() {
        let mut c = cache();
        // Incremental (higher slot) arrives first, then full (lower slot).
        c.seed_stake_account(pk(6), 600, 50, 5);
        c.seed_stake_account(pk(6), 500, 40, 4);
        let e = c.get(&pk(6)).unwrap();
        assert_eq!((e.lamports, e.slot), (600, 50));
        assert!(e.pinned);
    }

    #[test]
    fn refresh_pinned_never_downgrades() {
        let mut c = cache();
        c.seed_stake_account(pk(6), 600, 50, 5);
        c.refresh_pinned([(pk(6), 100, 40, 1)], 60);
        // Older scan row does not clobber the newer seed.
        assert_eq!(c.get(&pk(6)).unwrap().lamports, 600);
        c.refresh_pinned([(pk(6), 800, 70, 1)], 80);
        assert_eq!(c.get(&pk(6)).unwrap().lamports, 800);
    }

    #[test]
    fn refresh_pinned_drops_old_tombstone() {
        let mut c = cache();
        c.seed_stake_account(pk(6), 0, 50, 5);
        assert_eq!(c.pinned_len(), 1);
        // Scan far ahead: the old tombstone is dropped.
        c.refresh_pinned(std::iter::empty(), 50 + PINNED_TOMBSTONE_RETENTION + 1);
        assert!(c.get(&pk(6)).is_none());
        assert_eq!(c.pinned_len(), 0);
    }

    #[test]
    fn pin_failed_respects_cap() {
        let mut c = HotAccounts::with_capacity(64, 4, 2, true);
        c.apply_miss(pk(1), 1, 5, 1, &other(), None);
        c.apply_miss(pk(2), 1, 6, 1, &other(), None);
        c.apply_miss(pk(3), 1, 7, 1, &other(), None);
        assert!(c.pin_failed(&[pk(1), pk(2)]));
        // Third failure pin exceeds the cap of 2.
        assert!(!c.pin_failed(&[pk(3)]));
    }
}
