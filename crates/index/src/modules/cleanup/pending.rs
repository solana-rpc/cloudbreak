// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The pending map and the keys one finalized slot contributes to it.

use std::collections::{HashMap, HashSet};

use crate::indexer::AccountsReceivedPerBlock;

/// A cleanup target. `owner` is `Some` when the owner map knew the account, which lets the
/// DELETE prune to one hash partition. `None` runs the unrouted form.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CleanupKey {
    pub owner: Option<[u8; 32]>,
    pub pubkey: [u8; 32],
}

/// Which SQL form a key takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KeyForm {
    Routed,
    Unrouted,
}

impl KeyForm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Routed => "routed",
            Self::Unrouted => "unrouted",
        }
    }
}

/// One drain's worth of keys, split by the SQL form they take.
#[derive(Debug, Default)]
pub struct Taken {
    pub routed: Vec<(CleanupKey, u64)>,
    pub unrouted: Vec<(CleanupKey, u64)>,
    /// The frontier when the oldest of these keys was queued. Drives the lag gauge.
    pub oldest_stamp: u64,
}

impl Taken {
    pub fn is_empty(&self) -> bool {
        self.routed.is_empty() && self.unrouted.is_empty()
    }

    pub fn len(&self) -> usize {
        self.routed.len() + self.unrouted.len()
    }
}

/// Keys derived from one finalized slot, plus the closes the owner map did not cover.
pub struct DerivedKeys {
    pub items: Vec<(CleanupKey, u64)>,
    pub uncovered_closes: usize,
}

/// Turns one finalized slot's block record into keys with cutoffs.
///
/// Open keys take the finalized slot as their cutoff. Closed and owner-moved keys take one past
/// it, so the mask written at the slot goes with the rows it shadows.
///
/// On a node with the owner map on, only closes the map knew are queued. `save_block` collects
/// every zero-lamport account in the block before the program filter runs, so the rest are
/// accounts this node does not index and they have no rows to delete.
pub fn keys_for_slot(
    accounts: &AccountsReceivedPerBlock,
    slot: u64,
    owner_map_enabled: bool,
) -> DerivedKeys {
    let closed_cutoff = slot.saturating_add(1);
    let mut items =
        Vec::with_capacity(accounts.accounts.len() + accounts.closed_cleanup_pubkeys.len());

    let open_routed =
        owner_map_enabled && accounts.accounts_owners.len() == accounts.accounts.len();
    for (index, pubkey) in accounts.accounts.iter().enumerate() {
        let Some(pubkey) = to_key_bytes(pubkey) else {
            continue;
        };
        let owner = if open_routed {
            to_key_bytes(&accounts.accounts_owners[index])
        } else {
            None
        };
        items.push((CleanupKey { owner, pubkey }, slot));
    }

    let mut uncovered_closes = 0;
    if owner_map_enabled {
        let paired = accounts.closed_cleanup_owners.len() == accounts.closed_cleanup_pubkeys.len();
        let mut covered: HashSet<&[u8]> = HashSet::new();
        if paired {
            for (index, pubkey) in accounts.closed_cleanup_pubkeys.iter().enumerate() {
                covered.insert(pubkey.as_slice());
                let Some(pubkey) = to_key_bytes(pubkey) else {
                    continue;
                };
                let owner = to_key_bytes(&accounts.closed_cleanup_owners[index]);
                items.push((CleanupKey { owner, pubkey }, closed_cutoff));
            }
        }
        uncovered_closes = accounts
            .closed_accounts
            .iter()
            .filter(|pubkey| !covered.contains(pubkey.as_slice()))
            .count();
    } else {
        for pubkey in &accounts.closed_accounts {
            let Some(pubkey) = to_key_bytes(pubkey) else {
                continue;
            };
            items.push((
                CleanupKey {
                    owner: None,
                    pubkey,
                },
                closed_cutoff,
            ));
        }
    }

    DerivedKeys {
        items,
        uncovered_closes,
    }
}

fn to_key_bytes(bytes: &[u8]) -> Option<[u8; 32]> {
    bytes.try_into().ok()
}

/// Keys waiting to be cleaned, one exclusive cutoff each, merged by max.
///
/// One drainer takes the whole map at once, so a key is never in two statements and no in-flight
/// bookkeeping per key is needed. A key touched again while a drain is running lands back in the
/// map with its newer cutoff and drains on the next round.
pub(crate) struct Pending {
    keys: HashMap<CleanupKey, u64>,
    /// The frontier when the oldest key still queued was first inserted.
    oldest_stamp: Option<u64>,
    /// Same, for the batch a drain is running right now.
    in_flight_oldest: Option<u64>,
    high_water: u64,
    enqueued_since_drain: u64,
    last_drain_slot: Option<u64>,
    interval_slots: u64,
}

impl Pending {
    pub(crate) fn new(interval_slots: u64) -> Self {
        Self {
            keys: HashMap::new(),
            oldest_stamp: None,
            in_flight_oldest: None,
            high_water: 0,
            enqueued_since_drain: 0,
            last_drain_slot: None,
            interval_slots,
        }
    }

    /// Merges one finalized slot's keys in and returns how many coalesced into a queued key.
    pub(crate) fn enqueue(&mut self, slot: u64, items: &[(CleanupKey, u64)]) -> usize {
        self.high_water = self.high_water.max(slot);
        self.enqueued_since_drain = self.enqueued_since_drain.saturating_add(1);

        let stamp = self.high_water;
        let mut coalesced = 0;
        for (key, cutoff) in items {
            match self.keys.get_mut(key) {
                Some(existing) => {
                    *existing = (*existing).max(*cutoff);
                    coalesced += 1;
                }
                None => {
                    self.keys.insert(*key, *cutoff);
                    self.oldest_stamp = Some(self.oldest_stamp.unwrap_or(stamp).min(stamp));
                }
            }
        }
        coalesced
    }

    /// Takes every queued key, split by SQL form.
    pub(crate) fn take_all(&mut self) -> Taken {
        let oldest_stamp = self.oldest_stamp.unwrap_or(self.high_water);
        let mut taken = Taken {
            oldest_stamp,
            ..Default::default()
        };
        for (key, cutoff) in self.keys.drain() {
            if key.owner.is_some() {
                taken.routed.push((key, cutoff));
            } else {
                taken.unrouted.push((key, cutoff));
            }
        }
        self.oldest_stamp = None;
        self.in_flight_oldest = (!taken.is_empty()).then_some(oldest_stamp);
        taken
    }

    /// Releases a drain that succeeded.
    pub(crate) fn finish(&mut self) {
        self.in_flight_oldest = None;
    }

    /// Returns a failed drain's keys, keeping the older of the two cutoffs' stamps.
    pub(crate) fn reinsert(&mut self, taken: Taken) {
        let stamp = taken.oldest_stamp;
        for (key, cutoff) in taken.routed.into_iter().chain(taken.unrouted) {
            let entry = self.keys.entry(key).or_insert(cutoff);
            *entry = (*entry).max(cutoff);
        }
        if !self.keys.is_empty() {
            self.oldest_stamp = Some(self.oldest_stamp.unwrap_or(stamp).min(stamp));
        }
        self.in_flight_oldest = None;
    }

    /// `true` when the drainer should wake.
    ///
    /// The count term catches the ordinary case. The span term catches an ancestor walk
    /// finalizing several slots in one call and a gap fill jumping the frontier.
    pub(crate) fn should_drain(&self) -> bool {
        let span = self
            .high_water
            .saturating_sub(self.last_drain_slot.unwrap_or(self.high_water));
        self.enqueued_since_drain >= self.interval_slots || span >= self.interval_slots
    }

    pub(crate) fn note_drain(&mut self) {
        self.enqueued_since_drain = 0;
        self.last_drain_slot = Some(self.high_water);
    }

    /// Finalized slots since the oldest key still waiting, counting a drain in flight.
    pub(crate) fn lag_slots(&self) -> u64 {
        let oldest = match (self.oldest_stamp, self.in_flight_oldest) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        oldest.map_or(0, |stamp| self.high_water.saturating_sub(stamp))
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.keys.is_empty() && self.in_flight_oldest.is_none()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn pubkey(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    pub(crate) fn routed(owner: u8, pk: u8) -> CleanupKey {
        CleanupKey {
            owner: Some(pubkey(owner)),
            pubkey: pubkey(pk),
        }
    }

    pub(crate) fn unrouted(pk: u8) -> CleanupKey {
        CleanupKey {
            owner: None,
            pubkey: pubkey(pk),
        }
    }

    fn cutoff_of(pending: &Pending, key: &CleanupKey) -> Option<u64> {
        pending.keys.get(key).copied()
    }

    fn block(
        accounts: Vec<Vec<u8>>,
        owners: Vec<Vec<u8>>,
        closed: Vec<Vec<u8>>,
        closed_pubkeys: Vec<Vec<u8>>,
        closed_owners: Vec<Vec<u8>>,
    ) -> AccountsReceivedPerBlock {
        AccountsReceivedPerBlock {
            block_time: None,
            accounts,
            accounts_owners: owners,
            closed_accounts: closed,
            closed_cleanup_pubkeys: closed_pubkeys,
            closed_cleanup_owners: closed_owners,
        }
    }

    #[test]
    fn enqueue_twice_merges_cutoff_by_max() {
        let mut pending = Pending::new(1);
        let key = routed(1, 1);
        pending.enqueue(100, &[(key, 100)]);
        pending.enqueue(120, &[(key, 120)]);
        assert_eq!(cutoff_of(&pending, &key), Some(120));
    }

    #[test]
    fn a_lower_cutoff_after_a_higher_one_is_absorbed() {
        let mut pending = Pending::new(1);
        let key = routed(1, 1);
        pending.enqueue(120, &[(key, 120)]);
        // A repaired slot finalizing below the frontier must not lower the bound.
        pending.enqueue(90, &[(key, 90)]);
        assert_eq!(cutoff_of(&pending, &key), Some(120));
    }

    #[test]
    fn open_key_gets_cutoff_s_and_closed_key_gets_s_plus_one() {
        let derived = keys_for_slot(
            &block(
                vec![pubkey(1).to_vec()],
                vec![pubkey(9).to_vec()],
                vec![pubkey(2).to_vec()],
                vec![pubkey(2).to_vec()],
                vec![pubkey(9).to_vec()],
            ),
            100,
            true,
        );
        let open = derived
            .items
            .iter()
            .find(|(key, _)| key.pubkey == pubkey(1))
            .expect("open key");
        let closed = derived
            .items
            .iter()
            .find(|(key, _)| key.pubkey == pubkey(2))
            .expect("closed key");
        assert_eq!(open.1, 100);
        assert_eq!(closed.1, 101);
    }

    #[test]
    fn close_then_reopen_merges_to_the_reopen_cutoff() {
        let mut pending = Pending::new(1);
        let key = routed(1, 1);
        pending.enqueue(100, &[(key, 101)]);
        pending.enqueue(120, &[(key, 120)]);
        assert_eq!(cutoff_of(&pending, &key), Some(120));
    }

    #[test]
    fn owner_change_yields_two_keys() {
        // The account moved from owner 8 to owner 9 in the slot.
        let derived = keys_for_slot(
            &block(
                vec![pubkey(1).to_vec()],
                vec![pubkey(9).to_vec()],
                vec![],
                vec![pubkey(1).to_vec()],
                vec![pubkey(8).to_vec()],
            ),
            100,
            true,
        );
        assert_eq!(derived.items.len(), 2);
        let new_owner = derived
            .items
            .iter()
            .find(|(key, _)| key.owner == Some(pubkey(9)))
            .expect("new owner key");
        let old_owner = derived
            .items
            .iter()
            .find(|(key, _)| key.owner == Some(pubkey(8)))
            .expect("old owner key");
        assert_eq!(new_owner.1, 100, "the live row at the slot must survive");
        assert_eq!(old_owner.1, 101, "the old owner's mask must go");
    }

    #[test]
    fn owner_map_off_builds_pubkey_only_keys() {
        let derived = keys_for_slot(
            &block(
                vec![pubkey(1).to_vec()],
                vec![],
                vec![pubkey(2).to_vec()],
                vec![],
                vec![],
            ),
            100,
            false,
        );
        assert_eq!(derived.items.len(), 2);
        assert!(derived.items.iter().all(|(key, _)| key.owner.is_none()));
        assert_eq!(derived.uncovered_closes, 0);
    }

    #[test]
    fn uncovered_closes_are_counted_and_dropped_on_a_map_on_node() {
        // Two closes reached the block, the map knew one. The other is not indexed here.
        let derived = keys_for_slot(
            &block(
                vec![],
                vec![],
                vec![pubkey(2).to_vec(), pubkey(3).to_vec()],
                vec![pubkey(2).to_vec()],
                vec![pubkey(9).to_vec()],
            ),
            100,
            true,
        );
        assert_eq!(derived.items.len(), 1);
        assert_eq!(derived.uncovered_closes, 1);
    }

    #[test]
    fn mismatched_owner_lengths_fall_back_to_unrouted_open_keys() {
        let derived = keys_for_slot(
            &block(
                vec![pubkey(1).to_vec(), pubkey(2).to_vec()],
                vec![pubkey(9).to_vec()],
                vec![],
                vec![],
                vec![],
            ),
            100,
            true,
        );
        assert_eq!(derived.items.len(), 2);
        assert!(derived.items.iter().all(|(key, _)| key.owner.is_none()));
    }

    #[test]
    fn take_all_splits_by_form_and_empties_the_map() {
        let mut pending = Pending::new(1);
        pending.enqueue(100, &[(routed(1, 1), 100), (unrouted(2), 100)]);

        let taken = pending.take_all();
        assert_eq!(taken.routed.len(), 1);
        assert_eq!(taken.unrouted.len(), 1);
        assert!(pending.keys.is_empty());
        assert!(!pending.is_quiescent(), "the drain is still in flight");

        pending.finish();
        assert!(pending.is_quiescent());
    }

    #[test]
    fn a_key_retouched_during_a_drain_stays_queued() {
        let mut pending = Pending::new(1);
        let key = routed(1, 1);
        pending.enqueue(100, &[(key, 100)]);
        let taken = pending.take_all();

        pending.enqueue(101, &[(key, 101)]);
        pending.finish();

        // The drain that finished covered cutoff 100. The newer cutoff is still owed.
        assert_eq!(taken.routed[0].1, 100);
        assert_eq!(cutoff_of(&pending, &key), Some(101));
    }

    #[test]
    fn reinsert_after_failure_loses_no_key_and_merges_a_retouched_key() {
        let mut pending = Pending::new(1);
        let key = routed(1, 1);
        let other = routed(1, 2);
        pending.enqueue(100, &[(key, 100), (other, 100)]);
        let taken = pending.take_all();

        pending.enqueue(101, &[(key, 101)]);
        pending.reinsert(taken);

        assert_eq!(
            cutoff_of(&pending, &key),
            Some(101),
            "the newer cutoff wins"
        );
        assert_eq!(cutoff_of(&pending, &other), Some(100));
        assert!(!pending.is_quiescent());
    }

    #[test]
    fn lag_is_zero_when_empty_and_counts_a_drain_in_flight() {
        let mut pending = Pending::new(1);
        assert_eq!(pending.lag_slots(), 0);

        pending.enqueue(100, &[(routed(1, 1), 100)]);
        pending.enqueue(105, &[]);
        assert_eq!(pending.lag_slots(), 5);

        let _taken = pending.take_all();
        assert_eq!(pending.lag_slots(), 5, "a drain in flight still counts");

        pending.finish();
        assert_eq!(pending.lag_slots(), 0);
    }

    #[test]
    fn lag_does_not_spike_on_a_repaired_slot_below_the_frontier() {
        let mut pending = Pending::new(1);
        pending.enqueue(1000, &[(routed(1, 1), 1000)]);
        // A repaired slot far below the frontier is stamped with the frontier, not its own slot.
        pending.enqueue(60, &[(routed(1, 2), 60)]);
        assert_eq!(pending.lag_slots(), 0);
    }

    #[test]
    fn interval_trigger_fires_on_enqueued_count() {
        let mut pending = Pending::new(3);
        pending.enqueue(100, &[]);
        assert!(!pending.should_drain());
        pending.enqueue(101, &[]);
        assert!(!pending.should_drain());
        pending.enqueue(102, &[]);
        assert!(pending.should_drain());
    }

    #[test]
    fn interval_trigger_fires_on_slot_span_after_a_gap_fill() {
        let mut pending = Pending::new(10);
        pending.enqueue(100, &[]);
        pending.note_drain();
        // One call, but the frontier jumped a whole gap.
        pending.enqueue(200, &[]);
        assert!(pending.should_drain());
    }

    #[test]
    fn a_wide_interval_is_not_preempted_by_volume() {
        let mut pending = Pending::new(1_000);
        for slot in 100..200u64 {
            pending.enqueue(slot, &[(routed(1, (slot % 250) as u8), slot)]);
        }
        pending.note_drain();
        assert!(!pending.should_drain(), "only the interval governs");
    }

    #[test]
    fn zero_key_enqueue_raises_high_water_and_counts_a_slot() {
        let mut pending = Pending::new(1);
        pending.enqueue(100, &[]);
        assert!(pending.should_drain());
        assert_eq!(pending.lag_slots(), 0);
        assert!(pending.is_quiescent());
    }
}
