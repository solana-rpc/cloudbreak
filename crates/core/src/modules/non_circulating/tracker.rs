// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The stake map and the membership state: the per-block ingest entry point,
//! the snapshot seed, and the bootstrap flip. Everything mutable sits behind
//! one mutex. See the module rustdoc for the ownership and stamp rules.

use super::lists::{NON_CIRCULATING_ACCOUNTS, WITHDRAW_AUTHORITY};
use super::{
    BlockClock, CLOCK_SYSVAR_ID, NonCirculatingBalance, NonCirculatingSummary,
    STAKE_ACCOUNTS_CAPACITY, StakeBlock, StakeEntry, StakeEntryView,
};
use crate::{STAKE_PROGRAM_ID, metrics};
use solana_program::clock::Clock;
use solana_pubkey::Pubkey;
use solana_stake_interface::state::StakeStateV2;
use std::collections::{BTreeSet, HashMap, HashSet, hash_map::Entry};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

#[derive(Clone, Copy, Default, PartialEq)]
enum Status {
    #[default]
    Bootstrapping,
    Live,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrapping => "bootstrapping",
            Self::Live => "live",
        }
    }
}

/// The membership side of the state: the clock, the member set, the two expiry
/// sets, and the running sum. Kept apart from the map so a map entry can be
/// updated while it is placed.
#[derive(Default)]
struct Membership {
    clock: Option<BlockClock>,
    members: HashSet<Pubkey>,
    by_epoch: BTreeSet<(u64, Pubkey)>,
    by_timestamp: BTreeSet<(i64, Pubkey)>,
    total: u128,
}

impl Membership {
    /// Stores a newer clock. A clock at or below the stored slot is ignored.
    fn set_clock(&mut self, clock: BlockClock) -> bool {
        if self.clock.is_some_and(|current| clock.slot <= current.slot) {
            return false;
        }
        self.clock = Some(clock);
        true
    }

    /// Drops the entry's set placement and its share of the total.
    fn unplace(&mut self, pubkey: Pubkey, entry: &StakeEntry) {
        if entry.has(StakeEntry::IN_BY_EPOCH) {
            self.by_epoch.remove(&(entry.lockup_epoch, pubkey));
        }
        if entry.has(StakeEntry::IN_BY_TIMESTAMP) {
            self.by_timestamp
                .remove(&(entry.lockup_unix_timestamp, pubkey));
        }
        if entry.member() {
            self.total -= entry.lamports as u128;
        }
    }

    /// Evaluates the entry against the clock, sets its flags, and adds it to
    /// the set it expires from and to the total. Returns the membership.
    fn place(&mut self, pubkey: Pubkey, entry: &mut StakeEntry) -> bool {
        let clock = self.clock.expect("membership is placed only with a clock");
        let member = entry.evaluate(&clock);
        entry.flags &=
            !(StakeEntry::MEMBER | StakeEntry::IN_BY_EPOCH | StakeEntry::IN_BY_TIMESTAMP);
        // Only a lockup-only member can expire. Pinned and listed ones never do.
        if member && !entry.has(StakeEntry::PINNED) && !entry.has(StakeEntry::LISTED_WITHDRAWER) {
            if entry.lockup_epoch > clock.epoch {
                entry.flags |= StakeEntry::IN_BY_EPOCH;
                self.by_epoch.insert((entry.lockup_epoch, pubkey));
            } else {
                entry.flags |= StakeEntry::IN_BY_TIMESTAMP;
                self.by_timestamp
                    .insert((entry.lockup_unix_timestamp, pubkey));
            }
        }
        if member {
            entry.flags |= StakeEntry::MEMBER;
            self.total += entry.lamports as u128;
            self.members.insert(pubkey);
        } else {
            self.members.remove(&pubkey);
        }
        member
    }

    /// Pops every member the clock has passed. An epoch expiry whose timestamp
    /// is still in force moves to the timestamp set instead of leaving.
    fn expire(&mut self, accounts: &mut HashMap<Pubkey, StakeEntry>, block: &mut StakeBlock) {
        let clock = self.clock.expect("expiries run only with a clock");
        while let Some(&(epoch, pubkey)) = self.by_epoch.first() {
            if epoch > clock.epoch {
                break;
            }
            self.by_epoch.pop_first();
            let entry = accounts.get_mut(&pubkey).expect("indexed entry");
            entry.flags &= !StakeEntry::IN_BY_EPOCH;
            if entry.lockup_unix_timestamp > clock.unix_timestamp {
                entry.flags |= StakeEntry::IN_BY_TIMESTAMP;
                self.by_timestamp
                    .insert((entry.lockup_unix_timestamp, pubkey));
            } else {
                self.leave(pubkey, entry, block);
            }
        }
        while let Some(&(unix_timestamp, pubkey)) = self.by_timestamp.first() {
            if unix_timestamp > clock.unix_timestamp {
                break;
            }
            self.by_timestamp.pop_first();
            let entry = accounts.get_mut(&pubkey).expect("indexed entry");
            entry.flags &= !StakeEntry::IN_BY_TIMESTAMP;
            self.leave(pubkey, entry, block);
        }
    }

    fn leave(&mut self, pubkey: Pubkey, entry: &mut StakeEntry, block: &mut StakeBlock) {
        entry.flags &= !StakeEntry::MEMBER;
        self.total -= entry.lamports as u128;
        self.members.remove(&pubkey);
        block.expired.push((pubkey, entry.lamports));
        note_flip(true, false, block);
    }
}

fn note_flip(was_member: bool, is_member: bool, block: &mut StakeBlock) {
    if was_member == is_member {
        return;
    }
    block.members_changed = true;
    let kind = if is_member { "join" } else { "leave" };
    metrics::NON_CIRCULATING_CHANGES_TOTAL
        .with_label_values(&[kind])
        .inc();
}

struct State {
    status: Status,
    slot: u64,
    accounts: HashMap<Pubkey, StakeEntry>,
    membership: Membership,
    /// Newest slot at which a live block wrote a pubkey the seed had not
    /// reached yet, under an owner other than the stake program. Blocks a
    /// stale snapshot seed from resurrecting it. Dropped at the Live flip.
    seed_shadow: HashMap<Pubkey, u64>,
}

struct Shared {
    pinned: HashSet<Pubkey>,
    withdraw_authorities: HashSet<Pubkey>,
    state: Mutex<State>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("Failed to lock non-circulating state")
    }

    /// Builds the record for one write. Only a stake-owned account is parsed.
    fn entry_from_account(
        &self,
        stake_owned: bool,
        pinned: bool,
        lamports: u64,
        data: &[u8],
        slot: u64,
    ) -> StakeEntry {
        let mut entry = StakeEntry {
            lamports,
            lockup_unix_timestamp: 0,
            lockup_epoch: 0,
            slot,
            flags: 0,
        };
        if pinned {
            entry.flags |= StakeEntry::PINNED;
        }
        if !stake_owned {
            return entry;
        }
        entry.flags |= StakeEntry::STAKE_OWNED;
        if let Ok(StakeStateV2::Initialized(meta) | StakeStateV2::Stake(meta, _, _)) =
            bincode::deserialize::<StakeStateV2>(data)
        {
            entry.flags |= StakeEntry::LOCKUP;
            entry.lockup_unix_timestamp = meta.lockup.unix_timestamp;
            entry.lockup_epoch = meta.lockup.epoch;
            if self
                .withdraw_authorities
                .contains(&meta.authorized.withdrawer)
            {
                entry.flags |= StakeEntry::LISTED_WITHDRAWER;
            }
        }
        entry
    }
}

/// Cheap-clone handle to the stake map and the membership state. The default
/// (`None`) means the feature is disabled and every operation is a no-op.
#[derive(Clone, Default)]
pub struct NonCirculatingTracker(Option<Arc<Shared>>);

impl NonCirculatingTracker {
    pub fn new() -> Self {
        let pinned: HashSet<Pubkey> = NON_CIRCULATING_ACCOUNTS.iter().copied().collect();
        Self(Some(Arc::new(Shared {
            state: Mutex::new(State {
                status: Status::default(),
                slot: 0,
                accounts: HashMap::with_capacity(STAKE_ACCOUNTS_CAPACITY),
                membership: Membership {
                    members: pinned.clone(),
                    ..Membership::default()
                },
                seed_shadow: HashMap::new(),
            }),
            pinned,
            withdraw_authorities: WITHDRAW_AUTHORITY.iter().copied().collect(),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub fn is_live(&self) -> bool {
        self.0
            .as_deref()
            .is_some_and(|shared| shared.state().status == Status::Live)
    }

    /// True for a current member. False before Live, so nothing is served on a
    /// partial set.
    pub fn is_non_circulating(&self, pubkey: &Pubkey) -> bool {
        let Some(shared) = self.0.as_deref() else {
            return false;
        };
        let state = shared.state();
        state.status == Status::Live && state.membership.members.contains(pubkey)
    }

    /// The running sum of the members' lamports. `None` before Live.
    pub fn total(&self) -> Option<u64> {
        let state = self.0.as_deref()?.state();
        (state.status == Status::Live).then_some(state.membership.total as u64)
    }

    /// The applied slot and the member list, empty before Live.
    pub fn members(&self) -> (u64, Vec<Pubkey>) {
        let Some(shared) = self.0.as_deref() else {
            return (0, Vec::new());
        };
        let state = shared.state();
        if state.status != Status::Live {
            return (state.slot, Vec::new());
        }
        (
            state.slot,
            state.membership.members.iter().copied().collect(),
        )
    }

    /// Every member the map holds a balance for, empty before Live. A pinned
    /// account the chain never wrote has no balance and is skipped.
    pub fn member_balances(&self) -> Vec<NonCirculatingBalance> {
        let Some(shared) = self.0.as_deref() else {
            return Vec::new();
        };
        let state = shared.state();
        if state.status != Status::Live {
            return Vec::new();
        }
        state
            .membership
            .members
            .iter()
            .filter_map(|pubkey| {
                let entry = state.accounts.get(pubkey)?;
                Some(NonCirculatingBalance {
                    pubkey: *pubkey,
                    slot: entry.slot,
                    lamports: entry.lamports,
                })
            })
            .collect()
    }

    /// Seeds one snapshot account: the Clock sysvar, a stake account, or a
    /// pinned account. The newest slot wins across the concurrent full and
    /// incremental passes.
    pub fn seed_account(
        &self,
        pubkey: &Pubkey,
        owner: &Pubkey,
        lamports: u64,
        data: &[u8],
        slot: u64,
    ) {
        let Some(shared) = self.0.as_deref() else {
            return;
        };
        if *pubkey == CLOCK_SYSVAR_ID {
            if let Ok(clock) = bincode::deserialize::<Clock>(data) {
                shared.state().membership.set_clock(clock.into());
            }
            return;
        }
        let stake_owned = owner == &STAKE_PROGRAM_ID;
        let pinned = shared.pinned.contains(pubkey);
        if !stake_owned && !pinned {
            return;
        }
        let entry = shared.entry_from_account(stake_owned, pinned, lamports, data, slot);
        let mut state = shared.state();
        let shadowed = state.seed_shadow.get(pubkey).copied();
        match state.accounts.entry(*pubkey) {
            Entry::Vacant(vacant) => {
                if shadowed.is_none_or(|seen| seen < slot) {
                    vacant.insert(entry);
                }
            }
            Entry::Occupied(mut occupied) if occupied.get().slot < slot => {
                occupied.insert(entry);
            }
            Entry::Occupied(_) => {}
        }
    }

    /// Evaluates every seeded entry against the clock, builds the expiry sets
    /// and the running sum, purges tombstones, and flips Live. Returns false
    /// when there is nothing to flip or no clock arrived.
    pub fn finish_bootstrap(&self) -> bool {
        let Some(shared) = self.0.as_deref() else {
            return false;
        };
        let state = &mut *shared.state();
        if state.status != Status::Bootstrapping {
            return false;
        }
        if state.membership.clock.is_none() {
            tracing::error!(
                target: "non_circulating",
                "no Clock sysvar seeded, non-circulating membership stays bootstrapping"
            );
            return false;
        }
        state
            .accounts
            .retain(|_, entry| entry.stake_owned() || entry.has(StakeEntry::PINNED));
        state.seed_shadow = HashMap::new();
        for (pubkey, entry) in state.accounts.iter_mut() {
            state.membership.place(*pubkey, entry);
        }
        state.status = Status::Live;
        metrics::NON_CIRCULATING_MEMBERS.set(state.membership.members.len() as i64);
        tracing::info!(
            target: "non_circulating",
            "non-circulating membership live: {} members, {} stake accounts, {} lamports",
            state.membership.members.len(),
            state.accounts.len(),
            state.membership.total
        );
        true
    }

    /// The per-block path: the block's clock first, then its expiries, then
    /// every account. Deltas and flips are reported only when Live. Before
    /// that the map is updated silently and the supply tracker records every
    /// touch itself.
    pub fn apply_block(
        &self,
        slot: u64,
        is_repaired: bool,
        accounts: &[SubscribeUpdateAccountInfo],
    ) -> StakeBlock {
        let mut block = StakeBlock::default();
        let Some(shared) = self.0.as_deref() else {
            return block;
        };
        let started = Instant::now();
        let state = &mut *shared.state();
        // A replayed live block was already applied.
        if !is_repaired && slot <= state.slot {
            return block;
        }
        state.slot = state.slot.max(slot);
        let live = state.status == Status::Live;
        let State {
            accounts: map,
            membership,
            seed_shadow,
            ..
        } = state;

        let clock = accounts
            .iter()
            .find(|account| account.pubkey.as_slice() == CLOCK_SYSVAR_ID.as_ref())
            .and_then(|account| bincode::deserialize::<Clock>(&account.data).ok());
        if let Some(clock) = clock.map(BlockClock::from)
            && membership.set_clock(clock)
            && live
        {
            membership.expire(map, &mut block);
        }

        for account in accounts {
            let Ok(pubkey) = Pubkey::try_from(account.pubkey.as_slice()) else {
                continue;
            };
            let stake_owned = account.owner.as_slice() == STAKE_PROGRAM_ID.as_ref();
            let pinned = shared.pinned.contains(&pubkey);
            match map.entry(pubkey) {
                Entry::Vacant(vacant) => {
                    if !stake_owned && !pinned {
                        // Shadow the drop so an older seed cannot resurrect it.
                        if !live {
                            let seen = seed_shadow.entry(pubkey).or_insert(slot);
                            *seen = (*seen).max(slot);
                        }
                        continue;
                    }
                    let mut entry = shared.entry_from_account(
                        stake_owned,
                        pinned,
                        account.lamports,
                        &account.data,
                        slot,
                    );
                    if live {
                        let member = membership.place(pubkey, &mut entry);
                        note_flip(false, member, &mut block);
                    }
                    vacant.insert(entry);
                }
                Entry::Occupied(mut occupied) => {
                    let old = *occupied.get();
                    let owned = live && old.stake_owned();
                    if owned {
                        block.handled.insert(pubkey);
                    }
                    if old.slot >= slot {
                        continue;
                    }
                    let mut entry = shared.entry_from_account(
                        stake_owned,
                        pinned,
                        account.lamports,
                        &account.data,
                        slot,
                    );
                    if owned {
                        block.delta += account.lamports as i128 - old.lamports as i128;
                    }
                    if live {
                        membership.unplace(pubkey, &old);
                        let member = membership.place(pubkey, &mut entry);
                        note_flip(old.member(), member, &mut block);
                    }
                    occupied.insert(entry);
                }
            }
        }

        if block.members_changed {
            metrics::NON_CIRCULATING_MEMBERS.set(membership.members.len() as i64);
        }
        metrics::NON_CIRCULATING_BLOCK_MICROSECONDS.observe(started.elapsed().as_micros() as f64);
        block
    }

    /// Copies the counters and the first `heads` entries of each expiry set.
    /// `None` when disabled.
    pub fn summary(&self, heads: usize) -> Option<NonCirculatingSummary> {
        let state = self.0.as_deref()?.state();
        let membership = &state.membership;
        Some(NonCirculatingSummary {
            status: state.status.as_str(),
            slot: state.slot,
            clock: membership.clock,
            members: membership.members.len(),
            non_circulating_lamports: membership.total as u64,
            stake_accounts: state.accounts.len(),
            by_epoch: membership.by_epoch.len(),
            by_timestamp: membership.by_timestamp.len(),
            epoch_heads: membership
                .by_epoch
                .iter()
                .take(heads)
                .map(|(epoch, pubkey)| (*epoch, pubkey.to_string()))
                .collect(),
            timestamp_heads: membership
                .by_timestamp
                .iter()
                .take(heads)
                .map(|(unix_timestamp, pubkey)| (*unix_timestamp, pubkey.to_string()))
                .collect(),
        })
    }

    /// One map entry, for the debug endpoint.
    pub fn entry(&self, pubkey: &Pubkey) -> Option<StakeEntryView> {
        self.0
            .as_deref()?
            .state()
            .accounts
            .get(pubkey)
            .copied()
            .map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_stake_interface::state::{Authorized, Lockup, Meta};

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn clock(slot: u64, epoch: u64, unix_timestamp: i64) -> Clock {
        Clock {
            slot,
            epoch,
            unix_timestamp,
            ..Clock::default()
        }
    }

    fn clock_account(clock: Clock) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: CLOCK_SYSVAR_ID.to_bytes().to_vec(),
            owner: vec![0; 32],
            lamports: 1,
            data: bincode::serialize(&clock).unwrap(),
            ..Default::default()
        }
    }

    fn stake_data(withdrawer: Pubkey, epoch: u64, unix_timestamp: i64) -> Vec<u8> {
        let meta = Meta {
            authorized: Authorized {
                staker: withdrawer,
                withdrawer,
            },
            lockup: Lockup {
                unix_timestamp,
                epoch,
                custodian: Pubkey::default(),
            },
            ..Meta::default()
        };
        bincode::serialize(&StakeStateV2::Initialized(meta)).unwrap()
    }

    pub(crate) fn stake_account(
        pubkey: Pubkey,
        lamports: u64,
        epoch: u64,
        unix_timestamp: i64,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            owner: STAKE_PROGRAM_ID.to_bytes().to_vec(),
            lamports,
            data: stake_data(pk(99), epoch, unix_timestamp),
            ..Default::default()
        }
    }

    pub(crate) fn system_account(pubkey: Pubkey, lamports: u64) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            owner: vec![0; 32],
            lamports,
            ..Default::default()
        }
    }

    /// A live tracker whose clock is at slot 10, epoch 5, timestamp 1000.
    pub(crate) fn live_tracker() -> NonCirculatingTracker {
        let tracker = NonCirculatingTracker::new();
        tracker.seed_account(
            &CLOCK_SYSVAR_ID,
            &Pubkey::default(),
            1,
            &bincode::serialize(&clock(10, 5, 1000)).unwrap(),
            10,
        );
        assert!(tracker.finish_bootstrap());
        tracker
    }

    fn recomputed_total(tracker: &NonCirculatingTracker) -> u128 {
        let state = tracker.0.as_deref().unwrap().state();
        let clock = state.membership.clock.unwrap();
        state
            .accounts
            .values()
            .filter(|entry| entry.evaluate(&clock))
            .map(|entry| entry.lamports as u128)
            .sum()
    }

    #[test]
    fn record_is_forty_bytes() {
        assert_eq!(std::mem::size_of::<StakeEntry>(), 40);
    }

    #[test]
    fn membership_flips_both_ways_and_moves_the_total() {
        let t = live_tracker();
        // Epoch lockup in force: joins.
        let block = t.apply_block(11, false, &[stake_account(pk(1), 500, 9, 0)]);
        assert!(block.members_changed);
        assert!(t.is_non_circulating(&pk(1)));
        assert_eq!(t.total(), Some(500));
        // Balance change while a member: handled, total follows.
        let block = t.apply_block(12, false, &[stake_account(pk(1), 700, 9, 0)]);
        assert!(block.handled.contains(&pk(1)));
        assert_eq!(block.delta, 200);
        assert!(!block.members_changed);
        assert_eq!(t.total(), Some(700));
        // Lockup lifted: leaves.
        let block = t.apply_block(13, false, &[stake_account(pk(1), 700, 0, 0)]);
        assert!(block.members_changed);
        assert!(!t.is_non_circulating(&pk(1)));
        assert_eq!(t.total(), Some(0));
    }

    #[test]
    fn epoch_expiry_leaves_or_moves_to_the_timestamp_set() {
        let t = live_tracker();
        // pk(1) expires with the epoch. pk(2) still has its timestamp after.
        t.apply_block(
            11,
            false,
            &[
                stake_account(pk(1), 100, 6, 0),
                stake_account(pk(2), 200, 6, 5000),
            ],
        );
        assert_eq!(t.summary(10).unwrap().by_epoch, 2);
        let block = t.apply_block(12, false, &[clock_account(clock(12, 6, 2000))]);
        assert_eq!(block.expired, vec![(pk(1), 100)]);
        assert!(t.is_non_circulating(&pk(2)));
        let summary = t.summary(10).unwrap();
        assert_eq!((summary.by_epoch, summary.by_timestamp), (0, 1));
        assert_eq!(t.total(), Some(200));
        // Then the timestamp passes.
        let block = t.apply_block(13, false, &[clock_account(clock(13, 6, 5000))]);
        assert_eq!(block.expired, vec![(pk(2), 200)]);
        assert_eq!(t.total(), Some(0));
        assert_eq!(t.summary(10).unwrap().by_timestamp, 0);
    }

    #[test]
    fn clock_never_regresses() {
        let t = live_tracker();
        t.apply_block(11, false, &[stake_account(pk(1), 100, 5, 3000)]);
        t.apply_block(12, false, &[clock_account(clock(12, 5, 4000))]);
        assert!(!t.is_non_circulating(&pk(1)));
        // A repaired block with an older clock cannot bring the member back.
        t.apply_block(9, true, &[clock_account(clock(9, 5, 100))]);
        assert_eq!(t.summary(10).unwrap().clock.unwrap().slot, 12);
        assert!(!t.is_non_circulating(&pk(1)));
        // Nor does re-evaluating the account against the current clock.
        t.apply_block(13, false, &[stake_account(pk(1), 100, 5, 3000)]);
        assert!(!t.is_non_circulating(&pk(1)));
    }

    #[test]
    fn stale_write_is_handled_with_zero_delta() {
        let t = live_tracker();
        t.apply_block(20, false, &[stake_account(pk(1), 100, 9, 0)]);
        let block = t.apply_block(15, true, &[stake_account(pk(1), 900, 9, 0)]);
        assert!(block.handled.contains(&pk(1)));
        assert_eq!(block.delta, 0);
        assert_eq!(t.total(), Some(100));
    }

    #[test]
    fn new_stake_pubkey_is_left_to_the_supply_tracker() {
        let t = live_tracker();
        let block = t.apply_block(11, false, &[stake_account(pk(1), 100, 0, 0)]);
        assert!(!block.handled.contains(&pk(1)));
        assert_eq!(block.delta, 0);
    }

    #[test]
    fn close_is_handled_and_tombstone_keeps_its_stamp() {
        let t = live_tracker();
        t.apply_block(11, false, &[stake_account(pk(1), 100, 9, 0)]);
        let block = t.apply_block(12, false, &[system_account(pk(1), 0)]);
        assert!(block.handled.contains(&pk(1)));
        assert_eq!(block.delta, -100);
        assert!(!t.is_non_circulating(&pk(1)));
        // The tombstone rejects an older repaired write and keeps ownership off.
        let block = t.apply_block(11, true, &[stake_account(pk(1), 100, 9, 0)]);
        assert!(!block.handled.contains(&pk(1)));
        assert!(!t.is_non_circulating(&pk(1)));
        assert_eq!(t.total(), Some(0));
    }

    #[test]
    fn pinned_account_is_always_a_member() {
        let t = live_tracker();
        let pinned = NON_CIRCULATING_ACCOUNTS[0];
        assert!(t.is_non_circulating(&pinned));
        let block = t.apply_block(11, false, &[system_account(pinned, 42)]);
        assert!(!block.handled.contains(&pinned));
        assert_eq!(t.total(), Some(42));
        t.apply_block(12, false, &[system_account(pinned, 0)]);
        assert!(t.is_non_circulating(&pinned));
        assert_eq!(t.total(), Some(0));
    }

    #[test]
    fn nothing_is_served_before_live() {
        let t = NonCirculatingTracker::new();
        t.apply_block(11, false, &[stake_account(pk(1), 100, 9, 0)]);
        assert!(!t.is_non_circulating(&pk(1)));
        assert_eq!(t.total(), None);
        assert!(t.members().1.is_empty());
        // No clock seeded: the flip fails closed.
        assert!(!t.finish_bootstrap());
    }

    #[test]
    fn running_total_matches_a_recompute_after_random_updates() {
        let t = live_tracker();
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let (mut epoch, mut unix_timestamp) = (5u64, 1000i64);
        for slot in 11..600 {
            let mut accounts = Vec::new();
            for _ in 0..8 {
                let pubkey = pk((next() % 40) as u8 + 1);
                let lamports = next() % 1_000;
                match next() % 5 {
                    0 => accounts.push(system_account(pubkey, 0)),
                    1 => accounts.push(stake_account(pubkey, lamports, 0, 0)),
                    2 => accounts.push(stake_account(pubkey, lamports, epoch + next() % 3, 0)),
                    _ => accounts.push(stake_account(
                        pubkey,
                        lamports,
                        epoch + next() % 2,
                        unix_timestamp + (next() % 400) as i64,
                    )),
                }
            }
            if next() % 4 == 0 {
                epoch += next() % 2;
                unix_timestamp += (next() % 300) as i64;
                accounts.push(clock_account(clock(slot, epoch, unix_timestamp)));
            }
            t.apply_block(slot, false, &accounts);
            assert_eq!(t.total().map(u128::from), Some(recomputed_total(&t)));
            let summary = t.summary(0).unwrap();
            assert_eq!(summary.members, t.members().1.len());
        }
    }
}
