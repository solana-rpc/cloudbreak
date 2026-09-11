// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The supply tracker: the running total, the status machine (Bootstrapping,
//! Live, GapFilling, Stale), the bootstrap window, the gap-close set, and the
//! hot-accounts cache.
//!
//! The block path calls [`SupplyTracker::build_block_pending`], then
//! [`SupplyTracker::apply_block`] with the stake map's outcome and the block's
//! row writes, then [`SupplyTracker::finish_block`]. The delta source is the
//! stake map for stake pubkeys, and the hot cache plus a by-pubkey miss read
//! for everything else. See the module rustdoc for the lock protocol and the
//! feed invariant the block path relies on.

use crate::metrics;
use crate::modules::non_circulating::{NonCirculatingTracker, StakeBlock};
use crate::modules::supply::cache::{Entry, HotAccounts, Probe};
use crate::modules::supply::persist::persist_supply_row;
use crate::modules::supply::prev;
use sea_orm::DatabaseConnection;
use serde::Serialize;
use solana_pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

pub const SUPPLY_RING_SLOTS: u64 = 128;

/// One account's update in a block: the one version the feed carries for it.
#[derive(Clone, Copy, Debug)]
pub struct Pending {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
}

#[derive(Clone, Copy, Debug)]
struct TouchedAccount {
    slot: u64,
    lamports: u64,
}

/// The outcome of [`SupplyTracker::apply_block`], consumed by
/// [`SupplyTracker::finish_block`].
pub enum BlockOutcome {
    /// Disabled, stale, or a replayed slot: nothing to commit.
    Idle,
    /// A bootstrap-window block: touches recorded and the cache warmed.
    Bootstrapping,
    /// A live or gap-filling block whose delta was computed.
    Delta { delta: i128, touched: Vec<Pubkey> },
    /// The miss read failed after a retry. The tracker is already Stale.
    ReadFailed,
}

#[derive(Clone, Default)]
pub struct SupplyTracker(Option<Arc<Inner>>);

struct Inner {
    state: Mutex<SupplyState>,
    block_writes: tokio::sync::Mutex<()>,
    non_circulating: NonCirculatingTracker,
    db: DatabaseConnection,
    query_timeout: Duration,
}

#[derive(Clone, Copy, Default, PartialEq)]
enum SupplyStatus {
    #[default]
    Bootstrapping,
    Live,
    GapFilling,
    Stale,
}

impl SupplyStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrapping => "bootstrapping",
            Self::Live => "live",
            Self::GapFilling => "gap_filling",
            Self::Stale => "stale",
        }
    }
}

struct SupplyState {
    status: SupplyStatus,
    bootstrap_failed: bool,
    total: u64,
    slot: u64,
    startup_slot: u64,
    startup_touched: HashMap<Pubkey, TouchedAccount>,
    startup_zero_prev: HashSet<Pubkey>,
    gap_closes: HashMap<Pubkey, u64>,
    cache: HotAccounts,
}

#[derive(Debug, Clone)]
pub struct SupplyCommit {
    pub slot: u64,
    pub total: u64,
    pub non_circulating: Option<u64>,
}

/// An O(1) copy of the tracker state for the debug endpoint.
#[derive(Serialize)]
pub struct SupplySummary {
    pub status: &'static str,
    pub bootstrap_failed: bool,
    pub total: u64,
    pub slot: u64,
    pub startup_slot: u64,
    pub startup_touched: usize,
    pub gap_closes: usize,
    pub hot_entries: usize,
    pub pinned_entries: usize,
    pub capacity: usize,
    pub cap: usize,
    pub last_sweep_slot: u64,
}

impl SupplyTracker {
    /// Builds an enabled tracker with a pre-sized cache. `cap` bounds the unpinned
    /// entries and `fail_pin_cap` the live write-failure pins. `query_timeout`
    /// bounds the miss read and the row upsert. `non_circulating` supplies the
    /// non-circulating lamports of every commit.
    pub fn new(
        db: DatabaseConnection,
        query_timeout: Duration,
        cap: usize,
        fail_pin_cap: usize,
        non_circulating: NonCirculatingTracker,
    ) -> Self {
        Self(Some(Arc::new(Inner {
            state: Mutex::new(SupplyState {
                status: SupplyStatus::default(),
                bootstrap_failed: false,
                total: 0,
                slot: 0,
                startup_slot: 0,
                startup_touched: HashMap::new(),
                startup_zero_prev: HashSet::new(),
                gap_closes: HashMap::new(),
                cache: HotAccounts::with_capacity(cap, fail_pin_cap),
            }),
            block_writes: tokio::sync::Mutex::new(()),
            non_circulating,
            db,
            query_timeout,
        })))
    }

    /// Copies the counters and status the debug endpoint shows. `None` when
    /// disabled.
    pub fn summary(&self) -> Option<SupplySummary> {
        let inner = self.0.as_deref()?;
        let state = inner.state();
        Some(SupplySummary {
            status: state.status.as_str(),
            bootstrap_failed: state.bootstrap_failed,
            total: state.total,
            slot: state.slot,
            startup_slot: state.startup_slot,
            startup_touched: state.startup_touched.len(),
            gap_closes: state.gap_closes.len(),
            hot_entries: state.cache.hot_len(),
            pinned_entries: state.cache.pinned_len(),
            capacity: state.cache.capacity(),
            cap: state.cache.cap(),
            last_sweep_slot: state.cache.last_sweep_slot(),
        })
    }

    /// One cache entry, for the debug endpoint.
    pub fn entry(&self, pubkey: &Pubkey) -> Option<Entry> {
        self.0.as_deref()?.state().cache.get(pubkey)
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub async fn lock_block_writes(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        let inner = self.0.as_deref()?;
        Some(inner.block_writes.lock().await)
    }

    pub fn set_startup_total(&self, slot: u64, capitalization: u64) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.status == SupplyStatus::Bootstrapping && slot > state.startup_slot {
            state.startup_slot = slot;
            state.total = capitalization;
        }
    }

    pub fn startup_slot(&self) -> Option<u64> {
        let inner = self.0.as_deref()?;
        let state = inner.state();
        (state.status == SupplyStatus::Bootstrapping && state.startup_slot > 0)
            .then_some(state.startup_slot)
    }

    pub fn startup_touched_pubkeys(&self) -> Vec<Pubkey> {
        let Some(inner) = &self.0 else {
            return Vec::new();
        };
        inner.state().startup_touched.keys().copied().collect()
    }

    pub fn mark_bootstrap_failed(&self) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        let mut state = inner.state();
        if state.status != SupplyStatus::Bootstrapping || state.bootstrap_failed {
            return false;
        }
        state.bootstrap_failed = true;
        true
    }

    pub fn bootstrap_failed(&self) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        inner.state().bootstrap_failed
    }

    /// Folds the bootstrap window delta onto the anchor and flips Live. Called by
    /// the resolve after every startup touch has a startup balance.
    pub fn finish_bootstrap(
        &self,
        startup_balances: &HashMap<Pubkey, u64>,
    ) -> Option<SupplyCommit> {
        let inner = self.0.as_deref()?;
        let mut state = inner.state();
        if state.status != SupplyStatus::Bootstrapping
            || state.startup_slot == 0
            || state.bootstrap_failed
        {
            return None;
        }
        let startup_slot = state.startup_slot;
        let mut window_delta: i128 = 0;
        let mut max_slot = startup_slot;
        let mut zero_prev = HashSet::new();
        for (pubkey, account) in &state.startup_touched {
            if account.lamports == 0 {
                zero_prev.insert(*pubkey);
            }
            if account.slot <= startup_slot {
                continue;
            }
            let balance = *startup_balances.get(pubkey)?;
            window_delta += account.lamports as i128 - balance as i128;
            max_slot = max_slot.max(account.slot);
        }
        state.total = (state.total as i128 + window_delta) as u64;
        state.slot = state.slot.max(max_slot);
        state.startup_touched = HashMap::new();
        state.startup_zero_prev = zero_prev;
        state.status = SupplyStatus::Live;
        Some(inner.commit(&state))
    }

    pub fn is_gap_filling(&self) -> bool {
        let Some(inner) = &self.0 else { return false };
        inner.state().status == SupplyStatus::GapFilling
    }

    pub fn mark_gap(&self) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        let mut state = inner.state();
        if state.status != SupplyStatus::Live {
            return false;
        }
        state.status = SupplyStatus::GapFilling;
        true
    }

    pub fn finish_gap(&self) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.status == SupplyStatus::GapFilling {
            state.status = SupplyStatus::Live;
            // The repaired range is applied, so the gap-close skips are void.
            state.gap_closes.clear();
        }
    }

    pub fn mark_stale(&self) -> bool {
        self.0.as_deref().is_some_and(Inner::mark_stale)
    }

    /// Runs the cache sweep when it is due. Called from the slot watch off the
    /// block path. Updates the cache gauges.
    pub fn sweep_if_due(&self, slot: u64) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.cache.sweep_due(slot) {
            let started = Instant::now();
            let evicted = state.cache.sweep(slot);
            metrics::SUPPLY_SWEEP_MICROSECONDS.observe(started.elapsed().as_micros() as f64);
            tracing::debug!(target: "supply_cache", "swept {} unpinned entries at slot {}", evicted, slot);
        }
        inner.refresh_cache_gauges(&state.cache);
    }

    /// Folds a block's accounts into the per-block pending list. Empty when
    /// disabled. No dedup: the feed carries one version per pubkey per block.
    pub fn build_block_pending(&self, accounts: &[SubscribeUpdateAccountInfo]) -> Vec<Pending> {
        if !self.is_enabled() {
            return Vec::new();
        }
        accounts
            .iter()
            .map(|account| Pending {
                pubkey: Pubkey::try_from(account.pubkey.as_slice()).unwrap(),
                owner: Pubkey::try_from(account.owner.as_slice()).unwrap(),
                lamports: account.lamports,
            })
            .collect()
    }

    /// The per-block delta path, run together with the block's row writes.
    /// Phase one probes the cache under the state mutex. The miss read then
    /// resolves the rest. A live block's writes start after phase one and
    /// overlap the read. A repaired block's writes wait for the read, because
    /// that read is unbounded. Returns the outcome and the writes' result.
    pub async fn apply_block(
        &self,
        slot: u64,
        is_repaired: bool,
        pending: Vec<Pending>,
        stake: &StakeBlock,
        block_writes: impl Future<Output = bool>,
    ) -> (BlockOutcome, bool) {
        let Some(inner) = self.0.as_deref() else {
            return (BlockOutcome::Idle, block_writes.await);
        };
        let started = Instant::now();
        let _write_guard = inner.block_writes.lock().await;

        let (outcome, misses) = inner.probe_block(slot, is_repaired, pending, stake);

        let resolve = async {
            let BlockOutcome::Delta {
                mut delta,
                mut touched,
            } = outcome
            else {
                return outcome;
            };
            if !misses.is_empty() {
                metrics::SUPPLY_CACHE_MISSES_TOTAL.inc_by(misses.len() as u64);
                let pubkeys: Vec<Pubkey> = misses.iter().map(|p| p.pubkey).collect();
                let below_slot = (!is_repaired).then_some(slot);
                let prev_map = match inner.fetch_prev_balances(slot, &pubkeys, below_slot).await {
                    Some(map) => map,
                    None => return BlockOutcome::ReadFailed,
                };
                let mut state = inner.state();
                for p in misses {
                    let prev = prev_map.get(&p.pubkey).copied();
                    delta += state
                        .cache
                        .apply_miss(p.pubkey, p.lamports, slot, &p.owner, prev);
                    touched.push(p.pubkey);
                }
                inner.refresh_cache_gauges(&state.cache);
            }
            metrics::SUPPLY_BLOCK_MICROSECONDS.observe(started.elapsed().as_micros() as f64);
            BlockOutcome::Delta { delta, touched }
        };

        if is_repaired {
            let outcome = resolve.await;
            return (outcome, block_writes.await);
        }
        tokio::join!(resolve, block_writes)
    }

    /// Commits the block outcome and spawns the row upsert. A write failure
    /// while Live pins the touched set, or marks Stale past the pin cap. During
    /// bootstrap it poisons the seed.
    pub fn finish_block(&self, slot: u64, outcome: BlockOutcome, block_writes_ok: bool) {
        let Some(inner) = self.0.as_deref() else {
            return;
        };
        let commit = match outcome {
            BlockOutcome::Idle | BlockOutcome::ReadFailed => None,
            BlockOutcome::Bootstrapping => {
                if !block_writes_ok && self.mark_bootstrap_failed() {
                    tracing::error!(
                        target: "supply_tracker",
                        "account writes failed for slot {} during supply bootstrap, marking bootstrap failed",
                        slot
                    );
                }
                None
            }
            BlockOutcome::Delta { delta, touched } => {
                if !block_writes_ok {
                    let pinned = inner.state().cache.pin_failed(&touched);
                    if !pinned {
                        tracing::error!(
                            target: "supply_tracker",
                            "account writes failed for slot {} and the failure-pin cap is exceeded, marking supply stale",
                            slot
                        );
                        self.mark_stale();
                        return;
                    }
                    tracing::warn!(
                        target: "supply_tracker",
                        "account writes failed for slot {}, pinned {} accounts so the cache stays authoritative",
                        slot,
                        touched.len()
                    );
                }
                inner.commit_block(slot, delta)
            }
        };
        if let Some(commit) = commit {
            // The upsert is idempotent and keyed by slot, so it needs no await.
            let db = inner.db.clone();
            let query_timeout = inner.query_timeout;
            tokio::spawn(async move { persist_supply_row(&db, &commit, query_timeout).await });
        }
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, SupplyState> {
        self.state.lock().expect("Failed to lock supply state")
    }

    /// Phase one under the state mutex: hits fold in place, misses come back
    /// for the DB read. A bootstrap block only records touches, every one of
    /// them, because the window is resolved against the snapshot alone.
    fn probe_block(
        &self,
        slot: u64,
        is_repaired: bool,
        pending: Vec<Pending>,
        stake: &StakeBlock,
    ) -> (BlockOutcome, Vec<Pending>) {
        let started = Instant::now();
        let mut state = self.state();
        match state.status {
            SupplyStatus::Bootstrapping => {
                for p in &pending {
                    state
                        .cache
                        .probe(p.pubkey, p.lamports, slot, &p.owner, false);
                    if state
                        .startup_touched
                        .get(&p.pubkey)
                        .is_none_or(|touched| touched.slot <= slot)
                    {
                        let touched = TouchedAccount {
                            slot,
                            lamports: p.lamports,
                        };
                        state.startup_touched.insert(p.pubkey, touched);
                    }
                }
                self.refresh_cache_gauges(&state.cache);
                return (BlockOutcome::Bootstrapping, Vec::new());
            }
            SupplyStatus::Stale => return (BlockOutcome::Idle, Vec::new()),
            SupplyStatus::Live | SupplyStatus::GapFilling => {}
        }
        // A replayed live block was already counted.
        if !is_repaired && slot <= state.slot {
            return (BlockOutcome::Idle, Vec::new());
        }
        if !is_repaired && state.status == SupplyStatus::GapFilling {
            for p in pending.iter().filter(|p| p.lamports == 0) {
                let closed = state.gap_closes.entry(p.pubkey).or_insert(slot);
                *closed = (*closed).max(slot);
            }
        }

        let mut delta: i128 = stake.delta;
        let mut touched = Vec::with_capacity(pending.len());
        let mut misses = Vec::new();
        let mut hits = 0u64;
        for p in pending {
            if stake.handled.contains(&p.pubkey) {
                continue;
            }
            let zero_prev = state.startup_zero_prev.remove(&p.pubkey);
            if is_repaired
                && !zero_prev
                && state
                    .gap_closes
                    .get(&p.pubkey)
                    .is_some_and(|closed_slot| *closed_slot >= slot)
            {
                continue;
            }
            touched.push(p.pubkey);
            match state
                .cache
                .probe(p.pubkey, p.lamports, slot, &p.owner, zero_prev)
            {
                Probe::Hit(d) => {
                    delta += d;
                    hits += 1;
                }
                Probe::Miss => misses.push(p),
            }
        }
        metrics::SUPPLY_CACHE_HITS_TOTAL.inc_by(hits);
        metrics::SUPPLY_PROBE_MICROSECONDS.observe(started.elapsed().as_micros() as f64);
        (BlockOutcome::Delta { delta, touched }, misses)
    }

    /// The batched miss read, retried once. A second failure marks the tracker
    /// Stale and returns `None`.
    async fn fetch_prev_balances(
        &self,
        slot: u64,
        pubkeys: &[Pubkey],
        below_slot: Option<u64>,
    ) -> Option<HashMap<Pubkey, (u64, u64)>> {
        let read = || prev::fetch_prev_balances(&self.db, pubkeys, below_slot, self.query_timeout);
        match read().await {
            Ok(map) => return Some(map),
            Err(first) => {
                tracing::warn!(target: "supply_tracker", "miss read failed for slot {}, retrying: {:?}", slot, first);
            }
        }
        match read().await {
            Ok(map) => Some(map),
            Err(second) => {
                tracing::error!(target: "supply_tracker", "miss read failed twice for slot {}, marking stale: {:?}", slot, second);
                self.mark_stale();
                None
            }
        }
    }

    fn mark_stale(&self) -> bool {
        let mut state = self.state();
        if !matches!(state.status, SupplyStatus::Live | SupplyStatus::GapFilling) {
            return false;
        }
        state.status = SupplyStatus::Stale;
        true
    }

    /// Advances the running total and, when Live, returns the row to persist.
    fn commit_block(&self, slot: u64, block_delta: i128) -> Option<SupplyCommit> {
        let mut state = self.state();
        state.slot = state.slot.max(slot);
        if slot <= state.startup_slot {
            return None;
        }
        match state.status {
            SupplyStatus::Bootstrapping | SupplyStatus::Stale => None,
            SupplyStatus::GapFilling | SupplyStatus::Live => {
                state.total = (state.total as i128 + block_delta) as u64;
                if state.status == SupplyStatus::Live {
                    let commit = self.commit(&state);
                    Some(commit)
                } else {
                    None
                }
            }
        }
    }

    fn commit(&self, state: &SupplyState) -> SupplyCommit {
        SupplyCommit {
            slot: state.slot,
            total: state.total,
            non_circulating: self.non_circulating.total(),
        }
    }

    fn refresh_cache_gauges(&self, cache: &HotAccounts) {
        metrics::SUPPLY_CACHE_ENTRIES
            .with_label_values(&["pinned"])
            .set(cache.pinned_len() as i64);
        metrics::SUPPLY_CACHE_ENTRIES
            .with_label_values(&["hot"])
            .set(cache.hot_len() as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::STAKE_PROGRAM_ID;
    use crate::modules::non_circulating::CLOCK_SYSVAR_ID;
    use solana_program::clock::Clock;
    use solana_stake_interface::state::{Meta, StakeStateV2};

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn tracker(non_circulating: NonCirculatingTracker) -> SupplyTracker {
        SupplyTracker::new(
            DatabaseConnection::Disconnected,
            Duration::from_secs(1),
            1_000,
            100,
            non_circulating,
        )
    }

    fn live_tracker() -> SupplyTracker {
        let t = tracker(NonCirculatingTracker::default());
        t.set_startup_total(100, 1_000);
        t.finish_bootstrap(&HashMap::new()).expect("live");
        t
    }

    fn balances(pairs: &[(Pubkey, u64)]) -> HashMap<Pubkey, u64> {
        pairs.iter().copied().collect()
    }

    fn account(pubkey: Pubkey, owner: Pubkey, lamports: u64) -> SubscribeUpdateAccountInfo {
        let data = if owner == STAKE_PROGRAM_ID {
            bincode::serialize(&StakeStateV2::Initialized(Meta::default())).unwrap()
        } else {
            Vec::new()
        };
        SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            owner: owner.to_bytes().to_vec(),
            lamports,
            data,
            ..Default::default()
        }
    }

    #[test]
    fn finish_bootstrap_window_delta_and_zero_prev() {
        let t = tracker(NonCirculatingTracker::default());
        t.set_startup_total(100, 1_000);
        // A touch above the anchor slot and a close inside the window.
        {
            let inner = t.0.as_deref().unwrap();
            let mut state = inner.state();
            state.startup_touched.insert(
                pk(1),
                TouchedAccount {
                    slot: 105,
                    lamports: 300,
                },
            );
            state.startup_touched.insert(
                pk(2),
                TouchedAccount {
                    slot: 106,
                    lamports: 0,
                },
            );
        }
        let commit = t
            .finish_bootstrap(&balances(&[(pk(1), 200), (pk(2), 50)]))
            .expect("bootstrap commits");
        // total = 1000 + (300 - 200) + (0 - 50) = 1050.
        assert_eq!(commit.total, 1050);
        assert!(
            t.0.as_deref()
                .unwrap()
                .state()
                .startup_zero_prev
                .contains(&pk(2))
        );
    }

    #[test]
    fn stale_tracker_commits_nothing() {
        let t = live_tracker();
        assert!(t.mark_stale());
        t.finish_block(
            200,
            BlockOutcome::Delta {
                delta: 5,
                touched: vec![],
            },
            true,
        );
        assert_eq!(t.summary().unwrap().total, 1_000);
    }

    #[test]
    fn replayed_live_block_is_idle_and_repaired_block_is_not() {
        let t = live_tracker();
        let inner = t.0.as_deref().unwrap();
        inner.commit_block(200, 0);
        let pending = vec![Pending {
            pubkey: pk(1),
            owner: pk(9),
            lamports: 5,
        }];
        let stake = StakeBlock::default();
        assert!(matches!(
            inner.probe_block(200, false, pending.clone(), &stake).0,
            BlockOutcome::Idle
        ));
        assert!(matches!(
            inner.probe_block(150, true, pending, &stake).0,
            BlockOutcome::Delta { .. }
        ));
    }

    #[test]
    fn gap_transitions_and_finish_clears_closes() {
        let t = live_tracker();
        assert!(t.mark_gap());
        assert!(t.is_gap_filling());
        {
            let inner = t.0.as_deref().unwrap();
            inner.state().gap_closes.insert(pk(3), 150);
        }
        t.finish_gap();
        assert!(!t.is_gap_filling());
        assert!(t.0.as_deref().unwrap().state().gap_closes.is_empty());
    }

    #[test]
    fn commit_block_gated_on_status_and_startup_slot() {
        let t = tracker(NonCirculatingTracker::default());
        t.set_startup_total(100, 1_000);
        let inner = t.0.as_deref().unwrap();
        // At or below the anchor: no commit.
        assert!(inner.commit_block(100, 500).is_none());
        // Bootstrapping: no commit.
        assert!(inner.commit_block(101, 500).is_none());
        t.finish_bootstrap(&HashMap::new()).expect("live");
        // Live: commits and advances the total.
        let commit = inner.commit_block(200, 25).expect("commit");
        assert_eq!(commit.total, 1_025);
    }

    #[test]
    fn bootstrap_records_every_touch_and_ignores_stake_deltas() {
        let t = tracker(NonCirculatingTracker::default());
        t.set_startup_total(100, 1_000);
        let inner = t.0.as_deref().unwrap();
        let stake = StakeBlock {
            delta: 500,
            handled: [pk(1)].into_iter().collect(),
            ..StakeBlock::default()
        };
        let pending = vec![Pending {
            pubkey: pk(1),
            owner: STAKE_PROGRAM_ID,
            lamports: 700,
        }];
        assert!(matches!(
            inner.probe_block(105, false, pending, &stake).0,
            BlockOutcome::Bootstrapping
        ));
        let commit = t.finish_bootstrap(&balances(&[(pk(1), 200)])).unwrap();
        assert_eq!(commit.total, 1_500);
    }

    /// Walks one pubkey from system to stake ownership and back, applying each
    /// block to both trackers, and checks the supply delta counts every
    /// transition exactly once. A miss resolves against the given DB row.
    #[test]
    fn ownership_transfer_counts_one_delta_per_transition() {
        let nc = NonCirculatingTracker::new();
        nc.seed_account(
            &CLOCK_SYSVAR_ID,
            &Pubkey::default(),
            1,
            &bincode::serialize(&Clock::default()).unwrap(),
            100,
        );
        assert!(nc.finish_bootstrap());
        let t = tracker(nc.clone());
        t.set_startup_total(100, 1_000);
        t.finish_bootstrap(&HashMap::new()).expect("live");
        let inner = t.0.as_deref().unwrap();
        let system = Pubkey::default();

        let apply = |slot: u64, owner: Pubkey, lamports: u64, prev: Option<(u64, u64)>| {
            let stake = nc.apply_block(slot, false, &[account(pk(1), owner, lamports)]);
            let pending = t.build_block_pending(&[account(pk(1), owner, lamports)]);
            let (outcome, misses) = inner.probe_block(slot, false, pending, &stake);
            let BlockOutcome::Delta { mut delta, .. } = outcome else {
                panic!("expected a delta");
            };
            for miss in misses {
                delta += inner.state().cache.apply_miss(
                    miss.pubkey,
                    miss.lamports,
                    slot,
                    &miss.owner,
                    prev,
                );
            }
            inner.commit_block(slot, delta);
            delta
        };

        // New system account: a miss with no row counts the full balance.
        assert_eq!(apply(101, system, 100, None), 100);
        // Becomes a stake account: the cache hit counts zero, then hands over.
        assert_eq!(apply(102, STAKE_PROGRAM_ID, 100, None), 0);
        assert!(t.entry(&pk(1)).is_none());
        // A reward while stake-owned comes from the stake map.
        assert_eq!(apply(103, STAKE_PROGRAM_ID, 150, None), 50);
        // The close is the stake map's too.
        assert_eq!(apply(104, system, 0, None), -150);
        // Recreated as a system account: a miss against the close row.
        assert_eq!(apply(105, system, 30, Some((0, 104))), 30);
        assert_eq!(t.summary().unwrap().total, 1_030);
    }
}
