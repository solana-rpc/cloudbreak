// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The supply tracker: the running total, the status machine (Bootstrapping,
//! Live, GapFilling, Stale), the bootstrap window, the gap-close set, the
//! non-circulating member balances, and the hot-accounts cache.
//!
//! The block path calls [`SupplyTracker::apply_block`] then
//! [`SupplyTracker::finish_block`]. The delta source is the hot cache plus a
//! by-pubkey miss read, not the owner map.

pub use crate::modules::non_circulating::NonCirculatingBalance;
use crate::metrics;
use crate::modules::supply::cache::{HotAccounts, Probe};
use crate::modules::supply::prev;
use sea_orm::DatabaseConnection;
use solana_pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

pub const SUPPLY_RING_SLOTS: u64 = 128;

/// One account's deduplicated update in a block, carrying the highest write
/// version seen for that pubkey.
#[derive(Clone, Copy, Debug)]
pub struct Pending {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub write_version: u64,
}

#[derive(Clone, Copy, Debug)]
struct MemberBalance {
    slot: u64,
    lamports: u64,
}

#[derive(Clone, Copy, Debug)]
struct TouchedAccount {
    slot: u64,
    write_version: u64,
    lamports: u64,
}

/// The outcome of [`SupplyTracker::apply_block`], consumed by
/// [`SupplyTracker::finish_block`].
pub enum BlockOutcome {
    /// Disabled, or the slot is at or below the anchor: nothing to commit.
    Idle,
    /// A bootstrap-window block: touches recorded and the cache warmed.
    Bootstrapping,
    /// A live or gap-filling block whose delta was computed.
    Delta { delta: i128, touched: Vec<Pubkey> },
    /// The miss read failed after a retry; the tracker is already Stale.
    ReadFailed,
}

#[derive(Clone, Default)]
pub struct SupplyTracker(Option<Arc<Inner>>);

struct Inner {
    state: Mutex<SupplyState>,
    non_circulating: RwLock<NonCirculatingState>,
    block_writes: tokio::sync::Mutex<()>,
    /// Whether every stake account is pinned resident.
    pin_stake: bool,
    /// Aggregated save_block supply timing, logged every SUPPLY_TIMING_WINDOW blocks.
    timing: Mutex<LoopTiming>,
}

/// Rolling min/max/mean of the supply work's per-block cost inside the
/// save_block loop. Logged and reset every [`SUPPLY_TIMING_WINDOW`] blocks so
/// the cost is visible without a line per block.
#[derive(Default)]
struct LoopTiming {
    count: u32,
    first_slot: u64,
    min_ms: f64,
    max_ms: f64,
    sum_ms: f64,
}

const SUPPLY_TIMING_WINDOW: u32 = 50;

#[derive(Clone, Copy, Default, PartialEq)]
enum SupplyStatus {
    #[default]
    Bootstrapping,
    Live,
    GapFilling,
    Stale,
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

#[derive(Default)]
struct NonCirculatingState {
    members: Option<HashSet<Pubkey>>,
    balances: HashMap<Pubkey, MemberBalance>,
}

impl NonCirculatingState {
    fn is_member(&self, pubkey: &Pubkey) -> bool {
        self.members
            .as_ref()
            .is_some_and(|members| members.contains(pubkey))
    }
}

#[derive(Debug, Clone)]
pub struct SupplyCommit {
    pub slot: u64,
    pub total: u64,
    pub non_circulating: Option<u64>,
}

impl SupplyTracker {
    /// Builds an enabled tracker with a pre-sized cache. `cap` is the unpinned
    /// cap, `buckets` the pre-sized bucket target, `fail_pin_cap` the live
    /// write-failure pin budget, `pin_stake` whether stake accounts are pinned.
    pub fn new(buckets: usize, cap: usize, fail_pin_cap: usize, pin_stake: bool) -> Self {
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
                cache: HotAccounts::with_capacity(buckets, cap, fail_pin_cap, pin_stake),
            }),
            non_circulating: RwLock::new(NonCirculatingState::default()),
            block_writes: tokio::sync::Mutex::new(()),
            pin_stake,
            timing: Mutex::new(LoopTiming::default()),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub async fn lock_block_writes(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        let inner = self.0.as_deref()?;
        Some(inner.block_writes.lock().await)
    }

    /// Records the wall time the supply work took in the save_block loop for one
    /// block. Every [`SUPPLY_TIMING_WINDOW`] blocks it logs the min, max, and
    /// mean over that window and resets, so per-block cost is observable in the
    /// logs without a line every block. Off the hot lock; no-op when disabled.
    pub fn observe_loop_time(&self, slot: u64, elapsed: std::time::Duration) {
        let Some(inner) = self.0.as_deref() else {
            return;
        };
        let ms = elapsed.as_secs_f64() * 1000.0;
        let mut t = inner.timing.lock().expect("Failed to lock supply timing");
        if t.count == 0 {
            t.first_slot = slot;
            t.min_ms = ms;
            t.max_ms = ms;
        } else {
            t.min_ms = t.min_ms.min(ms);
            t.max_ms = t.max_ms.max(ms);
        }
        t.sum_ms += ms;
        t.count += 1;
        if t.count >= SUPPLY_TIMING_WINDOW {
            tracing::info!(
                target: "supply_tracker",
                "supply save_block time over last {} blocks (slots {}..{}): min {:.2} ms, max {:.2} ms, mean {:.2} ms",
                t.count,
                t.first_slot,
                slot,
                t.min_ms,
                t.max_ms,
                t.sum_ms / t.count as f64,
            );
            *t = LoopTiming::default();
        }
    }

    pub fn is_non_circulating(&self, pubkey: &Pubkey) -> bool {
        let Some(inner) = &self.0 else { return false };
        inner.non_circulating_read().is_member(pubkey)
    }

    /// Feeds one non-circulating member's balance, guarded on the slot so an
    /// older update never clobbers newer state.
    pub fn observe_account(&self, pubkey: Pubkey, slot: u64, lamports: u64) {
        let Some(inner) = &self.0 else { return };
        if !inner.non_circulating_read().is_member(&pubkey) {
            return;
        }

        let mut non_circulating = inner.non_circulating_write();
        if !non_circulating.is_member(&pubkey) {
            return;
        }
        if non_circulating
            .balances
            .get(&pubkey)
            .is_none_or(|entry| entry.slot <= slot)
        {
            non_circulating
                .balances
                .insert(pubkey, MemberBalance { slot, lamports });
        }
    }

    pub fn set_non_circulating_accounts(
        &self,
        accounts: Vec<Pubkey>,
        balances: Vec<NonCirculatingBalance>,
    ) {
        let Some(inner) = &self.0 else { return };
        let members: HashSet<Pubkey> = accounts.into_iter().collect();
        let mut non_circulating = inner.non_circulating_write();
        non_circulating
            .balances
            .retain(|pubkey, _| members.contains(pubkey));
        for balance in balances {
            if !members.contains(&balance.pubkey) {
                continue;
            }
            if non_circulating
                .balances
                .get(&balance.pubkey)
                .is_none_or(|entry| entry.slot < balance.slot)
            {
                non_circulating.balances.insert(
                    balance.pubkey,
                    MemberBalance {
                        slot: balance.slot,
                        lamports: balance.lamports,
                    },
                );
            }
        }
        non_circulating.members = Some(members);
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

    /// Whether every stake account is pinned resident (config `pin-stake-accounts`).
    pub fn pins_stake(&self) -> bool {
        self.0.as_deref().is_some_and(|inner| inner.pin_stake)
    }

    /// Seeds one stake account from the snapshot into the pinned set. No-op when
    /// disabled or when stake pinning is off.
    pub fn seed_stake_account(&self, pubkey: Pubkey, lamports: u64, slot: u64, write_version: u64) {
        let Some(inner) = &self.0 else { return };
        if !inner.pin_stake {
            return;
        }
        inner
            .state()
            .cache
            .seed_stake_account(pubkey, lamports, slot, write_version);
    }

    /// Folds the recomputer's stake scan into the pinned set and drops old
    /// pinned tombstones. No-op when disabled or when stake pinning is off.
    pub fn refresh_pinned(
        &self,
        rows: impl IntoIterator<Item = (Pubkey, u64, u64, u64)>,
        scan_slot: u64,
    ) {
        let Some(inner) = &self.0 else { return };
        if !inner.pin_stake {
            return;
        }
        inner.state().cache.refresh_pinned(rows, scan_slot);
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
        inner.set_status_metric(&state);
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
        inner.set_status_metric(&state);
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
        inner.set_status_metric(&state);
        true
    }

    pub fn finish_gap(&self) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.status == SupplyStatus::GapFilling {
            state.status = SupplyStatus::Live;
            // The repaired range is applied. Its gap-close skips no longer hold.
            state.gap_closes.clear();
            inner.set_status_metric(&state);
        }
    }

    pub fn mark_stale(&self) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        let mut state = inner.state();
        if !matches!(state.status, SupplyStatus::Live | SupplyStatus::GapFilling) {
            return false;
        }
        state.status = SupplyStatus::Stale;
        inner.set_status_metric(&state);
        true
    }

    /// Runs the cache sweep when it is due. Called from the slot watch off the
    /// block path. Updates the cache gauges.
    pub fn sweep_if_due(&self, slot: u64) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.cache.sweep_due(slot) {
            let evicted = state.cache.sweep(slot);
            tracing::debug!(target: "supply_cache", "swept {} unpinned entries at slot {}", evicted, slot);
        }
        inner.refresh_cache_gauges(&state.cache);
    }

    /// The per-block delta path. Runs under the block-writes lock, releases it
    /// before returning so the caller's inserts and the next block's miss read
    /// run outside it. See the module rustdoc for the lock protocol.
    pub async fn apply_block(
        &self,
        slot: u64,
        is_repaired: bool,
        pending: Vec<Pending>,
        closed: &[Vec<u8>],
        db: &DatabaseConnection,
        query_timeout: Duration,
    ) -> BlockOutcome {
        let Some(inner) = self.0.as_deref() else {
            return BlockOutcome::Idle;
        };
        let _write_guard = inner.block_writes.lock().await;
        let start = std::time::Instant::now();

        // Non-circulating member balances come from the deduped set, guarded on slot.
        for p in &pending {
            self.observe_account(p.pubkey, slot, p.lamports);
        }

        // Phase one: hits under the state mutex, misses collected for the DB read.
        let (mut delta, mut touched, misses) = {
            let mut state = inner.state();

            if !is_repaired && state.status == SupplyStatus::GapFilling {
                for pubkey in closed {
                    let pubkey = Pubkey::try_from(pubkey.as_slice()).unwrap();
                    let entry = state.gap_closes.entry(pubkey).or_insert(slot);
                    *entry = (*entry).max(slot);
                }
            }

            match state.status {
                SupplyStatus::Bootstrapping => {
                    for p in &pending {
                        state.cache.probe(
                            p.pubkey,
                            p.lamports,
                            slot,
                            p.write_version,
                            &p.owner,
                            false,
                        );
                    }
                    for p in &pending {
                        let account = TouchedAccount {
                            slot,
                            write_version: p.write_version,
                            lamports: p.lamports,
                        };
                        let entry = state.startup_touched.entry(p.pubkey).or_insert(account);
                        if (slot, p.write_version) > (entry.slot, entry.write_version) {
                            *entry = account;
                        }
                    }
                    inner.refresh_cache_gauges(&state.cache);
                    return BlockOutcome::Bootstrapping;
                }
                SupplyStatus::Stale => return BlockOutcome::Idle,
                SupplyStatus::Live | SupplyStatus::GapFilling => {}
            }

            let mut delta: i128 = 0;
            let mut touched = Vec::with_capacity(pending.len());
            let mut misses = Vec::new();
            let mut hits = 0u64;
            for p in pending {
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
                match state.cache.probe(
                    p.pubkey,
                    p.lamports,
                    slot,
                    p.write_version,
                    &p.owner,
                    zero_prev,
                ) {
                    Probe::Hit(d) => {
                        delta += d;
                        hits += 1;
                    }
                    Probe::Miss => misses.push(p),
                }
            }
            metrics::SUPPLY_CACHE_HITS_TOTAL.inc_by(hits);
            (delta, touched, misses)
        };

        // Phase two: resolve misses with one batched by-pubkey read, retried once.
        if !misses.is_empty() {
            metrics::SUPPLY_CACHE_MISSES_TOTAL.inc_by(misses.len() as u64);
            let pubkeys: Vec<Pubkey> = misses.iter().map(|p| p.pubkey).collect();
            let read_start = std::time::Instant::now();
            let prev_map = match prev::fetch_prev_balances(db, &pubkeys, query_timeout).await {
                Ok(map) => map,
                Err(first) => {
                    tracing::warn!(target: "supply_tracker", "miss read failed for slot {}, retrying: {:?}", slot, first);
                    match prev::fetch_prev_balances(db, &pubkeys, query_timeout).await {
                        Ok(map) => map,
                        Err(second) => {
                            tracing::error!(target: "supply_tracker", "miss read failed twice for slot {}, marking stale: {:?}", slot, second);
                            metrics::SUPPLY_QUERY_ERRORS.inc();
                            self.mark_stale();
                            return BlockOutcome::ReadFailed;
                        }
                    }
                }
            };
            metrics::SUPPLY_MISS_READ_SECONDS.observe(read_start.elapsed().as_secs_f64());

            let mut state = inner.state();
            for p in misses {
                let prev = prev::prev_row(&prev_map, &p.pubkey);
                delta += state
                    .cache
                    .apply_miss(p.pubkey, p.lamports, slot, p.write_version, &p.owner, prev);
                touched.push(p.pubkey);
            }
            inner.refresh_cache_gauges(&state.cache);
        }

        metrics::SUPPLY_DELTA_SECONDS.observe(start.elapsed().as_secs_f64());
        BlockOutcome::Delta { delta, touched }
    }

    /// Commits the block outcome. On a write failure while Live it pins the
    /// block's touched set so the DB is never consulted for it; if the pin cap is
    /// exceeded it marks Stale. During bootstrap a write failure poisons it.
    pub fn finish_block(&self, slot: u64, outcome: BlockOutcome, block_writes_ok: bool) -> Option<SupplyCommit> {
        let inner = self.0.as_deref()?;
        match outcome {
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
                    metrics::SUPPLY_WRITE_FAILURES_TOTAL.inc();
                    let pinned = inner.state().cache.pin_failed(&touched);
                    if !pinned {
                        tracing::error!(
                            target: "supply_tracker",
                            "account writes failed for slot {} and the failure-pin cap is exceeded, marking supply stale",
                            slot
                        );
                        self.mark_stale();
                        return None;
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
        }
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, SupplyState> {
        self.state.lock().expect("Failed to lock supply state")
    }

    fn non_circulating_read(&self) -> RwLockReadGuard<'_, NonCirculatingState> {
        self.non_circulating
            .read()
            .expect("Failed to read non-circulating state")
    }

    fn non_circulating_write(&self) -> RwLockWriteGuard<'_, NonCirculatingState> {
        self.non_circulating
            .write()
            .expect("Failed to write non-circulating state")
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
                    metrics::SUPPLY_TOTAL_LAMPORTS.set(commit.total as i64);
                    metrics::SUPPLY_SLOT.set(commit.slot as i64);
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
            non_circulating: self.sum_non_circulating(),
        }
    }

    fn sum_non_circulating(&self) -> Option<u64> {
        let non_circulating = self.non_circulating_read();
        non_circulating.members.as_ref()?;
        let lamports: u128 = non_circulating
            .balances
            .values()
            .map(|balance| balance.lamports as u128)
            .sum();
        Some(lamports as u64)
    }

    fn set_status_metric(&self, state: &SupplyState) {
        let code = if state.bootstrap_failed {
            4
        } else {
            match state.status {
                SupplyStatus::Bootstrapping => 0,
                SupplyStatus::Live => 1,
                SupplyStatus::GapFilling => 2,
                SupplyStatus::Stale => 3,
            }
        };
        metrics::SUPPLY_STATUS.set(code);
    }

    fn refresh_cache_gauges(&self, cache: &HotAccounts) {
        metrics::SUPPLY_CACHE_ENTRIES
            .with_label_values(&["pinned"])
            .set(cache.pinned_len() as i64);
        metrics::SUPPLY_CACHE_ENTRIES
            .with_label_values(&["hot"])
            .set(cache.hot_len() as i64);
        metrics::SUPPLY_CACHE_BUCKETS.set(cache.bucket_count() as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn tracker() -> SupplyTracker {
        SupplyTracker::new(64, 1_000, 100, true)
    }

    fn balances(pairs: &[(Pubkey, u64)]) -> HashMap<Pubkey, u64> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn finish_bootstrap_window_delta_and_zero_prev() {
        let t = tracker();
        t.set_startup_total(100, 1_000);
        // A touch above the anchor slot and a close inside the window.
        {
            let inner = t.0.as_deref().unwrap();
            let mut state = inner.state();
            state.startup_touched.insert(
                pk(1),
                TouchedAccount { slot: 105, write_version: 1, lamports: 300 },
            );
            state.startup_touched.insert(
                pk(2),
                TouchedAccount { slot: 106, write_version: 1, lamports: 0 },
            );
        }
        let commit = t
            .finish_bootstrap(&balances(&[(pk(1), 200), (pk(2), 50)]))
            .expect("bootstrap commits");
        // total = 1000 + (300 - 200) + (0 - 50) = 1050.
        assert_eq!(commit.total, 1050);
        // The closed account seeds zero_prev.
        assert!(t.0.as_deref().unwrap().state().startup_zero_prev.contains(&pk(2)));
    }

    #[tokio::test]
    async fn stale_after_read_failure_returns_no_delta() {
        let t = tracker();
        t.set_startup_total(100, 1_000);
        // Flip to Live with no touches.
        t.finish_bootstrap(&HashMap::new()).expect("live");
        // A miss with no DB connection can't be exercised here; instead assert the
        // status machine: mark_stale then commit_block is a no-op.
        assert!(t.mark_stale());
        assert!(t.finish_block(200, BlockOutcome::Delta { delta: 5, touched: vec![] }, true).is_none());
    }

    #[test]
    fn gap_transitions_and_finish_clears_closes() {
        let t = tracker();
        t.set_startup_total(100, 1_000);
        t.finish_bootstrap(&HashMap::new()).expect("live");
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
        let t = tracker();
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
    fn non_circulating_sum_and_observe_guard() {
        let t = tracker();
        t.set_non_circulating_accounts(
            vec![pk(1), pk(2)],
            vec![
                NonCirculatingBalance { pubkey: pk(1), slot: 10, lamports: 100 },
                NonCirculatingBalance { pubkey: pk(2), slot: 10, lamports: 200 },
            ],
        );
        // An older observation does not clobber.
        t.observe_account(pk(1), 5, 999);
        t.observe_account(pk(1), 12, 150);
        let inner = t.0.as_deref().unwrap();
        assert_eq!(inner.sum_non_circulating(), Some(350));
    }
}
