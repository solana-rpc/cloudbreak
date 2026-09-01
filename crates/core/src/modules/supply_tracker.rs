// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! In-memory supply tracker. Holds the running total-supply figure, the
//! bootstrap state machine (snapshot anchor, startup touches, gap filling),
//! and the non-circulating member balances that back `getSupply`.

pub use crate::modules::non_circulating::NonCirculatingBalance;
use solana_pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

pub const SUPPLY_RING_SLOTS: u64 = 128;

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

#[derive(Clone, Default)]
pub struct SupplyTracker(Option<Arc<Inner>>);

struct Inner {
    state: Mutex<SupplyState>,
    non_circulating: RwLock<NonCirculatingState>,
    block_writes: tokio::sync::Mutex<()>,
}

#[derive(Clone, Copy, Default, PartialEq)]
enum SupplyStatus {
    #[default]
    Bootstrapping,
    Live,
    GapFilling,
    Stale,
}

#[derive(Default)]
struct SupplyState {
    status: SupplyStatus,
    bootstrap_failed: bool,
    total: u64,
    slot: u64,
    startup_slot: u64,
    startup_touched: HashMap<Pubkey, TouchedAccount>,
    startup_zero_prev: HashSet<Pubkey>,
    gap_closes: HashMap<Pubkey, u64>,
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
    pub fn new() -> Self {
        Self(Some(Arc::new(Inner {
            state: Mutex::new(SupplyState::default()),
            non_circulating: RwLock::new(NonCirculatingState::default()),
            block_writes: tokio::sync::Mutex::new(()),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub fn is_tracking_deltas(&self) -> bool {
        let Some(inner) = &self.0 else { return false };
        matches!(
            inner.state().status,
            SupplyStatus::Live | SupplyStatus::GapFilling
        )
    }

    pub async fn lock_block_writes(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        let inner = self.0.as_deref()?;
        Some(inner.block_writes.lock().await)
    }

    pub fn is_non_circulating(&self, pubkey: &Pubkey) -> bool {
        let Some(inner) = &self.0 else { return false };
        inner.non_circulating_read().is_member(pubkey)
    }

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

    pub fn record_startup_touches(
        &self,
        slot: u64,
        touches: impl IntoIterator<Item = (Pubkey, u64, u64)>,
    ) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.status != SupplyStatus::Bootstrapping {
            return;
        }
        for (pubkey, lamports, write_version) in touches {
            let account = TouchedAccount {
                slot,
                write_version,
                lamports,
            };
            let entry = state.startup_touched.entry(pubkey).or_insert(account);
            if (slot, write_version) > (entry.slot, entry.write_version) {
                *entry = account;
            }
        }
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

    pub fn take_zero_prev(&self, pubkey: &Pubkey) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        inner.state().startup_zero_prev.remove(pubkey)
    }

    pub fn is_gap_filling(&self) -> bool {
        let Some(inner) = &self.0 else { return false };
        inner.state().status == SupplyStatus::GapFilling
    }

    pub fn record_gap_closes(&self, slot: u64, closed_accounts: &[Vec<u8>]) {
        let Some(inner) = &self.0 else { return };
        let mut state = inner.state();
        if state.status != SupplyStatus::GapFilling {
            return;
        }
        for pubkey in closed_accounts {
            let pubkey = Pubkey::try_from(pubkey.as_slice()).unwrap();
            let entry = state.gap_closes.entry(pubkey).or_insert(slot);
            *entry = (*entry).max(slot);
        }
    }

    pub fn gap_close_floor(&self, pubkey: &Pubkey) -> Option<u64> {
        let inner = self.0.as_deref()?;
        let state = inner.state();
        state.gap_closes.get(pubkey).copied()
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
        true
    }

    pub fn commit_block(&self, slot: u64, block_delta: i128) -> Option<SupplyCommit> {
        let inner = self.0.as_deref()?;
        let mut state = inner.state();
        state.slot = state.slot.max(slot);
        if slot <= state.startup_slot {
            return None;
        }
        match state.status {
            SupplyStatus::Bootstrapping | SupplyStatus::Stale => None,
            SupplyStatus::GapFilling | SupplyStatus::Live => {
                state.total = (state.total as i128 + block_delta) as u64;
                (state.status == SupplyStatus::Live).then(|| inner.commit(&state))
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
}
