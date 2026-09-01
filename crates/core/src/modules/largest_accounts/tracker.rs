//! In-memory reservoir state: per-mint tops, the seed/bootstrap path, and
//! apply_block.

use super::{
    CIRCULATING_SENTINEL_MINT, MintRecord, NON_CIRCULATING_SENTINEL_MINT, SOL_SENTINEL_MINT,
    TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID, is_sentinel,
};
use crate::modules::non_circulating::{NonCirculatingBalance, NonCirculatingTracker};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

/// One account's post-block state as collected by the ingest path, before it is
/// routed into the token, SOL, and circulating-class tops by `apply_block`.
pub struct PendingLargestAccount {
    pub write_version: u64,
    pub lamports: u64,
    pub non_circulating: bool,
    pub token: Option<(Pubkey, u64)>,
}

/// What the caller must persist after applying a block: changed records to
/// upsert, mints that just became unservable, and mints whose rows should be
/// deleted because their top emptied.
#[derive(Debug, Default)]
pub struct BlockOutcome {
    pub records: Vec<MintRecord>,
    pub newly_stale: Vec<Pubkey>,
    pub cleared: Vec<Pubkey>,
}

/// A tracked holder's amount stamped with the (slot, write_version) that set it,
/// so stale out-of-order updates never clobber newer state.
#[derive(Clone, Copy)]
struct Entry {
    amount: u64,
    slot: u64,
    write_version: u64,
}

/// One mint's (or sentinel's) live top-N holder set, with the bookkeeping needed
/// to know when the set is no longer provably correct (`dropped_floor` / `stale`).
#[derive(Default)]
struct MintTop {
    entries: HashMap<Pubkey, Entry>,
    cached_min: Option<(Pubkey, u64)>,
    dropped_floor: u64,
    stale: bool,
    last_record: Vec<(Pubkey, u64)>,
}

/// Result of extracting a mint's persisted top-N: `Sound` rows safe to serve, or
/// `Unsound` when evictions mean the true top-N can no longer be proven.
enum ComputedTop {
    Sound(Vec<(Pubkey, u64)>),
    Unsound,
}

impl MintTop {
    fn min_entry(&self) -> Option<(Pubkey, u64)> {
        self.entries
            .iter()
            .min_by_key(|(pubkey, entry)| (entry.amount, *pubkey))
            .map(|(pubkey, entry)| (*pubkey, entry.amount))
    }

    fn refresh_cached_min(&mut self) {
        self.cached_min = self.min_entry();
    }

    fn compute_top(&self) -> ComputedTop {
        let mut rows: Vec<(Pubkey, u64)> = self
            .entries
            .iter()
            .map(|(pubkey, entry)| (*pubkey, entry.amount))
            .collect();
        rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        rows.truncate(super::PERSISTED_TOP_N);
        if rows.len() < super::PERSISTED_TOP_N && self.dropped_floor > 0 {
            return ComputedTop::Unsound;
        }
        if let Some(last) = rows.last()
            && last.1 < self.dropped_floor
        {
            return ComputedTop::Unsound;
        }
        ComputedTop::Sound(rows)
    }

    fn latch_stale(&mut self) -> bool {
        if self.stale {
            return false;
        }
        self.stale = true;
        self.last_record = Vec::new();
        true
    }
}

/// Tracker lifecycle: `Bootstrapping` while snapshot seeds are still merging,
/// `Live` once records may be emitted.
#[derive(Clone, Copy, Default, PartialEq)]
enum Status {
    #[default]
    Bootstrapping,
    Live,
}

/// All mutable tracker state behind the mutex: per-mint tops, the reverse
/// holder→mint index, and the bootstrap-only tombstones and seed reservoir.
#[derive(Default)]
struct TrackerState {
    status: Status,
    max_applied_slot: u64,
    mints: HashMap<Pubkey, MintTop>,
    member_index: HashMap<Pubkey, Pubkey>,
    tombstones: HashMap<Pubkey, (u64, u64)>,
    seed_reservoir: HashMap<Pubkey, Vec<SeedAccount>>,
}

impl TrackerState {
    fn record_tombstone(&mut self, pubkey: Pubkey, slot: u64, write_version: u64) {
        let entry = self.tombstones.entry(pubkey).or_insert((slot, write_version));
        if (slot, write_version) > *entry {
            *entry = (slot, write_version);
        }
    }

    fn remove_member(
        &mut self,
        mint: Pubkey,
        pubkey: Pubkey,
        slot: u64,
        write_version: u64,
        touched: &mut HashSet<Pubkey>,
    ) {
        let Some(top) = self.mints.get_mut(&mint) else {
            return;
        };
        let Some(entry) = top.entries.get(&pubkey) else {
            return;
        };
        if (slot, write_version) > (entry.slot, entry.write_version) {
            top.entries.remove(&pubkey);
            if top.cached_min.is_some_and(|(min_pubkey, _)| min_pubkey == pubkey) {
                top.refresh_cached_min();
            }
            if !is_sentinel(&mint) {
                self.member_index.remove(&pubkey);
            }
            touched.insert(mint);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_update(
        &mut self,
        mint: Pubkey,
        pubkey: Pubkey,
        amount: u64,
        slot: u64,
        write_version: u64,
        bootstrapping: bool,
        old_block: bool,
        k: usize,
        touched: &mut HashSet<Pubkey>,
    ) {
        if bootstrapping
            && let Some(tombstone) = self.tombstones.get(&pubkey)
            && *tombstone >= (slot, write_version)
        {
            return;
        }
        let Some(top) = self.mints.get_mut(&mint) else {
            return;
        };
        if let Some(entry) = top.entries.get_mut(&pubkey) {
            if (slot, write_version) > (entry.slot, entry.write_version) {
                *entry = Entry {
                    amount,
                    slot,
                    write_version,
                };
                match top.cached_min {
                    Some((min_pubkey, min_amount)) => {
                        if min_pubkey == pubkey {
                            top.refresh_cached_min();
                        } else if (amount, pubkey) < (min_amount, min_pubkey) {
                            top.cached_min = Some((pubkey, amount));
                        }
                    }
                    None => top.cached_min = Some((pubkey, amount)),
                }
                touched.insert(mint);
            }
            return;
        }
        if old_block {
            if amount > top.dropped_floor {
                top.dropped_floor = amount;
                touched.insert(mint);
            }
            return;
        }
        let mut min_evicted = false;
        if !bootstrapping && top.entries.len() >= k {
            let (min_pubkey, min_amount) = top.cached_min.expect("non-empty full map");
            if amount <= min_amount {
                if amount > top.dropped_floor {
                    top.dropped_floor = amount;
                    touched.insert(mint);
                }
                return;
            }
            top.entries.remove(&min_pubkey);
            min_evicted = true;
            if !is_sentinel(&mint) {
                self.member_index.remove(&min_pubkey);
            }
            top.dropped_floor = top.dropped_floor.max(min_amount);
        }
        top.entries.insert(
            pubkey,
            Entry {
                amount,
                slot,
                write_version,
            },
        );
        if min_evicted {
            top.refresh_cached_min();
        } else {
            match top.cached_min {
                Some((min_pubkey, min_amount)) => {
                    if (amount, pubkey) < (min_amount, min_pubkey) {
                        top.cached_min = Some((pubkey, amount));
                    }
                }
                None => top.cached_min = Some((pubkey, amount)),
            }
        }
        if !is_sentinel(&mint) {
            self.member_index.insert(pubkey, mint);
        }
        touched.insert(mint);
    }

    fn trim(&mut self, mint: Pubkey, k: usize) {
        let Self {
            mints,
            member_index,
            ..
        } = self;
        let Some(top) = mints.get_mut(&mint) else {
            return;
        };
        if top.entries.len() <= k {
            return;
        }
        let mut victims: Vec<(Pubkey, u64)> = top
            .entries
            .iter()
            .map(|(pubkey, entry)| (*pubkey, entry.amount))
            .collect();
        victims.sort_unstable_by(|a, b| (a.1, a.0).cmp(&(b.1, b.0)));
        victims.truncate(top.entries.len() - k);
        for (pubkey, amount) in victims {
            top.entries.remove(&pubkey);
            top.dropped_floor = top.dropped_floor.max(amount);
            if !is_sentinel(&mint) {
                member_index.remove(&pubkey);
            }
        }
        top.refresh_cached_min();
    }

    fn emit(&mut self, mint: Pubkey, slot: u64, outcome: &mut BlockOutcome) {
        let Some(top) = self.mints.get_mut(&mint) else {
            return;
        };
        match top.compute_top() {
            ComputedTop::Unsound => {
                if top.latch_stale() {
                    outcome.newly_stale.push(mint);
                }
            }
            ComputedTop::Sound(rows) => {
                top.stale = false;
                if rows != top.last_record {
                    top.last_record = rows.clone();
                    if rows.is_empty() {
                        outcome.cleared.push(mint);
                    } else {
                        outcome.records.push(MintRecord { mint, slot, rows });
                    }
                }
            }
        }
    }
}

pub struct SeedAccount {
    pub pubkey: Pubkey,
    pub amount: u64,
    pub slot: u64,
    pub write_version: u64,
}

/// Per-snapshot-file accumulator of candidate holders and observed closes, built
/// without locking and merged into the tracker while it is still bootstrapping.
pub struct LargestAccountsSeed {
    sol_k: Option<usize>,
    token_k: usize,
    tracked_mints: Arc<HashSet<Pubkey>>,
    mints: HashMap<Pubkey, Vec<SeedAccount>>,
    closes: Vec<(Pubkey, u64, u64)>,
}

impl LargestAccountsSeed {
    /// Reservoir size for `mint`, or `None` when its domain is not tracked.
    fn k_of(&self, mint: &Pubkey) -> Option<usize> {
        if is_sentinel(mint) {
            self.sol_k
        } else {
            Some(self.token_k)
        }
    }

    /// Fold one snapshot account into the seed: record a close for zero-lamport
    /// accounts, otherwise observe the native-SOL balance and, for tracked SPL
    /// token accounts, the token balance.
    pub fn observe_snapshot_account(
        &mut self,
        pubkey: Pubkey,
        owner: &Pubkey,
        lamports: u64,
        data: &[u8],
        slot: u64,
        write_version: u64,
    ) {
        if lamports == 0 {
            self.observe_close(pubkey, slot, write_version);
            return;
        }

        self.observe(SOL_SENTINEL_MINT, pubkey, lamports, slot, write_version);

        if (owner == &TOKEN_PROGRAM_ID || owner == &TOKEN_2022_PROGRAM_ID) && data.len() >= 72
            && let Ok(mint) = Pubkey::try_from(&data[0..32])
                && self.tracked_mints.contains(&mint) {
                    let amount = u64::from_le_bytes(data[64..72].try_into().unwrap());
                    self.observe(mint, pubkey, amount, slot, write_version);
                }
    }

    pub fn observe(&mut self, mint: Pubkey, pubkey: Pubkey, amount: u64, slot: u64, write_version: u64) {
        let Some(k) = self.k_of(&mint) else {
            return;
        };
        let entries = self.mints.entry(mint).or_default();
        entries.push(SeedAccount {
            pubkey,
            amount,
            slot,
            write_version,
        });
        if entries.len() >= k * 8 {
            Self::shrink(entries, k);
        }
    }

    pub fn observe_close(&mut self, pubkey: Pubkey, slot: u64, write_version: u64) {
        self.closes.push((pubkey, slot, write_version));
    }

    fn shrink(entries: &mut Vec<SeedAccount>, k: usize) {
        entries.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.amount));
        entries.truncate(k);
    }
}

/// The tracker's shared innards: the immutable per-domain configuration plus
/// the mutex-guarded state.
struct Shared {
    /// Reservoir size for the SOL/circulating/non-circulating sentinel tops;
    /// `None` when the `[largest-accounts]` section is absent.
    sol_k: Option<usize>,
    tracked_mints: Arc<HashSet<Pubkey>>,
    /// Reservoir size for the per-mint token tops.
    token_k: usize,
    state: Mutex<TrackerState>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, TrackerState> {
        self.state
            .lock()
            .expect("Failed to lock largest accounts state")
    }

    /// Reservoir size for `mint`'s domain (unused when the domain is absent).
    fn k_for(&self, mint: &Pubkey) -> usize {
        if is_sentinel(mint) {
            self.sol_k.unwrap_or(0)
        } else {
            self.token_k
        }
    }
}

/// Cheap-clone handle to the largest-accounts tracker shared by the ingest and
/// snapshot paths. The default (`None`) means both features are disabled and
/// every operation is a no-op.
#[derive(Clone, Default)]
pub struct LargestAccountsTracker(Option<Arc<Shared>>);

impl LargestAccountsTracker {
    /// Builds the tracker from the two independent domains: `sol_k` sizes the
    /// SOL/class sentinel tops (`[largest-accounts]`), `token` carries the
    /// tracked mints and per-mint reservoir size (`[token-largest-accounts]`).
    pub fn new(sol_k: Option<usize>, token: Option<(HashSet<Pubkey>, usize)>) -> Self {
        if sol_k.is_none() && token.is_none() {
            return Self::default();
        }
        let (tracked_mints, token_k) =
            token.unwrap_or((HashSet::new(), super::PERSISTED_TOP_N));
        let mut mints: HashMap<Pubkey, MintTop> = tracked_mints
            .iter()
            .map(|mint| (*mint, MintTop::default()))
            .collect();
        if sol_k.is_some() {
            mints.insert(SOL_SENTINEL_MINT, MintTop::default());
            mints.insert(CIRCULATING_SENTINEL_MINT, MintTop::default());
            mints.insert(NON_CIRCULATING_SENTINEL_MINT, MintTop::default());
        }
        Self(Some(Arc::new(Shared {
            sol_k,
            tracked_mints: Arc::new(tracked_mints),
            token_k,
            state: Mutex::new(TrackerState {
                mints,
                ..TrackerState::default()
            }),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// True when the SOL/class sentinel tops are maintained (`[largest-accounts]`).
    pub fn sol_tracking_enabled(&self) -> bool {
        self.0.as_deref().is_some_and(|shared| shared.sol_k.is_some())
    }

    /// True when per-mint token tops are maintained (`[token-largest-accounts]`).
    pub fn token_tracking_enabled(&self) -> bool {
        self.0
            .as_deref()
            .is_some_and(|shared| !shared.tracked_mints.is_empty())
    }

    pub fn is_tracked(&self, mint_bytes: &[u8]) -> bool {
        let Some(shared) = &self.0 else {
            return false;
        };
        Pubkey::try_from(mint_bytes)
            .is_ok_and(|mint| shared.tracked_mints.contains(&mint))
    }

    pub fn new_seed(&self) -> Option<LargestAccountsSeed> {
        let shared = self.0.as_deref()?;
        Some(LargestAccountsSeed {
            sol_k: shared.sol_k,
            token_k: shared.token_k,
            tracked_mints: shared.tracked_mints.clone(),
            mints: HashMap::new(),
            closes: Vec::new(),
        })
    }

    pub fn merge_seed(&self, seed: LargestAccountsSeed) {
        let Some(shared) = &self.0 else {
            return;
        };
        let mut state = shared.state();
        if state.status != Status::Bootstrapping {
            return;
        }
        let mut touched = HashSet::new();
        for (pubkey, slot, write_version) in seed.closes {
            state.max_applied_slot = state.max_applied_slot.max(slot);
            state.record_tombstone(pubkey, slot, write_version);
            if let Some(mint) = state.member_index.get(&pubkey).copied() {
                state.remove_member(mint, pubkey, slot, write_version, &mut touched);
            }
            for sentinel in [
                SOL_SENTINEL_MINT,
                CIRCULATING_SENTINEL_MINT,
                NON_CIRCULATING_SENTINEL_MINT,
            ] {
                state.remove_member(sentinel, pubkey, slot, write_version, &mut touched);
            }
        }
        for (mint, mut entries) in seed.mints {
            let k = shared.k_for(&mint);
            LargestAccountsSeed::shrink(&mut entries, k);
            for account in &entries {
                state.max_applied_slot = state.max_applied_slot.max(account.slot);
            }
            let reservoir = state.seed_reservoir.entry(mint).or_default();
            reservoir.extend(entries);
            LargestAccountsSeed::shrink(reservoir, k);
        }
    }

    pub fn apply_block(
        &self,
        slot: u64,
        pending: HashMap<Pubkey, PendingLargestAccount>,
    ) -> BlockOutcome {
        let Some(shared) = &self.0 else {
            return BlockOutcome::default();
        };
        let mut state = shared.state();
        let bootstrapping = state.status == Status::Bootstrapping;
        let old_block = !bootstrapping && slot <= state.max_applied_slot;
        state.max_applied_slot = state.max_applied_slot.max(slot);
        let mut touched = HashSet::new();
        for (pubkey, update) in pending {
            let update_mint = update.token.map(|(mint, _)| mint);
            if let Some(current_mint) = state.member_index.get(&pubkey).copied()
                && update_mint != Some(current_mint)
            {
                state.remove_member(current_mint, pubkey, slot, update.write_version, &mut touched);
            }
            if update.lamports == 0 {
                if bootstrapping {
                    state.record_tombstone(pubkey, slot, update.write_version);
                }
                for sentinel in [
                    SOL_SENTINEL_MINT,
                    CIRCULATING_SENTINEL_MINT,
                    NON_CIRCULATING_SENTINEL_MINT,
                ] {
                    state.remove_member(sentinel, pubkey, slot, update.write_version, &mut touched);
                }
                continue;
            }
            if let Some((mint, amount)) = update.token {
                state.apply_update(
                    mint,
                    pubkey,
                    amount,
                    slot,
                    update.write_version,
                    bootstrapping,
                    old_block,
                    shared.k_for(&mint),
                    &mut touched,
                );
            }
            state.apply_update(
                SOL_SENTINEL_MINT,
                pubkey,
                update.lamports,
                slot,
                update.write_version,
                bootstrapping,
                old_block,
                shared.k_for(&SOL_SENTINEL_MINT),
                &mut touched,
            );
            let (class_mint, other_class_mint) = if update.non_circulating {
                (NON_CIRCULATING_SENTINEL_MINT, CIRCULATING_SENTINEL_MINT)
            } else {
                (CIRCULATING_SENTINEL_MINT, NON_CIRCULATING_SENTINEL_MINT)
            };
            state.remove_member(
                other_class_mint,
                pubkey,
                slot,
                update.write_version,
                &mut touched,
            );
            state.apply_update(
                class_mint,
                pubkey,
                update.lamports,
                slot,
                update.write_version,
                bootstrapping,
                old_block,
                shared.k_for(&class_mint),
                &mut touched,
            );
        }
        let mut outcome = BlockOutcome::default();
        if bootstrapping {
            return outcome;
        }
        let effective_slot = state.max_applied_slot;
        for mint in touched {
            state.emit(mint, effective_slot, &mut outcome);
        }
        outcome
    }

    pub fn finish_bootstrap(&self) -> BlockOutcome {
        let Some(shared) = &self.0 else {
            return BlockOutcome::default();
        };
        let mut state = shared.state();
        if state.status != Status::Bootstrapping {
            return BlockOutcome::default();
        }
        let mut touched = HashSet::new();
        let reservoir = std::mem::take(&mut state.seed_reservoir);
        for (mint, accounts) in reservoir {
            let k = shared.k_for(&mint);
            for account in accounts {
                state.apply_update(
                    mint,
                    account.pubkey,
                    account.amount,
                    account.slot,
                    account.write_version,
                    true,
                    false,
                    k,
                    &mut touched,
                );
            }
        }
        state.tombstones = HashMap::new();
        state.status = Status::Live;
        let mints: Vec<Pubkey> = state.mints.keys().copied().collect();
        let effective_slot = state.max_applied_slot;
        let mut outcome = BlockOutcome::default();
        for mint in mints {
            state.trim(mint, shared.k_for(&mint));
            state.emit(mint, effective_slot, &mut outcome);
        }
        outcome
    }

    /// True once bootstrap has finished and the tracker is applying live blocks.
    pub fn is_live(&self) -> bool {
        self.0
            .as_deref()
            .is_some_and(|shared| shared.state().status == Status::Live)
    }

    /// Reseeds the circulating/non-circulating sentinels from a freshly recomputed
    /// non-circulating member set and its balances, returning the records to persist.
    pub fn seed_class_sentinels(
        &self,
        recompute_slot: u64,
        members: &HashSet<Pubkey>,
        balances: &[NonCirculatingBalance],
    ) -> Option<BlockOutcome> {
        let shared = self.0.as_deref()?;
        let sentinel_k = shared.sol_k?;
        let mut state = shared.state();
        if state.status != Status::Live {
            return None;
        }
        let mut touched = HashSet::new();
        let stale_non_circulating: Vec<Pubkey> = state
            .mints
            .get(&NON_CIRCULATING_SENTINEL_MINT)
            .map(|top| {
                top.entries
                    .keys()
                    .filter(|pubkey| !members.contains(pubkey))
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        for pubkey in stale_non_circulating {
            state.remove_member(
                NON_CIRCULATING_SENTINEL_MINT,
                pubkey,
                recompute_slot,
                0,
                &mut touched,
            );
        }
        let stale_circulating: Vec<Pubkey> = state
            .mints
            .get(&CIRCULATING_SENTINEL_MINT)
            .map(|top| {
                top.entries
                    .keys()
                    .filter(|pubkey| members.contains(pubkey))
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        for pubkey in stale_circulating {
            state.remove_member(
                CIRCULATING_SENTINEL_MINT,
                pubkey,
                recompute_slot,
                0,
                &mut touched,
            );
        }
        if let Some(top) = state.mints.get_mut(&NON_CIRCULATING_SENTINEL_MINT) {
            top.dropped_floor = 0;
        }
        for balance in balances {
            if balance.lamports == 0 || !members.contains(&balance.pubkey) {
                continue;
            }
            state.apply_update(
                NON_CIRCULATING_SENTINEL_MINT,
                balance.pubkey,
                balance.lamports,
                balance.slot,
                0,
                false,
                false,
                sentinel_k,
                &mut touched,
            );
        }
        let sol_floor = state
            .mints
            .get(&SOL_SENTINEL_MINT)
            .map(|top| top.dropped_floor)
            .unwrap_or(0);
        let candidates: Vec<(Pubkey, Entry)> = state
            .mints
            .get(&SOL_SENTINEL_MINT)
            .map(|top| {
                top.entries
                    .iter()
                    .filter(|(pubkey, _)| !members.contains(*pubkey))
                    .map(|(pubkey, entry)| (*pubkey, *entry))
                    .collect()
            })
            .unwrap_or_default();
        if let Some(top) = state.mints.get_mut(&CIRCULATING_SENTINEL_MINT) {
            top.dropped_floor = sol_floor;
        }
        for (pubkey, entry) in candidates {
            state.apply_update(
                CIRCULATING_SENTINEL_MINT,
                pubkey,
                entry.amount,
                entry.slot,
                entry.write_version,
                false,
                false,
                sentinel_k,
                &mut touched,
            );
        }
        let effective_slot = state.max_applied_slot;
        let mut outcome = BlockOutcome::default();
        state.emit(NON_CIRCULATING_SENTINEL_MINT, effective_slot, &mut outcome);
        state.emit(CIRCULATING_SENTINEL_MINT, effective_slot, &mut outcome);
        Some(outcome)
    }

    pub fn mark_mint_stale(&self, mint: &Pubkey) -> bool {
        let Some(shared) = &self.0 else {
            return false;
        };
        let mut state = shared.state();
        let Some(top) = state.mints.get_mut(mint) else {
            return false;
        };
        top.latch_stale()
    }

    pub fn stale_count(&self) -> usize {
        let Some(shared) = &self.0 else {
            return 0;
        };
        shared
            .state()
            .mints
            .values()
            .filter(|top| top.stale)
            .count()
    }

    /// Folds one block's accounts into a per-account pending map (native-SOL
    /// lamports plus tracked SPL token balances), keeping the highest write
    /// version per pubkey. Empty when the tracker is disabled.
    pub fn build_block_pending(
        &self,
        accounts: &[SubscribeUpdateAccountInfo],
        non_circulating: &NonCirculatingTracker,
    ) -> HashMap<Pubkey, PendingLargestAccount> {
        let mut pending: HashMap<Pubkey, PendingLargestAccount> = HashMap::new();
        if !self.is_enabled() {
            return pending;
        }

        for account in accounts {
            let Ok(pubkey) = Pubkey::try_from(account.pubkey.as_slice()) else {
                continue;
            };
            let is_token_owner = account.owner.as_slice() == TOKEN_PROGRAM_ID.as_ref()
                || account.owner.as_slice() == TOKEN_2022_PROGRAM_ID.as_ref();
            let token = if is_token_owner
                && account.data.len() >= 72
                && self.is_tracked(&account.data[0..32])
            {
                Some((
                    Pubkey::try_from(&account.data[0..32]).unwrap(),
                    u64::from_le_bytes(account.data[64..72].try_into().unwrap()),
                ))
            } else {
                None
            };
            let update = PendingLargestAccount {
                write_version: account.write_version,
                lamports: account.lamports,
                non_circulating: non_circulating.is_non_circulating(&pubkey),
                token,
            };
            match pending.entry(pubkey) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(update);
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    if update.write_version > slot.get().write_version {
                        slot.insert(update);
                    }
                }
            }
        }

        pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::largest_accounts::*;
    use std::collections::HashMap;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn tracker(k: usize) -> LargestAccountsTracker {
        LargestAccountsTracker::new(Some(k), Some(([pk(200)].into_iter().collect(), k)))
    }

    fn pending_sol(lamports: u64, write_version: u64) -> PendingLargestAccount {
        PendingLargestAccount {
            write_version,
            lamports,
            non_circulating: false,
            token: None,
        }
    }

    fn pending_token(amount: u64, write_version: u64) -> PendingLargestAccount {
        PendingLargestAccount {
            write_version,
            lamports: 1_000_000,
            non_circulating: false,
            token: Some((pk(200), amount)),
        }
    }

    fn go_live(tracker: &LargestAccountsTracker) {
        tracker.finish_bootstrap();
    }

    fn token_rows(outcome: &BlockOutcome) -> Option<&Vec<(Pubkey, u64)>> {
        sentinel_rows(outcome, &pk(200))
    }

    fn sentinel_rows<'a>(
        outcome: &'a BlockOutcome,
        mint: &Pubkey,
    ) -> Option<&'a Vec<(Pubkey, u64)>> {
        outcome
            .records
            .iter()
            .find(|record| record.mint == *mint)
            .map(|record| &record.rows)
    }

    #[test]
    fn out_of_order_slot_does_not_clobber_member() {
        let tracker = tracker(25);
        go_live(&tracker);
        tracker.apply_block(100, [(pk(1), pending_token(500, 5))].into());
        let outcome = tracker.apply_block(90, [(pk(1), pending_token(900, 9))].into());
        assert!(outcome.records.is_empty());
        let outcome = tracker.apply_block(101, [(pk(2), pending_token(1, 1))].into());
        let rows = token_rows(&outcome).unwrap();
        assert_eq!(rows[0], (pk(1), 500));
    }

    #[test]
    fn same_block_dedup_is_callers_job_but_write_version_guard_holds() {
        let tracker = tracker(25);
        go_live(&tracker);
        tracker.apply_block(100, [(pk(1), pending_token(500, 9))].into());
        let outcome = tracker.apply_block(100, [(pk(1), pending_token(100, 5))].into());
        assert!(token_rows(&outcome).is_none());
    }

    #[test]
    fn close_promotes_reservoir_and_reopen_reenters() {
        let tracker = tracker(25);
        go_live(&tracker);
        let pending: HashMap<_, _> = (1..=22)
            .map(|i| (pk(i), pending_token(1000 + i as u64, i as u64)))
            .collect();
        let outcome = tracker.apply_block(100, pending);
        let rows = token_rows(&outcome).unwrap();
        assert_eq!(rows.len(), PERSISTED_TOP_N);
        assert!(!rows.iter().any(|(pubkey, _)| *pubkey == pk(2)));
        let outcome = tracker.apply_block(
            101,
            [(pk(22), pending_sol(0, 23))].into(),
        );
        let rows = token_rows(&outcome).unwrap();
        assert_eq!(rows.len(), PERSISTED_TOP_N);
        assert!(rows.iter().any(|(pubkey, _)| *pubkey == pk(2)));
        assert!(!rows.iter().any(|(pubkey, _)| *pubkey == pk(22)));
        let outcome = tracker.apply_block(102, [(pk(22), pending_token(5000, 24))].into());
        let rows = token_rows(&outcome).unwrap();
        assert_eq!(rows[0], (pk(22), 5000));
    }

    #[test]
    fn decrease_drops_member_from_top() {
        let tracker = tracker(25);
        go_live(&tracker);
        let pending: HashMap<_, _> = (1..=21)
            .map(|i| (pk(i), pending_token(1000 + i as u64, i as u64)))
            .collect();
        tracker.apply_block(100, pending);
        let outcome = tracker.apply_block(101, [(pk(21), pending_token(1, 22))].into());
        let rows = token_rows(&outcome).unwrap();
        assert!(!rows.iter().any(|(pubkey, _)| *pubkey == pk(21)));
        assert!(rows.iter().any(|(pubkey, _)| *pubkey == pk(1)));
    }

    #[test]
    fn reservoir_exhaustion_marks_stale() {
        let tracker = tracker(21);
        go_live(&tracker);
        let pending: HashMap<_, _> = (1..=30)
            .map(|i| (pk(i), pending_token(1000 + i as u64, i as u64)))
            .collect();
        let outcome = tracker.apply_block(100, pending);
        assert!(outcome.newly_stale.is_empty());
        let closes: HashMap<_, _> = (10..=30)
            .map(|i| (pk(i), pending_sol(0, 100 + i as u64)))
            .collect();
        let outcome = tracker.apply_block(101, closes);
        assert!(outcome.newly_stale.contains(&pk(200)));
        let outcome = tracker.apply_block(102, [(pk(1), pending_token(9999, 200))].into());
        assert!(outcome.records.iter().all(|s| s.mint != pk(200)));
    }

    #[test]
    fn old_block_non_member_ratchets_floor_and_can_stale() {
        let tracker = tracker(25);
        go_live(&tracker);
        let pending: HashMap<_, _> = (1..=20)
            .map(|i| (pk(i), pending_token(1000 + i as u64, i as u64)))
            .collect();
        tracker.apply_block(100, pending);
        let outcome = tracker.apply_block(50, [(pk(99), pending_token(999_999, 1))].into());
        assert!(outcome.newly_stale.contains(&pk(200)));
    }

    #[test]
    fn bootstrap_live_update_wins_over_older_seed() {
        let tracker = tracker(25);
        tracker.apply_block(200, [(pk(1), pending_token(5, 7))].into());
        let mut seed = tracker.new_seed().unwrap();
        seed.observe(pk(200), pk(1), 1_000_000, 150, 3);
        seed.observe(pk(200), pk(2), 700, 150, 3);
        tracker.merge_seed(seed);
        let outcome = tracker.finish_bootstrap();
        let rows = token_rows(&outcome).unwrap();
        assert_eq!(rows[0], (pk(2), 700));
        assert!(rows.contains(&(pk(1), 5)));
    }

    #[test]
    fn bootstrap_close_tombstone_blocks_seed_resurrection() {
        let tracker = tracker(25);
        tracker.apply_block(200, [(pk(1), pending_sol(0, 7))].into());
        let mut seed = tracker.new_seed().unwrap();
        seed.observe(pk(200), pk(1), 1_000_000, 150, 3);
        tracker.merge_seed(seed);
        let outcome = tracker.finish_bootstrap();
        assert!(token_rows(&outcome).is_none());
    }

    #[test]
    fn seed_tombstone_from_snapshot_blocks_older_seed_file() {
        let tracker = tracker(25);
        let mut incremental = tracker.new_seed().unwrap();
        incremental.observe_close(pk(1), 160, 2);
        tracker.merge_seed(incremental);
        let mut full = tracker.new_seed().unwrap();
        full.observe(pk(200), pk(1), 1_000_000, 150, 1);
        full.observe(pk(200), pk(2), 10, 150, 1);
        tracker.merge_seed(full);
        let outcome = tracker.finish_bootstrap();
        let rows = token_rows(&outcome).unwrap();
        assert_eq!(rows, &vec![(pk(2), 10)]);
    }

    #[test]
    fn all_holders_closed_clears_mint() {
        let tracker = tracker(25);
        go_live(&tracker);
        tracker.apply_block(100, [(pk(1), pending_token(10, 1))].into());
        let outcome = tracker.apply_block(101, [(pk(1), pending_sol(0, 2))].into());
        assert!(outcome.cleared.contains(&pk(200)));
        assert!(token_rows(&outcome).is_none());
    }

    #[test]
    fn record_only_emitted_on_change() {
        let tracker = tracker(25);
        go_live(&tracker);
        tracker.apply_block(100, [(pk(1), pending_token(10, 1))].into());
        let outcome = tracker.apply_block(101, [(pk(1), pending_token(10, 2))].into());
        assert!(token_rows(&outcome).is_none());
    }

    #[test]
    fn sol_sentinel_tracks_lamports() {
        let tracker = tracker(25);
        go_live(&tracker);
        let outcome = tracker.apply_block(
            100,
            [
                (pk(1), pending_sol(500, 1)),
                (pk(2), pending_sol(900, 1)),
            ]
            .into(),
        );
        let sol = outcome
            .records
            .iter()
            .find(|record| record.mint == SOL_SENTINEL_MINT)
            .unwrap();
        assert_eq!(sol.rows[0], (pk(2), 900));
        assert_eq!(sol.slot, 100);
    }

    #[test]
    fn record_encoding_roundtrips() {
        let rows = vec![(pk(3), u64::MAX), (pk(2), 700), (pk(1), 0)];
        let bytes = encode_record(&rows);
        assert_eq!(bytes.len(), rows.len() * RECORD_ENTRY_SIZE);
        assert_eq!(decode_record(&bytes), Some(rows));
        assert_eq!(decode_record(&[]), Some(Vec::new()));
        assert_eq!(decode_record(&bytes[..RECORD_ENTRY_SIZE - 1]), None);
    }

    #[test]
    fn untracked_mint_is_ignored() {
        let tracker = tracker(25);
        go_live(&tracker);
        let outcome = tracker.apply_block(
            100,
            [(
                pk(1),
                PendingLargestAccount {
                    write_version: 1,
                    lamports: 5,
                    non_circulating: false,
                    token: Some((pk(201), 777)),
                },
            )]
            .into(),
        );
        assert!(token_rows(&outcome).is_none());
        assert!(!tracker.is_tracked(pk(201).as_ref()));
        assert!(tracker.is_tracked(pk(200).as_ref()));
    }

    #[test]
    fn token_only_tracker_skips_sentinels() {
        let tracker =
            LargestAccountsTracker::new(None, Some(([pk(200)].into_iter().collect(), 25)));
        assert!(!tracker.sol_tracking_enabled());
        assert!(tracker.token_tracking_enabled());
        go_live(&tracker);
        let outcome = tracker.apply_block(100, [(pk(1), pending_token(500, 1))].into());
        assert!(token_rows(&outcome).is_some());
        assert!(sentinel_rows(&outcome, &SOL_SENTINEL_MINT).is_none());
        assert!(sentinel_rows(&outcome, &CIRCULATING_SENTINEL_MINT).is_none());
    }

    #[test]
    fn sol_only_tracker_skips_token_updates() {
        let tracker = LargestAccountsTracker::new(Some(25), None);
        assert!(tracker.sol_tracking_enabled());
        assert!(!tracker.token_tracking_enabled());
        go_live(&tracker);
        let outcome = tracker.apply_block(100, [(pk(1), pending_token(500, 1))].into());
        assert!(token_rows(&outcome).is_none());
        assert!(sentinel_rows(&outcome, &SOL_SENTINEL_MINT).is_some());
        assert!(!tracker.is_tracked(pk(200).as_ref()));
    }
}
