use crate::modules::supply_tracker::NonCirculatingBalance;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement, TransactionTrait};
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

pub const PERSISTED_TOP_N: usize = 20;

pub const SOL_SENTINEL_MINT: Pubkey = Pubkey::new_from_array([0u8; 32]);
pub const CIRCULATING_SENTINEL_MINT: Pubkey = Pubkey::new_from_array([1u8; 32]);
pub const NON_CIRCULATING_SENTINEL_MINT: Pubkey = Pubkey::new_from_array([2u8; 32]);

fn is_sentinel(mint: &Pubkey) -> bool {
    *mint == SOL_SENTINEL_MINT
        || *mint == CIRCULATING_SENTINEL_MINT
        || *mint == NON_CIRCULATING_SENTINEL_MINT
}

pub const TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const TOKEN_2022_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

pub struct PendingLargestAccount {
    pub write_version: u64,
    pub lamports: u64,
    pub non_circulating: bool,
    pub token: Option<(Pubkey, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintRecord {
    pub mint: Pubkey,
    pub slot: u64,
    pub rows: Vec<(Pubkey, u64)>,
}

#[derive(Debug, Default)]
pub struct BlockOutcome {
    pub records: Vec<MintRecord>,
    pub newly_stale: Vec<Pubkey>,
    pub cleared: Vec<Pubkey>,
}

pub const RECORD_ENTRY_SIZE: usize = 40;

pub fn encode_record(rows: &[(Pubkey, u64)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(rows.len() * RECORD_ENTRY_SIZE);
    for (pubkey, amount) in rows {
        bytes.extend_from_slice(pubkey.as_ref());
        bytes.extend_from_slice(&amount.to_le_bytes());
    }
    bytes
}

pub fn decode_record(bytes: &[u8]) -> Option<Vec<(Pubkey, u64)>> {
    if !bytes.len().is_multiple_of(RECORD_ENTRY_SIZE) {
        return None;
    }
    let mut rows = Vec::with_capacity(bytes.len() / RECORD_ENTRY_SIZE);
    for chunk in bytes.chunks_exact(RECORD_ENTRY_SIZE) {
        let pubkey = Pubkey::try_from(&chunk[..32]).ok()?;
        let amount = u64::from_le_bytes(chunk[32..].try_into().ok()?);
        rows.push((pubkey, amount));
    }
    Some(rows)
}

#[derive(Clone, Copy)]
struct Entry {
    amount: u64,
    slot: u64,
    write_version: u64,
}

#[derive(Default)]
struct MintTop {
    entries: HashMap<Pubkey, Entry>,
    dropped_floor: u64,
    stale: bool,
    last_record: Vec<(Pubkey, u64)>,
}

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

    fn compute_top(&self) -> ComputedTop {
        let mut rows: Vec<(Pubkey, u64)> = self
            .entries
            .iter()
            .map(|(pubkey, entry)| (*pubkey, entry.amount))
            .collect();
        rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        rows.truncate(PERSISTED_TOP_N);
        if rows.len() < PERSISTED_TOP_N && self.dropped_floor > 0 {
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

#[derive(Clone, Copy, Default, PartialEq)]
enum Status {
    #[default]
    Bootstrapping,
    Live,
}

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
        if !bootstrapping && top.entries.len() >= k {
            let (min_pubkey, min_amount) = top.min_entry().expect("non-empty full map");
            if amount <= min_amount {
                if amount > top.dropped_floor {
                    top.dropped_floor = amount;
                    touched.insert(mint);
                }
                return;
            }
            top.entries.remove(&min_pubkey);
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

pub struct LargestAccountsSeed {
    k: usize,
    mints: HashMap<Pubkey, Vec<SeedAccount>>,
    closes: Vec<(Pubkey, u64, u64)>,
}

impl LargestAccountsSeed {
    pub fn observe(&mut self, mint: Pubkey, pubkey: Pubkey, amount: u64, slot: u64, write_version: u64) {
        let entries = self.mints.entry(mint).or_default();
        entries.push(SeedAccount {
            pubkey,
            amount,
            slot,
            write_version,
        });
        if entries.len() >= self.k * 8 {
            Self::shrink(entries, self.k);
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

struct Shared {
    tracked_mints: HashSet<Pubkey>,
    k_per_mint: usize,
    state: Mutex<TrackerState>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, TrackerState> {
        self.state
            .lock()
            .expect("Failed to lock largest accounts state")
    }
}

#[derive(Clone, Default)]
pub struct LargestAccountsTracker(Option<Arc<Shared>>);

impl LargestAccountsTracker {
    pub fn new(tracked_mints: HashSet<Pubkey>, k_per_mint: usize) -> Self {
        let mut mints: HashMap<Pubkey, MintTop> = tracked_mints
            .iter()
            .map(|mint| (*mint, MintTop::default()))
            .collect();
        mints.insert(SOL_SENTINEL_MINT, MintTop::default());
        mints.insert(CIRCULATING_SENTINEL_MINT, MintTop::default());
        mints.insert(NON_CIRCULATING_SENTINEL_MINT, MintTop::default());
        Self(Some(Arc::new(Shared {
            tracked_mints,
            k_per_mint,
            state: Mutex::new(TrackerState {
                mints,
                ..TrackerState::default()
            }),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
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
            k: shared.k_per_mint,
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
            LargestAccountsSeed::shrink(&mut entries, shared.k_per_mint);
            for account in &entries {
                state.max_applied_slot = state.max_applied_slot.max(account.slot);
            }
            let reservoir = state.seed_reservoir.entry(mint).or_default();
            reservoir.extend(entries);
            LargestAccountsSeed::shrink(reservoir, shared.k_per_mint);
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
                    shared.k_per_mint,
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
                shared.k_per_mint,
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
                shared.k_per_mint,
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
            for account in accounts {
                state.apply_update(
                    mint,
                    account.pubkey,
                    account.amount,
                    account.slot,
                    account.write_version,
                    true,
                    false,
                    shared.k_per_mint,
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
            state.trim(mint, shared.k_per_mint);
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
                shared.k_per_mint,
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
                shared.k_per_mint,
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
}

pub async fn persist_records(
    db: &DatabaseConnection,
    records: &[MintRecord],
) -> Result<(), DbErr> {
    if records.is_empty() {
        return Ok(());
    }
    let txn = db.begin().await?;
    for record in records {
        txn.execute(Statement::from_string(
            DatabaseBackend::Postgres,
            format!(
                "INSERT INTO largest_accounts (mint, slot, record) \
                 VALUES ('\\x{}'::bytea, {}, '\\x{}'::bytea) \
                 ON CONFLICT (mint, slot) DO UPDATE \
                 SET record = EXCLUDED.record, updated_on = now()",
                hex::encode(record.mint.as_ref()),
                record.slot,
                hex::encode(encode_record(&record.rows)),
            ),
        ))
        .await?;
    }
    txn.commit().await
}

pub async fn delete_mint_rows(db: &DatabaseConnection, mint: &Pubkey) -> Result<(), DbErr> {
    db.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        format!(
            "DELETE FROM largest_accounts WHERE mint = '\\x{}'::bytea",
            hex::encode(mint.as_ref())
        ),
    ))
    .await?;
    Ok(())
}

pub async fn clear_largest_accounts(db: &DatabaseConnection) -> Result<(), DbErr> {
    db.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        "DELETE FROM largest_accounts".to_string(),
    ))
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn tracker(k: usize) -> LargestAccountsTracker {
        LargestAccountsTracker::new([pk(200)].into_iter().collect(), k)
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
}
