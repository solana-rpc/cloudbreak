// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The indexer side: keeps every mint in memory, from the snapshot and the confirmed blocks, and
//! writes the changed mints to the `mints` table every `db-write-interval-slots` slots.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sea_orm::DatabaseConnection;
use solana_pubkey::Pubkey;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

use super::MintChange;
use super::db::{MintRow, UPSERT_CHUNK, upsert_rows, write_last_db_slot};
use super::operational_endpoints::LastBlock;
use super::store::{ChangeKind, MintStore, StoredMint};
use crate::config::MintCacheConfig;
use crate::metrics::{
    MINT_CACHE_DB_WRITE_ERRORS_TOTAL, MINT_CACHE_UNCHANGED_WRITES_TOTAL, MintCacheSide,
    observe_mint_cache_stage, observe_mint_changes, set_mint_cache_startup_seconds,
    set_mint_entries,
};

pub(super) struct Inner {
    /// Keeps all mints included immutable ones: without them, every mint or burn of one would look like a
    /// new mint and rewrite its row, and the API would reload it.
    pub(super) store: MintStore,
    /// While the bootstrap runs: closes of keys the store does not hold yet, with their slot.
    /// Snapshot files and blocks arrive in any order, so a close can come before its mint: mint
    /// M is live at slot 100 in one snapshot file and closed at slot 200 in a file read before
    /// it. The store ignores a close of a key it does not hold (most zero-lamport writes are not
    /// mints), so the close waits here and is applied when the bootstrap ends. `None` after it.
    pub(super) closed_pending: Option<HashMap<Pubkey, u64>>,
    /// Keys changed since the last write to `mints`, with the slot of their last change.
    pub(super) pending_to_db: HashMap<Pubkey, u64>,
    /// The highest slot applied.
    pub(super) last_slot: u64,
    /// The lowest slot self-healing still has to repair, as of the last block. `last_db_slot`
    /// stays below it: the API never reloads rows at or below `last_db_slot`.
    pub(super) lowest_gap: Option<u64>,
    /// The slot `mints` is complete through. `None` until the bootstrap wrote the table.
    pub(super) last_db_slot: Option<u64>,
    /// True while a write to `mints` runs in the background. A new write waits until it ends:
    /// two writes at once could finish in the wrong order and move `last_db_slot` back.
    pub(super) db_write_running: bool,
    db_write_interval_slots: u64,
    /// The last block's mint changes, shown by the debug endpoint only.
    pub(super) last_block: Option<LastBlock>,
}

impl Inner {
    /// Applies one change and marks a changed key for the next write to `mints`. While
    /// bootstrapping, a close of a key the store does not hold waits in `closed_pending`.
    fn apply(&mut self, pubkey: Pubkey, slot: u64, change: MintChange) -> Option<ChangeKind> {
        if let (MintChange::Closed, Some(closed_pending)) = (&change, &mut self.closed_pending)
            && self.store.get(&pubkey) == StoredMint::Missing
        {
            let pending = closed_pending.entry(pubkey).or_insert(slot);
            *pending = (*pending).max(slot);
            return None;
        }
        let kind = self.store.apply(pubkey, slot, change)?;
        self.mark_pending(pubkey, slot);
        Some(kind)
    }

    /// Marks `pubkey` for the next write to `mints`.
    fn mark_pending(&mut self, pubkey: Pubkey, slot: u64) {
        let pending = self.pending_to_db.entry(pubkey).or_insert(slot);
        *pending = (*pending).max(slot);
    }

    /// The rows of every key changed since the last write. Empties `pending_to_db`.
    fn take_pending_rows(&mut self) -> Vec<MintRow> {
        std::mem::take(&mut self.pending_to_db)
            .into_iter()
            .filter_map(|(pubkey, slot)| self.store.row(pubkey, slot))
            .collect()
    }

    /// The slot `mints` is complete through once the pending rows are written: the highest
    /// slot applied, but never at or past an open gap.
    fn complete_through(&self) -> u64 {
        match self.lowest_gap {
            Some(gap) => self.last_slot.min(gap.saturating_sub(1)),
            None => self.last_slot,
        }
    }

    /// Takes the rows for a background write, when one is due: while bootstrapping once
    /// `pending_to_db` fills a chunk (no `last_db_slot` yet), after it once
    /// `db-write-interval-slots` passed since `last_db_slot`. Returns the rows and the
    /// `last_db_slot` to set when they are written.
    ///
    /// Returns `None` if no write is needed.
    ///
    /// Note: Actual DB write happens inside [`spawn_write`], so the [`Inner`] lock can be released before DB operation
    fn start_write_if_needed(&mut self) -> Option<(Vec<MintRow>, Option<u64>)> {
        if self.db_write_running {
            return None;
        }
        let new_last_db_slot = match self.last_db_slot {
            None if self.pending_to_db.len() >= UPSERT_CHUNK => None,
            None => return None,
            Some(last_db_slot)
                if self.complete_through() >= last_db_slot + self.db_write_interval_slots =>
            {
                Some(self.complete_through())
            }
            Some(_) => return None,
        };
        self.db_write_running = true;
        Some((self.take_pending_rows(), new_last_db_slot))
    }
}

fn lock(inner: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    inner.lock().expect("mint cache indexer mutex poisoned")
}

/// The indexer half of [`super::MintCache`]. Cheap-clone handle; the default is disabled and
/// every call is a no-op.
#[derive(Clone, Default)]
pub struct MintCacheIndexer(pub(super) Option<Arc<Mutex<Inner>>>);

impl MintCacheIndexer {
    /// Returns the disabled half when the section is absent or disabled. The caller checks
    /// the section's required modules first.
    pub(super) fn from_config(config: Option<&MintCacheConfig>) -> Self {
        let Some(config) = config.filter(|c| c.enabled) else {
            return Self::default();
        };
        Self(Some(Arc::new(Mutex::new(Inner {
            store: MintStore::presized(),
            closed_pending: Some(HashMap::new()),
            pending_to_db: HashMap::new(),
            last_slot: 0,
            lowest_gap: None,
            last_db_slot: None,
            db_write_running: false,
            db_write_interval_slots: config.db_write_interval_slots,
            last_block: None,
        }))))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Applies one snapshot account at `slot` when it is a mint or a close. Its row is written
    /// with the next background write, once `pending_to_db` fills a chunk.
    pub fn on_snapshot_account(
        &self,
        db: &DatabaseConnection,
        slot: u64,
        pubkey: &Pubkey,
        owner: &Pubkey,
        lamports: u64,
        data: &[u8],
    ) {
        let Some(inner) = &self.0 else {
            return;
        };
        let Some(change) = MintChange::parse(pubkey, owner, lamports, data) else {
            return;
        };
        let due_write = {
            let mut state = lock(inner);
            state.last_slot = state.last_slot.max(slot);
            state.apply(*pubkey, slot, change);
            state.start_write_if_needed()
        };
        if let Some((rows, new_last_db_slot)) = due_write {
            spawn_write(inner.clone(), db.clone(), rows, new_last_db_slot, None);
        }
    }

    /// Ends the bootstrap once the snapshot is loaded: applies the closes that waited for their
    /// mint and writes every change since the snapshot files, then `last_db_slot`. The API
    /// loads `mints` only after.
    pub async fn finish_bootstrap(&self, db: &DatabaseConnection) -> anyhow::Result<()> {
        let Some(inner) = &self.0 else {
            return Ok(());
        };
        let started = Instant::now();
        // A running write must end first: if it failed after `last_db_slot` is set, its rows
        // would be missing from a table the API trusts.
        while lock(inner).db_write_running {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (closed_pending, new_last_db_slot) = {
            let mut state = lock(inner);
            for (pubkey, slot) in state.closed_pending.take().unwrap_or_default() {
                // Apply pending closed accounts
                state.apply(pubkey, slot, MintChange::Closed);
            }
            state.db_write_running = true;
            (state.take_pending_rows(), state.complete_through())
        };
        write(db, &closed_pending, Some(new_last_db_slot), None).await?;

        let seconds = started.elapsed().as_secs_f64();
        set_mint_cache_startup_seconds(MintCacheSide::Indexer, seconds);
        let mut state = lock(inner);
        state.db_write_running = false;
        state.last_db_slot = Some(new_last_db_slot);
        set_mint_entries(MintCacheSide::Indexer, &state.store);
        tracing::info!(
            closed_pending = closed_pending.len(),
            new_last_db_slot,
            seconds,
            "mint cache: bootstrap written"
        );
        Ok(())
    }

    /// Applies a confirmed block's mint writes. Every `db-write-interval-slots` slots it starts
    /// a background write of the changed mints to the `mints` table; a failed write is retried with the next block.
    /// `lowest_gap` is the lowest slot self-healing still repairs; `last_db_slot` stays below it.
    pub fn on_block(
        &self,
        db: &DatabaseConnection,
        slot: u64,
        accounts: &[SubscribeUpdateAccountInfo],
        lowest_gap: Option<u64>,
        query_timeout_secs: u64,
    ) {
        let Some(inner) = &self.0 else {
            return;
        };
        let started = Instant::now();
        let changes: Vec<(Pubkey, MintChange)> = accounts
            .iter()
            .filter_map(|account| {
                let pubkey = Pubkey::try_from(account.pubkey.as_slice()).ok()?;
                let owner = Pubkey::try_from(account.owner.as_slice()).ok()?;
                let change = MintChange::parse(&pubkey, &owner, account.lamports, &account.data)?;
                Some((pubkey, change))
            })
            .collect();
        observe_mint_cache_stage(MintCacheSide::Indexer, "classify", started);

        let started = Instant::now();
        let due_write = {
            let mut state = lock(inner);
            state.lowest_gap = lowest_gap;
            state.last_slot = state.last_slot.max(slot);

            // The changes counts from this block, by kind (`create`, `update`, `close`, `reinit`).
            let mut written: HashMap<&'static str, usize> =
                ChangeKind::ALL.iter().map(|k| (k.label(), 0)).collect();
            let mut skipped = 0;

            for (pubkey, change) in changes {
                let live = matches!(change, MintChange::Live(_));
                match state.apply(pubkey, slot, change) {
                    Some(kind) => *written.entry(kind.label()).or_default() += 1,
                    None if live => skipped += 1,
                    None => {}
                }
            }
            for (kind, count) in &written {
                observe_mint_changes(MintCacheSide::Indexer, kind, *count);
            }
            MINT_CACHE_UNCHANGED_WRITES_TOTAL.inc_by(skipped as u64);
            set_mint_entries(MintCacheSide::Indexer, &state.store);
            state.last_block = Some(LastBlock {
                slot,
                written,
                skipped,
            });
            state.start_write_if_needed()
        };
        observe_mint_cache_stage(MintCacheSide::Indexer, "apply", started);
        if let Some((rows, new_last_db_slot)) = due_write {
            let timeout = Duration::from_secs(query_timeout_secs);
            spawn_write(
                inner.clone(),
                db.clone(),
                rows,
                new_last_db_slot,
                Some(timeout),
            );
        }
    }
}

/// Writes `rows` in the background, then `last_db_slot = new_last_db_slot` when set. On
/// failure the keys go back to `pending_to_db` and the next trigger retries them.
fn spawn_write(
    inner: Arc<Mutex<Inner>>,
    db: DatabaseConnection,
    rows: Vec<MintRow>,
    new_last_db_slot: Option<u64>,
    timeout: Option<Duration>,
) {
    tokio::spawn(async move {
        let started = Instant::now();
        let result = write(&db, &rows, new_last_db_slot, timeout).await;
        observe_mint_cache_stage(MintCacheSide::Indexer, "write", started);
        let mut state = lock(&inner);
        state.db_write_running = false;
        match result {
            Ok(()) => {
                if new_last_db_slot.is_some() {
                    state.last_db_slot = new_last_db_slot;
                }
            }
            Err(e) => {
                tracing::error!(
                    ?new_last_db_slot,
                    rows = rows.len(),
                    "mints write failed, retrying with the next trigger: {e:#}"
                );
                MINT_CACHE_DB_WRITE_ERRORS_TOTAL.inc();
                for row in rows {
                    state.mark_pending(row.pubkey, row.slot);
                }
            }
        }
    });
}

/// Writes `rows`, then `last_db_slot = new_last_db_slot` when set, which tells the API the
/// table is complete through that slot.
async fn write(
    db: &DatabaseConnection,
    rows: &[MintRow],
    new_last_db_slot: Option<u64>,
    timeout: Option<Duration>,
) -> anyhow::Result<()> {
    let pool = db.get_postgres_connection_pool();
    upsert_rows(pool, rows, timeout).await?;
    if let Some(new_last_db_slot) = new_last_db_slot {
        write_last_db_slot(pool, new_last_db_slot).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexConfig;
    use crate::modules::mint_cache::tests::{legacy_mint, token_2022_mint};
    use crate::utils::token::{LEGACY_TOKEN_PROGRAM_ID, MintState, TOKEN_2022_PROGRAM_ID};
    use spl_token_2022_interface::extension::ExtensionType;

    fn index_config() -> IndexConfig {
        toml::from_str(&format!(
            "{}\n[mint-cache]\nenabled = true\ndb-write-interval-slots = 10\n",
            include_str!("../../../../../example.cloudbreak.index.toml")
        ))
        .unwrap()
    }

    fn inner() -> Arc<Mutex<Inner>> {
        let mut config = index_config();
        config.programs = crate::config::AccountSelectorConfig::default();
        MintCacheIndexer::from_config(config.mint_cache.as_ref())
            .0
            .unwrap()
    }

    fn live(decimals: u8) -> MintChange {
        let state = MintState::from_mint(&Pubkey::new_unique(), &legacy_mint(decimals)).unwrap();
        MintChange::Live(state.classify(&LEGACY_TOKEN_PROGRAM_ID))
    }

    /// A Token-2022 mint with a close authority: mutable, so it can close.
    fn closable() -> MintChange {
        let data = token_2022_mint(&[ExtensionType::MintCloseAuthority], 0.0);
        let state = MintState::from_mint(&Pubkey::new_unique(), &data).unwrap();
        MintChange::Live(state.classify(&TOKEN_2022_PROGRAM_ID))
    }

    #[test]
    fn requires_the_snapshot_and_the_token_programs() {
        // The example filters [programs] to Stake.
        let mut config = index_config();
        let mints = config.mint_cache.clone().unwrap();
        let err = mints
            .check_required_indexer_modules(&config)
            .unwrap_err()
            .to_string();
        assert!(err.contains("both token programs"), "{err}");
        config.programs = crate::config::AccountSelectorConfig::default();
        mints.check_required_indexer_modules(&config).unwrap();
        config.snapshot = None;
        let err = mints
            .check_required_indexer_modules(&config)
            .unwrap_err()
            .to_string();
        assert!(err.contains("[snapshot]"), "{err}");
    }

    #[test]
    fn disabled_tracker_is_a_noop() {
        let tracker = MintCacheIndexer::default();
        assert!(tracker.summary().is_none());
    }

    #[test]
    fn a_close_that_arrives_before_its_mint_wins_at_the_end_of_the_bootstrap() {
        let inner = inner();
        let mut state = lock(&inner);
        let mint = Pubkey::new_unique();
        // The close at 200 comes first; the store does not hold the mint yet.
        assert_eq!(state.apply(mint, 200, MintChange::Closed), None);
        assert_eq!(state.closed_pending.as_ref().unwrap()[&mint], 200);
        // The older live version at 100 arrives later and is stored.
        state.apply(mint, 100, closable());
        // What `finish_bootstrap` does with the waiting closes.
        for (pubkey, slot) in state.closed_pending.take().unwrap() {
            state.apply(pubkey, slot, MintChange::Closed);
        }
        let rows = state.take_pending_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].slot, rows[0].state.is_none()), (200, true));
    }

    #[test]
    fn a_full_chunk_starts_a_write_while_bootstrapping_without_last_db_slot() {
        let inner = inner();
        let mut state = lock(&inner);
        for slot in 0..UPSERT_CHUNK as u64 {
            let mint = Pubkey::new_unique();
            state.apply(mint, slot, live(6));
        }
        let (rows, new_last_db_slot) = state.start_write_if_needed().unwrap();
        assert_eq!((rows.len(), new_last_db_slot), (UPSERT_CHUNK, None));
        assert!(state.db_write_running);
    }

    #[test]
    fn last_db_slot_never_passes_an_open_gap() {
        let inner = inner();
        let mut state = lock(&inner);
        state.last_db_slot = Some(900);
        state.last_slot = 1_500;
        // Slots from 1000 still wait for repair: the table is complete through 999 only.
        state.lowest_gap = Some(1_000);
        let (_, new_last_db_slot) = state.start_write_if_needed().unwrap();
        assert_eq!(new_last_db_slot, Some(999));
        state.db_write_running = false;
        state.last_db_slot = Some(999);
        // While the gap stays open, nothing moves `last_db_slot`.
        state.last_slot = 2_000;
        assert!(state.start_write_if_needed().is_none());
        // Repaired: the next write covers everything applied.
        state.lowest_gap = None;
        let (_, new_last_db_slot) = state.start_write_if_needed().unwrap();
        assert_eq!(new_last_db_slot, Some(2_000));
    }

    #[test]
    fn writes_wait_for_a_chunk_the_interval_and_the_running_write() {
        let inner = inner();
        let mut state = lock(&inner);
        let mint = Pubkey::new_unique();
        state.apply(mint, 100, live(6));
        state.last_slot = 100;
        // While bootstrapping, a write waits for a full chunk.
        assert!(state.start_write_if_needed().is_none());
        state.last_db_slot = Some(95);
        // The interval (10) has not passed.
        assert!(state.start_write_if_needed().is_none());
        state.last_slot = 105;
        let (rows, new_last_db_slot) = state.start_write_if_needed().unwrap();
        assert_eq!((rows.len(), new_last_db_slot), (1, Some(105)));
        assert!(state.pending_to_db.is_empty());
        // One write at a time.
        state.last_slot = 200;
        assert!(state.start_write_if_needed().is_none());
    }
}
