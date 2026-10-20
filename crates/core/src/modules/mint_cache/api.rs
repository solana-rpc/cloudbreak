// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The API side: loads `mints` once the indexer wrote it (`last_db_slot` is set), buffers mint
//! changes from the gRPC feed, and applies them when the slot syncronizer reports their block
//! confirmed. After a gap, a background task reloads from `mints` once it covers the gap.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use sea_orm::DatabaseConnection;
use serde::Serialize;
use solana_pubkey::Pubkey;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use super::MintChange;
use super::db::{MintRow, read_last_db_slot};
use super::store::{ChangeKind, MintStore, MutableMint, StoredMint};
use crate::config::MintCacheConfig;
use crate::metrics::{
    MINT_CACHE_BUFFER_BLOCKS, MINT_CACHE_RELOADS_TOTAL, MintCacheSide, count_mint_lookup,
    observe_mint_cache_stage, observe_mint_changes, set_mint_cache_api_gauges,
    set_mint_cache_startup_seconds,
};
use crate::utils::token::MintState;

/// Blocks kept in the buffer: more than a full table load plus one `db-write-interval-slots`.
const MAX_BUFFERED_BLOCKS: usize = 20_000;
/// How often the background task reads `last_db_slot` again, and retries a failed load.
const RETRY: Duration = Duration::from_secs(10);

/// The answer for one mint at a request slot.
#[derive(Debug, Clone, PartialEq)]
enum MintLookup {
    Found(MintState),
    /// Closed at or before the request slot: the mint does not exist there.
    Closed,
    /// Read the mint from Postgres at the request slot.
    Postgres(PostgresReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresReason {
    /// The table is not loaded yet.
    Loading,
    /// A mutable mint changed after the request picked its slot.
    NewerThanRequest,
    /// The cache has not applied the request slot (a failed reload).
    Behind,
    /// Not in the cache while live: a bug or an indexer gap.
    Missing,
}

impl PostgresReason {
    fn label(self) -> &'static str {
        match self {
            Self::Loading => "loading",
            Self::NewerThanRequest => "newer_than_request",
            Self::Behind => "behind",
            Self::Missing => "missing",
        }
    }
}

/// A reload the background task runs once `last_db_slot >= target`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(super) struct Reload {
    /// Reloads the rows with `slot > after`.
    after: u64,
    target: u64,
    reason: ReloadReason,
}

/// Why `on_confirmed` asked for a reload. A request merged into a pending one keeps its reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ReloadReason {
    /// A block is missing from the buffer: the gRPC feed lagged or had a gap.
    BlockMissing,
    /// The indexer is healthy again after a gap repair.
    HealthRestored,
}

impl ReloadReason {
    fn label(self) -> &'static str {
        match self {
            Self::BlockMissing => "block_missing",
            Self::HealthRestored => "health_restored",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Status {
    Loading,
    Live,
}

/// The confirmed block the cache is current through.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct Applied {
    slot: u64,
    blockhash: String,
}

pub(super) struct CacheState {
    pub(super) status: Status,
    pub(super) store: MintStore,
    pub(super) applied: Applied,
}

pub(super) struct BufferedBlock {
    blockhash: String,
    parent_slot: u64,
    parent_blockhash: String,
    changes: Vec<(Pubkey, MintChange)>,
}

#[derive(Default)]
pub(super) struct Reloads {
    /// `applied` when a confirmed notification first reported the indexer unhealthy. A return to
    /// healthy asks for a reload of the changes since then.
    pub(super) unhealthy_since: Option<u64>,
    /// The reload `on_confirmed` asked for, after a missing block or an indexer gap. The
    /// background task runs it once `last_db_slot` reaches its target, then clears it.
    pub(super) pending: Option<Reload>,
}

pub(super) struct Inner {
    /// The mint maps, the confirmed slot they are current through (`applied`) and the status.
    /// Requests read it; confirmed slots, the startup load and the reloads write it.
    state: RwLock<CacheState>,
    /// The mint changes of each processed block from the gRPC feed, by slot, one entry per
    /// blockhash. Applied when the slot is confirmed; dropped once at or below `applied`.
    buffer: Mutex<BTreeMap<u64, Vec<BufferedBlock>>>,
    /// What `on_confirmed` and the background task share to decide and run reloads.
    pub(super) reloads: Mutex<Reloads>,
}

/// The API half of [`super::MintCache`]. Cheap-clone handle; the default is disabled.
#[derive(Clone, Default)]
pub struct MintCacheApi(pub(super) Option<Arc<Inner>>);

impl MintCacheApi {
    /// Returns the disabled half when the section is absent or disabled.
    pub(super) fn from_config(config: Option<&MintCacheConfig>) -> Self {
        if !config.is_some_and(|c| c.enabled) {
            return Self::default();
        }
        Self(Some(Arc::new(Inner {
            state: RwLock::new(CacheState {
                status: Status::Loading,
                store: MintStore::default(),
                applied: Applied {
                    slot: 0,
                    blockhash: String::new(),
                },
            }),
            buffer: Mutex::new(BTreeMap::new()),
            reloads: Mutex::new(Reloads::default()),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Feed thread: keeps the block's mint changes until its slot is confirmed.
    pub fn on_block(&self, block: &SubscribeUpdateBlock) {
        let Some(inner) = &self.0 else {
            return;
        };
        let started = Instant::now();
        let buffered = BufferedBlock {
            blockhash: block.blockhash.clone(),
            parent_slot: block.parent_slot,
            parent_blockhash: block.parent_blockhash.clone(),
            changes: block
                .accounts
                .iter()
                .filter_map(|account| {
                    let pubkey = Pubkey::try_from(account.pubkey.as_slice()).ok()?;
                    let owner = Pubkey::try_from(account.owner.as_slice()).ok()?;
                    let change =
                        MintChange::parse(&pubkey, &owner, account.lamports, &account.data)?;
                    Some((pubkey, change))
                })
                .collect(),
        };
        let mut buffer = inner.buffer();
        let slot_blocks = buffer.entry(block.slot).or_default();
        slot_blocks.retain(|b| b.blockhash != buffered.blockhash);
        slot_blocks.push(buffered);
        while buffer.len() > MAX_BUFFERED_BLOCKS {
            buffer.pop_first();
        }
        MINT_CACHE_BUFFER_BLOCKS.set(buffer.len() as i64);
        drop(buffer);
        observe_mint_cache_stage(MintCacheSide::Api, "ingest", started);
    }

    /// Slot syncronizer: a confirmed slot advanced. Applies its buffered mint changes before the
    /// slot is published. When a block is missing, or the indexer is healthy again after a gap,
    /// it asks the background task for a reload and returns; requests past the applied slot then
    /// read mutable mints from Postgres until the reload ran.
    pub fn on_confirmed(&self, slot: u64, blockhash: Option<&str>, healthy: bool) {
        let Some(inner) = &self.0 else {
            return;
        };
        let started = Instant::now();
        let confirmed = Applied {
            slot,
            blockhash: blockhash.unwrap_or_default().to_string(),
        };
        let (status, applied) = {
            let state = inner.read();
            (state.status, state.applied.clone())
        };
        if status == Status::Loading {
            return;
        }
        let (health_restored_from, reload_pending) = {
            let mut reloads = inner.reloads();
            let health_restored_from = match (healthy, reloads.unhealthy_since) {
                (false, None) => {
                    reloads.unhealthy_since = Some(applied.slot);
                    None
                }
                (true, Some(since)) => {
                    reloads.unhealthy_since = None;
                    Some(since)
                }
                _ => None,
            };
            (health_restored_from, reloads.pending.is_some())
        };

        if slot > applied.slot
            && !reload_pending
            && inner.apply_chain(&confirmed, &applied).is_none()
        {
            tracing::warn!(
                slot,
                applied = applied.slot,
                "mint buffer misses a block; reloading from mints once it covers the slot"
            );
            inner.request_reload(Reload {
                after: applied.slot,
                target: slot,
                reason: ReloadReason::BlockMissing,
            });
        }
        if let Some(since) = health_restored_from {
            tracing::info!(
                since,
                "indexer healthy again; reloading mints changed since"
            );
            inner.request_reload(Reload {
                after: since,
                target: slot,
                reason: ReloadReason::HealthRestored,
            });
        }
        observe_mint_cache_stage(MintCacheSide::Api, "apply", started);
    }

    /// Spawns the cache's one background task, which runs for the life of the API. First the
    /// startup load: it waits until the indexer sets `last_db_slot`, then reads all of `mints`
    /// (retried every 10 s on failure; lookups read Postgres until it ends). Then, every 10 s,
    /// it runs the reload `on_confirmed` asked for, if any. No-op when the cache is disabled.
    pub fn spawn_loader(&self, db: DatabaseConnection) {
        let Some(inner) = self.0.clone() else {
            return;
        };
        tokio::spawn(async move {
            while let Err(e) = inner.load(&db).await {
                tracing::error!("mint cache load failed, retrying in {RETRY:?}: {e:#}");
                tokio::time::sleep(RETRY).await;
            }
            // Every 10 s, checks for a reload `on_confirmed` asked for: e.g. the gRPC feed missed
            // a block, so buffered blocks cannot advance the cache past it. The reload reads the
            // changed rows from `mints` once `last_db_slot` covers that slot; until then it waits.
            loop {
                tokio::time::sleep(RETRY).await;
                inner.run_reload(&db).await;
            }
        });
    }

    /// Resolves `keys` at `request_slot` from memory. A mint it holds goes into `found`; a mint
    /// closed at the request slot is left out, as Postgres would not find it. Returns the keys the
    /// caller must read from Postgres: every key while the cache is disabled or cannot answer.
    pub fn resolve(
        &self,
        keys: Vec<Pubkey>,
        request_slot: u64,
        found: &Mutex<HashMap<Pubkey, MintState>>,
    ) -> Vec<Pubkey> {
        let Some(inner) = &self.0 else {
            return keys;
        };
        let cache = inner.read();
        let mut found = found.lock().expect("mint resolver mutex poisoned");
        let mut postgres_keys = Vec::new();
        for key in keys {
            match lookup_one(&cache, &key, request_slot) {
                MintLookup::Found(state) => {
                    count_mint_lookup("memory", "hit");
                    found.insert(key, state);
                }
                // Closed at the request slot: no decimals, as when Postgres finds no mint.
                MintLookup::Closed => count_mint_lookup("memory", "closed"),
                MintLookup::Postgres(reason) => {
                    if reason == PostgresReason::Missing {
                        tracing::warn!(
                            mint = %key,
                            request_slot,
                            "mint missing from the live mint cache"
                        );
                    }
                    count_mint_lookup("postgres", reason.label());
                    postgres_keys.push(key);
                }
            }
        }
        postgres_keys
    }
}

/// The read rule. An immutable mint never changes, so it is exact at any slot.
fn lookup_one(state: &CacheState, key: &Pubkey, request_slot: u64) -> MintLookup {
    if state.status == Status::Loading {
        return MintLookup::Postgres(PostgresReason::Loading);
    }
    let behind = state.applied.slot < request_slot;
    match state.store.get(key) {
        StoredMint::Immutable { decimals } => MintLookup::Found(MintState {
            decimals,
            configs: None,
            immutable: true,
        }),
        StoredMint::Mutable(MutableMint { slot, .. }) if *slot > request_slot => {
            MintLookup::Postgres(PostgresReason::NewerThanRequest)
        }
        StoredMint::Tombstone { slot } if slot > request_slot => {
            MintLookup::Postgres(PostgresReason::NewerThanRequest)
        }
        _ if behind => MintLookup::Postgres(PostgresReason::Behind),
        StoredMint::Tombstone { .. } => MintLookup::Closed,
        StoredMint::Mutable(MutableMint { state, .. }) => MintLookup::Found(state.clone()),
        StoredMint::Missing => MintLookup::Postgres(PostgresReason::Missing),
    }
}

impl Inner {
    pub(super) fn read(&self) -> std::sync::RwLockReadGuard<'_, CacheState> {
        self.state.read().expect("mint cache lock poisoned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, CacheState> {
        self.state.write().expect("mint cache lock poisoned")
    }

    pub(super) fn buffer(&self) -> MutexGuard<'_, BTreeMap<u64, Vec<BufferedBlock>>> {
        self.buffer.lock().expect("mint cache mutex poisoned")
    }

    /// Applies the buffered blocks from `confirmed` back to `applied`, along the parent links.
    /// `None` when a block is missing or the chain does not reach `applied`.
    fn apply_chain(&self, confirmed: &Applied, applied: &Applied) -> Option<usize> {
        let mut buffer = self.buffer();
        let mut chain = Vec::new();
        let (mut slot, mut blockhash) = (confirmed.slot, confirmed.blockhash.clone());
        loop {
            let block = buffer
                .get(&slot)?
                .iter()
                .position(|b| blockhash.is_empty() || b.blockhash == blockhash)?;
            let block = &buffer[&slot][block];
            chain.push((slot, block.blockhash.clone()));
            if block.parent_slot == applied.slot {
                let linked = applied.blockhash.is_empty()
                    || block.parent_blockhash.is_empty()
                    || block.parent_blockhash == applied.blockhash;
                if !linked {
                    return None;
                }
                break;
            }
            if block.parent_slot < applied.slot {
                return None;
            }
            (slot, blockhash) = (block.parent_slot, block.parent_blockhash.clone());
        }

        let blocks = chain.len();
        let mut state = self.write();
        for (slot, blockhash) in chain.into_iter().rev() {
            let slot_blocks = buffer.get_mut(&slot).expect("walked block");
            let index = slot_blocks
                .iter()
                .position(|b| b.blockhash == blockhash)
                .expect("walked block");
            let block = slot_blocks.swap_remove(index);
            let mut counts: HashMap<ChangeKind, usize> = HashMap::new();
            for (pubkey, change) in block.changes {
                if let Some(kind) = state.store.apply(pubkey, slot, change) {
                    *counts.entry(kind).or_default() += 1;
                }
            }
            for kind in ChangeKind::ALL {
                let count = counts.get(&kind).copied().unwrap_or(0);
                observe_mint_changes(MintCacheSide::Api, kind.label(), count);
            }
        }
        state.applied = confirmed.clone();
        set_mint_cache_api_gauges(&state.store, state.applied.slot);
        drop(state);
        drop(buffer);
        self.prune_buffer(confirmed.slot);
        Some(blocks)
    }

    /// Drops every buffered block at or below `slot`, skipped blocks included.
    fn prune_buffer(&self, slot: u64) {
        let mut buffer = self.buffer();
        *buffer = buffer.split_off(&(slot + 1));
        MINT_CACHE_BUFFER_BLOCKS.set(buffer.len() as i64);
    }

    pub(super) fn reloads(&self) -> MutexGuard<'_, Reloads> {
        self.reloads.lock().expect("mint cache mutex poisoned")
    }

    /// Records a reload. A pending one widens to cover both.
    fn request_reload(&self, request: Reload) {
        let mut reloads = self.reloads();
        match reloads.pending.as_mut() {
            Some(pending) => {
                pending.after = pending.after.min(request.after);
                pending.target = pending.target.max(request.target);
            }
            None => reloads.pending = Some(request),
        }
    }

    /// Runs the pending reload once `last_db_slot` reaches its target: applies the rows with
    /// `slot > after`, and the cache is then current through `last_db_slot`. Until then, and
    /// after a failure, the reload stays pending.
    async fn run_reload(&self, db: &DatabaseConnection) {
        let Some(request) = self.reloads().pending else {
            return;
        };
        let last_db_slot = match read_last_db_slot(db.get_postgres_connection_pool()).await {
            Ok(Some(slot)) if slot >= request.target => slot,
            Ok(_) => return,
            Err(e) => {
                tracing::error!("mint cache: reading last_db_slot failed: {e:#}");
                return;
            }
        };
        let rows = match MintRow::read_after(db.get_postgres_connection_pool(), request.after).await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(?request, "mint cache reload failed: {e:#}");
                MINT_CACHE_RELOADS_TOTAL
                    .with_label_values(&[request.reason.label(), "error"])
                    .inc();
                return;
            }
        };
        let applied = {
            let mut state = self.write();
            for row in rows {
                state.store.apply_row(row.pubkey, row.slot, row.state);
            }
            if last_db_slot > state.applied.slot {
                state.applied = Applied {
                    slot: last_db_slot,
                    blockhash: String::new(),
                };
            }
            set_mint_cache_api_gauges(&state.store, state.applied.slot);
            state.applied.slot
        };
        self.prune_buffer(applied);
        {
            // A request added while this one ran stays pending for the next run.
            let mut reloads = self.reloads();
            if reloads.pending == Some(request) {
                reloads.pending = None;
            }
        }
        MINT_CACHE_RELOADS_TOTAL
            .with_label_values(&[request.reason.label(), "ok"])
            .inc();
        tracing::info!(?request, last_db_slot, "mint cache: reloaded from mints");
    }

    /// The startup load. Waits until the indexer bootstrap wrote `mints`, then reads
    /// `last_db_slot` before the table, page by page: the rows hold at least every change through
    /// it, and the buffer holds the blocks after it.
    async fn load(&self, db: &DatabaseConnection) -> anyhow::Result<()> {
        let pool = db.get_postgres_connection_pool();
        let start = loop {
            if let Some(slot) = read_last_db_slot(pool).await? {
                break Applied {
                    slot,
                    blockhash: String::new(),
                };
            }
            tokio::time::sleep(RETRY).await;
        };
        tracing::info!(
            start_slot = start.slot,
            "mint cache: loading the mints table"
        );
        let started = Instant::now();
        let mut store = MintStore::presized();
        let mut rows = 0u64;
        let mut after = Pubkey::default();
        loop {
            let page = MintRow::read_page(pool, &after).await?;
            let Some(last) = page.last() else {
                break;
            };
            after = last.pubkey;
            rows += page.len() as u64;
            for row in page {
                store.apply_row(row.pubkey, row.slot, row.state);
            }
        }
        anyhow::ensure!(
            rows > 0,
            "the mints table is empty; is [mint-cache] enabled on the indexer?"
        );

        let seconds = started.elapsed().as_secs_f64();
        {
            let mut state = self.write();
            state.store = store;
            state.applied = start.clone();
            state.status = Status::Live;
            set_mint_cache_api_gauges(&state.store, state.applied.slot);
        }
        self.prune_buffer(start.slot);
        set_mint_cache_startup_seconds(MintCacheSide::Api, seconds);
        tracing::info!(rows, seconds, start_slot = start.slot, "mint cache: live");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::mint_cache::tests::legacy_mint;
    use crate::utils::token::LEGACY_TOKEN_PROGRAM_ID;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    fn cache() -> MintCacheApi {
        MintCacheApi::from_config(Some(&MintCacheConfig {
            enabled: true,
            db_write_interval_slots: 1_000,
        }))
    }

    fn block(
        slot: u64,
        parent: u64,
        accounts: Vec<SubscribeUpdateAccountInfo>,
    ) -> SubscribeUpdateBlock {
        SubscribeUpdateBlock {
            slot,
            blockhash: format!("h{slot}"),
            parent_slot: parent,
            parent_blockhash: format!("h{parent}"),
            accounts,
            ..Default::default()
        }
    }

    fn mint_write(
        pubkey: Pubkey,
        owner: Pubkey,
        lamports: u64,
        data: Vec<u8>,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            owner: owner.to_bytes().to_vec(),
            lamports,
            data,
            ..Default::default()
        }
    }

    fn go_live(cache: &MintCacheApi, slot: u64) {
        let inner = cache.0.as_ref().unwrap();
        let mut state = inner.write();
        state.status = Status::Live;
        state.applied = Applied {
            slot,
            blockhash: format!("h{slot}"),
        };
        drop(state);
        inner.prune_buffer(slot);
    }

    fn confirmed(slot: u64) -> Applied {
        Applied {
            slot,
            blockhash: format!("h{slot}"),
        }
    }

    #[test]
    fn loading_sends_every_lookup_to_postgres() {
        let cache = cache();
        let inner = cache.0.clone().unwrap();
        assert_eq!(
            lookup_one(&inner.read(), &Pubkey::new_unique(), 10),
            MintLookup::Postgres(PostgresReason::Loading)
        );
    }

    #[test]
    fn applies_the_chain_of_buffered_blocks_up_to_the_confirmed_slot() {
        let cache = cache();
        let inner = cache.0.clone().unwrap();
        let mint = Pubkey::new_unique();
        cache.on_block(&block(
            101,
            100,
            vec![mint_write(mint, LEGACY_TOKEN_PROGRAM_ID, 1, legacy_mint(6))],
        ));
        cache.on_block(&block(103, 101, vec![]));
        go_live(&cache, 100);

        assert_eq!(inner.apply_chain(&confirmed(103), &confirmed(100)), Some(2));
        let found = lookup_one(&inner.read(), &mint, 103);
        assert!(matches!(found, MintLookup::Found(s) if s.decimals == 6 && s.immutable));
        assert_eq!(inner.read().applied.slot, 103);
        assert!(inner.buffer().is_empty());
    }

    #[test]
    fn a_missing_block_or_a_wrong_parent_breaks_the_chain() {
        let cache = cache();
        let inner = cache.0.clone().unwrap();
        cache.on_block(&block(103, 102, vec![]));
        go_live(&cache, 100);
        assert_eq!(inner.apply_chain(&confirmed(103), &confirmed(100)), None);

        let mut skipped = block(101, 100, vec![]);
        skipped.parent_blockhash = "other".to_string();
        cache.on_block(&skipped);
        assert_eq!(inner.apply_chain(&confirmed(101), &confirmed(100)), None);
        assert_eq!(inner.read().applied.slot, 100);
    }

    #[test]
    fn the_read_rule_respects_the_request_slot() {
        let cache = cache();
        let inner = cache.0.clone().unwrap();
        let closable = Pubkey::new_unique();
        go_live(&cache, 100);
        {
            let mut state = inner.write();
            let mutable = MintState {
                decimals: 6,
                configs: None,
                immutable: false,
            };
            state.store.apply(closable, 90, MintChange::Live(mutable));
            state.store.apply(closable, 101, MintChange::Closed);
            state.applied = confirmed(101);
        }
        // A request at 100 must not see the close at 101.
        assert_eq!(
            lookup_one(&inner.read(), &closable, 100),
            MintLookup::Postgres(PostgresReason::NewerThanRequest)
        );
        assert_eq!(
            lookup_one(&inner.read(), &closable, 101),
            MintLookup::Closed
        );
        // A request above `applied_through` cannot trust a mutable or missing key.
        assert_eq!(
            lookup_one(&inner.read(), &Pubkey::new_unique(), 102),
            MintLookup::Postgres(PostgresReason::Behind)
        );
        assert_eq!(
            lookup_one(&inner.read(), &Pubkey::new_unique(), 101),
            MintLookup::Postgres(PostgresReason::Missing)
        );
    }

    #[test]
    fn resolve_fills_found_mints_and_returns_the_postgres_keys() {
        let keys = vec![
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let found = Mutex::new(HashMap::new());
        // Disabled: every key goes to Postgres.
        let disabled = MintCacheApi::default();
        assert_eq!(disabled.resolve(keys.clone(), 100, &found), keys);
        assert!(found.lock().unwrap().is_empty());

        let cache = cache();
        let inner = cache.0.clone().unwrap();
        let [live, closed, missing] = [keys[0], keys[1], keys[2]];
        go_live(&cache, 100);
        {
            let mut state = inner.write();
            let mutable = MintState {
                decimals: 6,
                configs: None,
                immutable: false,
            };
            state
                .store
                .apply(live, 90, MintChange::Live(mutable.clone()));
            state.store.apply(closed, 90, MintChange::Live(mutable));
            state.store.apply(closed, 95, MintChange::Closed);
        }
        assert_eq!(cache.resolve(keys, 100, &found), vec![missing]);
        let found = found.lock().unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[&live].decimals, 6);
    }

    #[test]
    fn the_buffer_is_capped_and_pruned() {
        let cache = cache();
        let inner = cache.0.clone().unwrap();
        for slot in 1..=5 {
            cache.on_block(&block(slot, slot - 1, vec![]));
        }
        inner.prune_buffer(3);
        assert_eq!(
            inner.buffer().keys().copied().collect::<Vec<_>>(),
            vec![4, 5]
        );
    }
}
