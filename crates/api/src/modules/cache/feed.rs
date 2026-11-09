// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Keeps the cached gPA queries of `grpc-feed-owners` current from the API gRPC feed. Blocks wait
//! in a buffer until the slot syncronizer reports them, then update the queries before it
//! publishes the slot. See the "GPA cache gRPC feed" section of the README.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cloudbreak_core::modules::rpc_filter_type::RpcFilterType;
use serde::Serialize;
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig, encode_ui_account};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::RpcKeyedAccount;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use super::{CachedQuery, GpaCache, GpaProcessor, NormalizedQuery};
use crate::metrics;
use crate::utils::encoding::check_account_data_len_for_encoding;
use crate::utils::row_account::RowAccount;

/// Processed blocks kept above the confirmed cursor.
const MAX_BUFFERED_BLOCKS: usize = 1_024;
/// Confirmed blocks kept to catch up a query Postgres installed at an older slot (> 45 s of slots).
const MAX_RING_BLOCKS: usize = 512;
const MAX_RING_BYTES: usize = 512 * 1024 * 1024;
/// Confirmed slots that may wait for their gRPC blocks (~13 s) before the feed resets.
const MAX_PENDING_SLOTS: usize = 32;

/// One account write by a fed owner.
#[derive(Debug)]
struct LiveWrite {
    owner: Pubkey,
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
}

/// What one block changes for the fed owners.
#[derive(Debug, Default)]
struct BlockDelta {
    live: Vec<(Pubkey, LiveWrite)>,
    /// Every other pubkey the block wrote: closed, or now owned by a program outside the feed.
    gone: Vec<Pubkey>,
    bytes: usize,
}

impl BlockDelta {
    fn from_block(block: &SubscribeUpdateBlock, owners: &HashSet<Pubkey>) -> Self {
        let mut delta = Self::default();
        for account in &block.accounts {
            let (Ok(pubkey), Ok(owner)) = (
                Pubkey::try_from(account.pubkey.as_slice()),
                Pubkey::try_from(account.owner.as_slice()),
            ) else {
                continue;
            };
            if account.lamports > 0 && owners.contains(&owner) {
                delta.bytes += account.data.len() + size_of::<(Pubkey, LiveWrite)>();
                delta.live.push((
                    pubkey,
                    LiveWrite {
                        owner,
                        lamports: account.lamports,
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                        data: account.data.clone(),
                    },
                ));
            } else {
                delta.bytes += size_of::<Pubkey>();
                delta.gone.push(pubkey);
            }
        }
        delta
    }
}

struct BufferedBlock {
    blockhash: String,
    parent_slot: u64,
    parent_blockhash: String,
    delta: Arc<BlockDelta>,
    received: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Cursor {
    slot: u64,
    blockhash: String,
}

/// A confirmed slot Postgres reported above the applied cursor.
struct Pending {
    cursor: Cursor,
    since: Instant,
    /// Its blocks had not all arrived when Postgres reported it.
    waited: bool,
}

/// One block of a chain from the applied cursor to a confirmed slot.
struct ChainBlock {
    slot: u64,
    delta: Arc<BlockDelta>,
    received: Instant,
}

/// The buffered blocks between the applied cursor and a confirmed slot.
enum Link {
    /// Every block, oldest first.
    Chain(Vec<ChainBlock>),
    /// A block on the way has not arrived.
    Missing,
    /// The blocks do not link to the applied cursor.
    Broken,
}

/// Which published slot a query follows: `Finalized`, or the confirmed slot for the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    Confirmed,
    Finalized,
}

impl Group {
    fn of(commitment: CommitmentLevel) -> Self {
        match commitment {
            CommitmentLevel::Finalized => Self::Finalized,
            CommitmentLevel::Confirmed | CommitmentLevel::Processed => Self::Confirmed,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }
}

#[derive(Default)]
struct FeedState {
    /// The confirmed block the ring ends at. `None` until the first confirmed slot.
    confirmed: Option<Cursor>,
    /// Confirmed slots above `confirmed` whose gRPC blocks have not all arrived, oldest first.
    pending: VecDeque<Pending>,
    /// The latest finalized slot reported.
    finalized: u64,
    /// Every confirmed block above `ring_base`, oldest first.
    ring: VecDeque<(u64, Arc<BlockDelta>)>,
    ring_base: u64,
    ring_bytes: usize,
    /// A confirmed row reported the indexer unhealthy since the last healthy one.
    unhealthy: bool,
}

impl FeedState {
    /// Starts an empty ring at `slot`: only queries at `slot` or newer can catch up.
    fn reset_ring(&mut self, slot: u64) {
        self.ring.clear();
        self.ring_base = slot;
        self.ring_bytes = 0;
    }

    fn applied_slot(&self) -> Option<u64> {
        self.confirmed.as_ref().map(|c| c.slot)
    }

    /// The newest confirmed slot reported, applied or pending.
    fn newest_slot(&self) -> Option<u64> {
        self.pending
            .back()
            .map(|p| p.cursor.slot)
            .or(self.applied_slot())
    }

    fn push_ring(&mut self, chain: Vec<ChainBlock>) {
        for block in chain {
            self.ring_bytes += block.delta.bytes;
            self.ring.push_back((block.slot, block.delta));
        }
        while self.ring.len() > MAX_RING_BLOCKS || self.ring_bytes > MAX_RING_BYTES {
            let Some((slot, delta)) = self.ring.pop_front() else {
                break;
            };
            self.ring_bytes -= delta.bytes;
            self.ring_base = slot;
        }
        metrics::CLOUDBREAK_GPA_CACHE_FEED_BLOCKS
            .with_label_values(&["ring"])
            .set(self.ring.len() as i64);
    }

    fn status(&self, owners: &HashSet<Pubkey>) -> FeedStatus {
        let mut owners: Vec<String> = owners.iter().map(Pubkey::to_string).collect();
        owners.sort();
        FeedStatus {
            owners,
            confirmed_slot: self.confirmed.as_ref().map_or(0, |c| c.slot),
            pending_slots: self.pending.iter().map(|p| p.cursor.slot).collect(),
            finalized_slot: self.finalized,
            ring_base: self.ring_base,
            ring_blocks: self.ring.len(),
            ring_bytes: self.ring_bytes,
        }
    }
}

/// The feed cursors, as `/debug/modules/gpa_cache` shows them.
#[derive(Debug, Clone, Default, Serialize)]
pub struct FeedStatus {
    pub owners: Vec<String>,
    /// The slot the fed queries are applied to.
    pub confirmed_slot: u64,
    /// Confirmed slots published while their gRPC blocks are missing; queries serve `confirmed_slot`.
    pub pending_slots: Vec<u64>,
    pub finalized_slot: u64,
    /// Queries at this slot or newer can catch up from the ring.
    pub ring_base: u64,
    pub ring_blocks: usize,
    pub ring_bytes: usize,
}

struct Inner {
    owners: HashSet<Pubkey>,
    cache: Arc<RwLock<GpaCache>>,
    buffer: Mutex<BTreeMap<u64, Vec<BufferedBlock>>>,
    /// Taken by the slot syncronizer and by `on_pending_block`.
    state: Mutex<FeedState>,
    /// The newest pending slot, or 0; read by the feed thread without the state lock.
    pending_up_to: AtomicU64,
}

/// Cheap-clone handle. The default is disabled.
#[derive(Clone, Default)]
pub struct GpaFeed(Option<Arc<Inner>>);

impl GpaFeed {
    /// Enabled when `[gpa-cache]` has `grpc-feed-enabled = true`.
    pub fn new(processor: &GpaProcessor) -> Self {
        let Some(cache) = processor.cache() else {
            return Self::default();
        };
        let owners: HashSet<Pubkey> = {
            let cache = cache.read().expect("gpa cache rwlock poisoned");
            cache.config.feed_owners().iter().copied().collect()
        };
        if owners.is_empty() {
            return Self::default();
        }
        Self(Some(Arc::new(Inner {
            owners,
            cache: cache.clone(),
            buffer: Mutex::new(BTreeMap::new()),
            state: Mutex::new(FeedState::default()),
            pending_up_to: AtomicU64::new(0),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Feed thread: keeps the block's changes for the fed owners until its slot is confirmed.
    /// True when a pending confirmed slot may now apply: then call `on_pending_block` off this thread.
    pub fn on_block(&self, block: &SubscribeUpdateBlock) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        let buffered = BufferedBlock {
            blockhash: block.blockhash.clone(),
            parent_slot: block.parent_slot,
            parent_blockhash: block.parent_blockhash.clone(),
            delta: Arc::new(BlockDelta::from_block(block, &inner.owners)),
            received: Instant::now(),
        };
        let mut buffer = inner.buffer();
        let slot_blocks = buffer.entry(block.slot).or_default();
        slot_blocks.retain(|b| b.blockhash != buffered.blockhash);
        slot_blocks.push(buffered);
        while buffer.len() > MAX_BUFFERED_BLOCKS {
            buffer.pop_first();
        }
        set_buffer_gauge(buffer.len());
        drop(buffer);
        block.slot <= inner.pending_up_to.load(Ordering::SeqCst)
    }

    /// Slot syncronizer, before it publishes a confirmed slot: applies the chain of buffered
    /// blocks up to it and brings the fed queries to it. When a block is still missing, the slot
    /// waits in `pending` and the queries keep serving their slot until `on_pending_block`.
    pub fn on_confirmed(&self, slot: u64, blockhash: Option<&str>, healthy: bool) {
        let Some(inner) = &self.0 else {
            return;
        };
        let started = Instant::now();
        let mut state = inner.state();
        if !healthy {
            state.unhealthy = true;
        } else if std::mem::take(&mut state.unhealthy) {
            // The indexer repaired a gap: Postgres changed below the slots the queries came from.
            tracing::info!(target: "gpa_cache", slot, "indexer healthy again; dropping the fed queries");
            count_reset("health_restored");
            inner.remove_fed_queries();
        }

        let confirmed = Cursor {
            slot,
            blockhash: blockhash.unwrap_or_default().to_string(),
        };
        match state.newest_slot() {
            None => {
                state.reset_ring(slot);
                state.confirmed = Some(confirmed);
            }
            Some(newest) if slot <= newest => return,
            Some(_) => {
                state.pending.push_back(Pending {
                    cursor: confirmed,
                    since: started,
                    waited: false,
                });
                // Published before the buffer is read, so a block landing meanwhile triggers `on_pending_block`.
                inner.sync_pending(&state);
                inner.advance(&mut state);
                if state.pending.back().is_some_and(|p| p.cursor.slot == slot) {
                    inner.pg_first(&mut state);
                }
            }
        }
        inner.catch_up(&state, started);
    }

    /// Blocking pool, after `on_block` returned true: applies the pending confirmed slots whose
    /// blocks have arrived and brings the fed queries to them.
    pub fn on_pending_block(&self) {
        let Some(inner) = &self.0 else {
            return;
        };
        let started = Instant::now();
        let mut state = inner.state();
        if state.pending.is_empty() {
            return;
        }
        let applied = state.applied_slot();
        inner.advance(&mut state);
        if state.applied_slot() != applied {
            inner.catch_up(&state, started);
        }
    }

    /// Slot syncronizer, before it publishes a finalized slot: brings the `finalized` queries to
    /// it, or to the confirmed cursor when the finalized slot is ahead.
    pub fn on_finalized(&self, slot: u64) {
        let Some(inner) = &self.0 else {
            return;
        };
        let started = Instant::now();
        let mut state = inner.state();
        if slot <= state.finalized {
            return;
        }
        state.finalized = slot;
        let Some(confirmed) = state.confirmed.as_ref().map(|c| c.slot) else {
            return;
        };
        let mut phases = Phases {
            ring: started.elapsed(),
            ..Default::default()
        };
        inner.update(&state, None, Some(slot.min(confirmed)), &mut phases);
        phases.observe(Group::Finalized);
        observe_update(Group::Finalized, started);
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, FeedState> {
        self.state.lock().expect("gpa feed mutex poisoned")
    }

    fn buffer(&self) -> MutexGuard<'_, BTreeMap<u64, Vec<BufferedBlock>>> {
        self.buffer.lock().expect("gpa feed mutex poisoned")
    }

    fn cache_read(&self) -> RwLockReadGuard<'_, GpaCache> {
        self.cache.read().expect("gpa cache rwlock poisoned")
    }

    fn cache_write(&self) -> RwLockWriteGuard<'_, GpaCache> {
        self.cache.write().expect("gpa cache rwlock poisoned")
    }

    /// The buffered blocks from `confirmed` back to `applied` along the parent links.
    fn chain(&self, confirmed: &Cursor, applied: &Cursor) -> Link {
        let buffer = self.buffer();
        let mut chain = Vec::new();
        let (mut slot, mut blockhash) = (confirmed.slot, confirmed.blockhash.as_str());
        loop {
            let Some(block) = buffer.get(&slot).and_then(|blocks| {
                blocks
                    .iter()
                    .find(|b| blockhash.is_empty() || b.blockhash == blockhash)
            }) else {
                return Link::Missing;
            };
            chain.push(ChainBlock {
                slot,
                delta: block.delta.clone(),
                received: block.received,
            });
            if block.parent_slot == applied.slot {
                let linked = applied.blockhash.is_empty()
                    || block.parent_blockhash.is_empty()
                    || block.parent_blockhash == applied.blockhash;
                if !linked {
                    return Link::Broken;
                }
                break;
            }
            if block.parent_slot < applied.slot {
                return Link::Broken;
            }
            (slot, blockhash) = (block.parent_slot, block.parent_blockhash.as_str());
        }
        chain.reverse();
        Link::Chain(chain)
    }

    /// Applies the newest pending slot whose blocks have all arrived, and every pending slot
    /// below it. A pending slot whose blocks do not link to the cursor resets the feed.
    fn advance(&self, state: &mut FeedState) {
        let Some(applied) = state.confirmed.clone() else {
            return;
        };
        for i in (0..state.pending.len()).rev() {
            match self.chain(&state.pending[i].cursor, &applied) {
                Link::Missing => continue,
                Link::Broken => return self.reset(state, "chain_broken"),
                Link::Chain(chain) => {
                    let target = state.pending[i].cursor.clone();
                    for pending in state.pending.drain(..=i) {
                        if let Some(block) = chain.iter().find(|b| b.slot == pending.cursor.slot) {
                            observe_gap(&pending, block.received);
                        }
                        if pending.waited {
                            tracing::info!(target: "gpa_cache", slot = pending.cursor.slot, waited_ms = pending.since.elapsed().as_millis() as u64, "gpa feed applied a slot postgres confirmed first");
                        }
                    }
                    state.push_ring(chain);
                    state.confirmed = Some(target);
                    self.sync_pending(state);
                    return;
                }
            }
        }
    }

    /// Postgres confirmed the newest pending slot before the gRPC feed delivered its blocks.
    fn pg_first(&self, state: &mut FeedState) {
        let depth = state.pending.len();
        let applied = state.applied_slot().unwrap_or_default();
        let Some(newest) = state.pending.back_mut() else {
            return;
        };
        newest.waited = true;
        let slot = newest.cursor.slot;
        metrics::CLOUDBREAK_GPA_CACHE_FEED_PG_FIRST_TOTAL.inc();
        metrics::CLOUDBREAK_GPA_CACHE_FEED_PG_FIRST_DEPTH.observe(depth as f64);
        tracing::warn!(target: "gpa_cache", slot, applied, depth, slots_behind = slot - applied, "postgres confirmed a slot before the api grpc feed delivered its block");
        if depth > MAX_PENDING_SLOTS {
            self.reset(state, "grpc_late");
        }
    }

    /// Starts a new ring at the newest confirmed slot: the fed queries go back to Postgres until
    /// they catch up from it.
    fn reset(&self, state: &mut FeedState, reason: &str) {
        let Some(newest) = state.pending.pop_back() else {
            return;
        };
        tracing::warn!(target: "gpa_cache", slot = newest.cursor.slot, applied = state.applied_slot().unwrap_or_default(), pending = state.pending.len() + 1, reason, "gpa feed resets the ring");
        count_reset(reason);
        state.pending.clear();
        state.reset_ring(newest.cursor.slot);
        state.confirmed = Some(newest.cursor);
        self.unfeed_queries();
        self.sync_pending(state);
    }

    fn sync_pending(&self, state: &FeedState) {
        let newest = state.pending.back().map_or(0, |p| p.cursor.slot);
        self.pending_up_to.store(newest, Ordering::SeqCst);
        metrics::CLOUDBREAK_GPA_CACHE_FEED_BLOCKS
            .with_label_values(&["pending"])
            .set(state.pending.len() as i64);
    }

    /// Drops the buffer below the cursor and brings the fed queries to it; `finalized` ones to
    /// the finalized slot when it is below.
    fn catch_up(&self, state: &FeedState, started: Instant) {
        let Some(applied) = state.applied_slot() else {
            return;
        };
        self.prune_buffer(applied);
        // A finalized slot reported before its confirmed slot catches up here.
        let finalized = (state.finalized > 0).then(|| state.finalized.min(applied));
        let mut phases = Phases {
            ring: started.elapsed(),
            ..Default::default()
        };
        self.update(state, Some(applied), finalized, &mut phases);
        phases.observe(Group::Confirmed);
        observe_update(Group::Confirmed, started);
    }

    /// Drops every buffered block at or below `slot`, skipped blocks included.
    fn prune_buffer(&self, slot: u64) {
        let mut buffer = self.buffer();
        *buffer = buffer.split_off(&(slot + 1));
        set_buffer_gauge(buffer.len());
    }

    fn is_fed(&self, query: &NormalizedQuery) -> bool {
        self.owners.contains(&query.program)
    }

    /// Brings every fed query of each group to its target slot, in three steps: collect under
    /// the read lock, encode without a lock, swap under the write lock.
    fn update(
        &self,
        state: &FeedState,
        confirmed: Option<u64>,
        finalized: Option<u64>,
        phases: &mut Phases,
    ) {
        let target_of = |query: &NormalizedQuery| match Group::of(query.commitment) {
            Group::Confirmed => confirmed,
            Group::Finalized => finalized,
        };
        let mut lap = Instant::now();
        let due: Vec<(NormalizedQuery, CachedQuery, u64)> = {
            let cache = self.cache_read();
            cache
                .queries
                .iter()
                .filter(|(query, _)| self.is_fed(query))
                .filter_map(|(query, cached)| {
                    let target = target_of(query)?;
                    let due =
                        cached.slot < target || (cached.slot == target && cached.sorted.is_none());
                    due.then(|| (query.clone(), cached.clone(), target))
                })
                .collect()
        };
        phases.collect = lap.elapsed();

        let mut results = Vec::with_capacity(due.len());
        for (query, cached, target) in due {
            let group = Group::of(query.commitment).label();
            if cached.slot < state.ring_base {
                count_query_update(group, "too_old");
                continue;
            }
            let advanced = advance(&query, &cached, target, &state.ring, phases);
            count_query_update(
                group,
                if advanced.is_some() {
                    "updated"
                } else {
                    "dropped"
                },
            );
            results.push((query, cached, advanced));
        }

        lap = Instant::now();
        let mut freed = Vec::new();
        let mut cache = self.cache_write();
        for (query, old, advanced) in results {
            // A FinalizeJob that replaced the entry meanwhile wins; the next slot catches it up.
            let unchanged = cache.queries.get(&query).is_some_and(|current| {
                current.slot == old.slot && Arc::ptr_eq(&current.accounts, &old.accounts)
            });
            if !unchanged {
                continue;
            }
            freed.extend(match advanced {
                Some(entry) => cache.replace_query(query, entry),
                None => cache.take_query(&query),
            });
        }
        cache.feed_status = Some(state.status(&self.owners));
        cache.update_size_metrics();
        drop(cache);
        phases.swap = lap.elapsed();
        lap = Instant::now();
        drop(freed);
        phases.free = lap.elapsed();
    }

    /// Removes every fed query, so the next request rebuilds it from Postgres.
    fn remove_fed_queries(&self) {
        let mut cache = self.cache_write();
        let fed: Vec<NormalizedQuery> = cache
            .queries
            .keys()
            .filter(|q| self.is_fed(q))
            .cloned()
            .collect();
        let freed: Vec<CachedQuery> = fed.iter().filter_map(|q| cache.take_query(q)).collect();
        drop(cache);
        drop(freed);
    }

    /// Stops serving the fed queries from memory until they catch up from the new ring.
    fn unfeed_queries(&self) {
        let mut cache = self.cache_write();
        let freed: Vec<_> = cache
            .queries
            .iter_mut()
            .filter(|(query, _)| self.owners.contains(&query.program))
            .filter_map(|(_, cached)| cached.sorted.take())
            .collect();
        drop(cache);
        drop(freed);
    }
}

/// `cached` brought to `target` with the ring blocks above its slot. `None` when an account
/// fails to encode: the next request then goes to Postgres and gets the same error.
fn advance(
    query: &NormalizedQuery,
    cached: &CachedQuery,
    target: u64,
    ring: &VecDeque<(u64, Arc<BlockDelta>)>,
    phases: &mut Phases,
) -> Option<CachedQuery> {
    let merge_started = Instant::now();
    let mut encoding = Duration::ZERO;
    // The latest change per pubkey. A `gone` pubkey matters only if the query holds it.
    let mut changes: HashMap<Pubkey, Option<&LiveWrite>> = HashMap::new();
    for (_, delta) in ring
        .iter()
        .filter(|(slot, _)| *slot > cached.slot && *slot <= target)
    {
        for pubkey in &delta.gone {
            if cached.accounts.contains_key(pubkey) || changes.contains_key(pubkey) {
                changes.insert(*pubkey, None);
            }
        }
        for (pubkey, write) in &delta.live {
            changes.insert(*pubkey, Some(write));
        }
    }
    if changes.is_empty() && cached.sorted.is_some() {
        return Some(CachedQuery {
            slot: target,
            ..cached.clone()
        });
    }

    let filters = Filters::compile(&query.filters);
    let mut accounts = (*cached.accounts).clone();
    let mut encoded = 0u64;
    for (pubkey, change) in changes {
        match change {
            Some(write) if write.owner == query.program && filters.matches(&write.data) => {
                let lap = Instant::now();
                let bytes = encode(&pubkey, write, query.encoding, query.data_slice)?;
                encoding += lap.elapsed();
                accounts.insert(pubkey, bytes);
                encoded += 1;
            }
            _ => {
                accounts.remove(&pubkey);
            }
        }
    }
    phases.encode += encoding;
    phases.merge += merge_started.elapsed() - encoding;
    phases.encoded += encoded;
    let sort_started = Instant::now();
    let mut sorted: Vec<(Pubkey, Bytes)> = accounts.iter().map(|(k, v)| (*k, v.clone())).collect();
    sorted.sort_unstable_by_key(|(pubkey, _)| *pubkey);
    let size = sorted.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    phases.sort += sort_started.elapsed();
    Some(CachedQuery {
        size,
        cache_hits: (accounts.len() as u64).saturating_sub(encoded),
        accounts: Arc::new(accounts),
        slot: target,
        sorted: Some(sorted.into()),
    })
}

/// The bytes the request path streams for this account (`process_row` then `serde_json`).
fn encode(
    pubkey: &Pubkey,
    write: &LiveWrite,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
) -> Option<Bytes> {
    check_account_data_len_for_encoding(encoding, data_slice, write.data.len(), pubkey).ok()?;
    let account = RowAccount {
        lamports: write.lamports,
        data: &write.data,
        owner: write.owner,
        executable: write.executable,
        rent_epoch: write.rent_epoch,
    };
    let account = encode_ui_account(pubkey, &account, encoding, None, data_slice);
    let keyed = RpcKeyedAccount {
        pubkey: pubkey.to_string(),
        account,
    };
    serde_json::to_vec(&keyed).ok().map(Bytes::from)
}

/// The filters of one query, with the memcmp bytes decoded once.
enum Filter {
    DataSize(u64),
    Memcmp {
        offset: usize,
        bytes: Vec<u8>,
    },
    TokenAccountState,
    /// A memcmp whose bytes do not decode: the SQL path adds no clause for it.
    Any,
}

struct Filters(Vec<Filter>);

impl Filters {
    /// `ValueCmp` never reaches the cache (see `GpaProcessor::for_request`).
    fn compile(filters: &[RpcFilterType]) -> Self {
        Self(
            filters
                .iter()
                .map(|filter| match filter {
                    RpcFilterType::DataSize(size) => Filter::DataSize(*size),
                    RpcFilterType::Memcmp(memcmp) => match memcmp.bytes() {
                        Some(bytes) => Filter::Memcmp {
                            offset: memcmp.offset(),
                            bytes: bytes.into_owned(),
                        },
                        None => Filter::Any,
                    },
                    RpcFilterType::TokenAccountState => Filter::TokenAccountState,
                    RpcFilterType::ValueCmp(_) => Filter::Any,
                })
                .collect(),
        )
    }

    /// Mirrors the SQL predicates of `utils::sql_filters` on a non-token program.
    fn matches(&self, data: &[u8]) -> bool {
        self.0.iter().all(|filter| match filter {
            Filter::DataSize(size) => data.len() as u64 == *size,
            Filter::Memcmp { offset, bytes } => {
                bytes.is_empty()
                    || data.get(*offset..offset.saturating_add(bytes.len()))
                        == Some(bytes.as_slice())
            }
            Filter::TokenAccountState => {
                let len = data.len();
                len > 108
                    && data[108] != 0
                    && (len == 165 || (len > 165 && len != 355 && data[165] == 2))
            }
            Filter::Any => true,
        })
    }
}

fn set_buffer_gauge(len: usize) {
    metrics::CLOUDBREAK_GPA_CACHE_FEED_BLOCKS
        .with_label_values(&["buffer"])
        .set(len as i64);
}

fn count_reset(reason: &str) {
    metrics::CLOUDBREAK_GPA_CACHE_FEED_RESETS_TOTAL
        .with_label_values(&[reason])
        .inc();
}

/// The time between a confirmed slot's gRPC block and its Postgres confirmation.
fn observe_gap(pending: &Pending, received: Instant) {
    let (first, gap) = if pending.waited {
        ("pg", received.saturating_duration_since(pending.since))
    } else {
        ("grpc", pending.since.saturating_duration_since(received))
    };
    metrics::CLOUDBREAK_GPA_CACHE_FEED_GRPC_PG_GAP_US
        .with_label_values(&[first])
        .observe(gap.as_secs_f64() * 1_000_000.0);
}

fn count_query_update(group: &str, result: &str) {
    metrics::CLOUDBREAK_GPA_CACHE_FEED_QUERY_UPDATES_TOTAL
        .with_label_values(&[group, result])
        .inc();
}

/// Where one update spends its time; `merge` excludes `encode`.
#[derive(Default)]
struct Phases {
    ring: Duration,
    collect: Duration,
    merge: Duration,
    encode: Duration,
    sort: Duration,
    swap: Duration,
    free: Duration,
    encoded: u64,
}

impl Phases {
    fn observe(&self, group: Group) {
        let group = group.label();
        for (phase, took) in [
            ("ring", self.ring),
            ("collect", self.collect),
            ("merge", self.merge),
            ("encode", self.encode),
            ("sort", self.sort),
            ("swap", self.swap),
            ("free", self.free),
        ] {
            observe_phase(group, phase, took);
        }
        metrics::CLOUDBREAK_GPA_CACHE_FEED_ENCODED_ACCOUNTS_TOTAL
            .with_label_values(&[group])
            .inc_by(self.encoded);
    }
}

/// Also used by the slot syncronizer for the whole wait it adds before publishing a slot.
pub fn observe_phase(commitment: &str, phase: &str, took: Duration) {
    metrics::CLOUDBREAK_GPA_CACHE_FEED_UPDATE_PHASE_DURATION_US
        .with_label_values(&[commitment, phase])
        .observe(took.as_secs_f64() * 1_000_000.0);
}

fn observe_update(group: Group, started: Instant) {
    metrics::CLOUDBREAK_GPA_CACHE_FEED_UPDATE_DURATION_US
        .with_label_values(&[group.label()])
        .observe(started.elapsed().as_secs_f64() * 1_000_000.0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudbreak_core::GpaCacheConfig;
    use cloudbreak_core::modules::rpc_filter_type::{Memcmp, RpcProgramAccountsConfig};
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    const VOTE: Pubkey = solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111");

    fn feed() -> GpaFeed {
        let processor = GpaProcessor::new(Some(GpaCacheConfig {
            max_total_bytes: 1 << 30,
            min_bytes_per_query: 0,
            max_bytes_query_cleanup: None,
            max_pinned_bytes_ratio: None,
            grpc_feed_enabled: true,
            grpc_feed_owners: vec![VOTE],
        }));
        GpaFeed::new(&processor)
    }

    fn inner(feed: &GpaFeed) -> &Arc<Inner> {
        feed.0.as_ref().unwrap()
    }

    fn write(
        pubkey: Pubkey,
        owner: Pubkey,
        lamports: u64,
        data: Vec<u8>,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            owner: owner.to_bytes().to_vec(),
            lamports,
            rent_epoch: u64::MAX,
            data,
            ..Default::default()
        }
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

    fn query(commitment: CommitmentLevel, filters: Vec<RpcFilterType>) -> NormalizedQuery {
        NormalizedQuery::new(VOTE, filters, UiAccountEncoding::Base64, None, commitment)
    }

    /// Installs `accounts` for `query` at `slot`, as a Postgres `FinalizeJob` would.
    fn install(feed: &GpaFeed, query: &NormalizedQuery, slot: u64, accounts: &[(Pubkey, Vec<u8>)]) {
        let accounts: HashMap<Pubkey, Bytes> = accounts
            .iter()
            .map(|(pubkey, data)| {
                let live = LiveWrite {
                    owner: VOTE,
                    lamports: 1,
                    executable: false,
                    rent_epoch: u64::MAX,
                    data: data.clone(),
                };
                (
                    *pubkey,
                    encode(pubkey, &live, query.encoding, query.data_slice).unwrap(),
                )
            })
            .collect();
        let size = accounts.values().map(|b| b.len() as u64).sum();
        let entry = CachedQuery {
            accounts: Arc::new(accounts),
            slot,
            size,
            cache_hits: 0,
            sorted: None,
        };
        inner(feed)
            .cache_write()
            .replace_query(query.clone(), entry);
    }

    fn cached(feed: &GpaFeed, query: &NormalizedQuery) -> Option<CachedQuery> {
        inner(feed).cache_read().queries.get(query).cloned()
    }

    fn sorted_keys(entry: &CachedQuery) -> Vec<Pubkey> {
        entry
            .sorted
            .as_ref()
            .unwrap()
            .iter()
            .map(|(k, _)| *k)
            .collect()
    }

    fn confirm(feed: &GpaFeed, slot: u64) {
        feed.on_confirmed(slot, Some(&format!("h{slot}")), true);
    }

    #[test]
    fn filters_mirror_the_sql_predicates() {
        let filters = Filters::compile(&[
            RpcFilterType::DataSize(4),
            RpcFilterType::Memcmp(Memcmp::new_raw_bytes(1, vec![2, 3])),
        ]);
        assert!(filters.matches(&[1, 2, 3, 4]));
        assert!(!filters.matches(&[1, 2, 4, 4]));
        assert!(!filters.matches(&[1, 2, 3]));
        // A memcmp past the end never matches; an empty one always does.
        assert!(
            !Filters::compile(&[RpcFilterType::Memcmp(Memcmp::new_raw_bytes(3, vec![4, 5]))])
                .matches(&[1, 2, 3, 4])
        );
        assert!(
            Filters::compile(&[RpcFilterType::Memcmp(Memcmp::new_raw_bytes(9, vec![]))])
                .matches(&[1])
        );
    }

    #[test]
    fn encode_matches_the_request_path_bytes() {
        let pubkey = Pubkey::new_unique();
        let data = vec![7u8; 40];
        let live = LiveWrite {
            owner: VOTE,
            lamports: 5,
            executable: false,
            rent_epoch: u64::MAX,
            data: data.clone(),
        };
        let row = RowAccount {
            lamports: 5,
            data: &data,
            owner: VOTE,
            executable: false,
            rent_epoch: u64::MAX,
        };
        let slice = Some(UiDataSliceConfig {
            offset: 0,
            length: 0,
        });
        let expected = serde_json::to_vec(&RpcKeyedAccount {
            pubkey: pubkey.to_string(),
            account: encode_ui_account(&pubkey, &row, UiAccountEncoding::Base64, None, slice),
        })
        .unwrap();
        assert_eq!(
            encode(&pubkey, &live, UiAccountEncoding::Base64, slice).unwrap(),
            expected
        );
        // Over the base58 limit: the query is dropped, as Postgres would return an error.
        let big = LiveWrite {
            data: vec![0; 200],
            ..live
        };
        assert!(encode(&pubkey, &big, UiAccountEncoding::Base58, None).is_none());
    }

    #[test]
    fn a_confirmed_slot_updates_adds_and_removes_accounts() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![RpcFilterType::DataSize(3)]);
        let [kept, changed, closed, resized, moved, added] = [(); 6].map(|_| Pubkey::new_unique());
        let other = Pubkey::new_unique();
        install(
            &feed,
            &q,
            100,
            &[
                (kept, vec![1; 3]),
                (changed, vec![1; 3]),
                (closed, vec![1; 3]),
                (resized, vec![1; 3]),
                (moved, vec![1; 3]),
            ],
        );
        confirm(&feed, 100);
        assert_eq!(cached(&feed, &q).unwrap().sorted.as_ref().unwrap().len(), 5);

        feed.on_block(&block(
            101,
            100,
            vec![
                write(changed, VOTE, 1, vec![2; 3]),
                write(closed, VOTE, 0, vec![]),
                write(resized, VOTE, 1, vec![1; 4]),
                write(moved, other, 1, vec![1; 3]),
                write(added, VOTE, 1, vec![3; 3]),
            ],
        ));
        confirm(&feed, 101);

        let entry = cached(&feed, &q).unwrap();
        assert_eq!(entry.slot, 101);
        let mut expected = vec![kept, changed, added];
        expected.sort();
        assert_eq!(sorted_keys(&entry), expected);
        let live = LiveWrite {
            owner: VOTE,
            lamports: 1,
            executable: false,
            rent_epoch: u64::MAX,
            data: vec![2; 3],
        };
        assert_eq!(
            entry.accounts[&changed],
            encode(&changed, &live, q.encoding, None).unwrap()
        );
        assert_eq!(
            entry.size,
            entry.accounts.values().map(|b| b.len() as u64).sum::<u64>()
        );
        assert_eq!(inner(&feed).cache_read().size, entry.size);
    }

    #[test]
    fn a_new_query_catches_up_from_the_ring_and_an_old_one_cannot() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![]);
        let account = Pubkey::new_unique();
        confirm(&feed, 100);
        feed.on_block(&block(101, 100, vec![write(account, VOTE, 1, vec![9])]));
        confirm(&feed, 101);
        feed.on_block(&block(102, 101, vec![]));

        // Postgres served it at 100, before the write at 101.
        install(&feed, &q, 100, &[]);
        confirm(&feed, 102);
        let entry = cached(&feed, &q).unwrap();
        assert_eq!((entry.slot, sorted_keys(&entry)), (102, vec![account]));

        let old = query(CommitmentLevel::Processed, vec![RpcFilterType::DataSize(1)]);
        install(&feed, &old, 99, &[]);
        feed.on_block(&block(103, 102, vec![]));
        confirm(&feed, 103);
        let entry = cached(&feed, &old).unwrap();
        assert_eq!(entry.slot, 99);
        assert!(entry.sorted.is_none());
    }

    #[test]
    fn finalized_queries_follow_the_finalized_slot() {
        let feed = feed();
        let q = query(CommitmentLevel::Finalized, vec![]);
        let account = Pubkey::new_unique();
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);
        feed.on_finalized(100);
        assert_eq!(cached(&feed, &q).unwrap().slot, 100);

        feed.on_block(&block(101, 100, vec![write(account, VOTE, 1, vec![1])]));
        confirm(&feed, 101);
        assert_eq!(cached(&feed, &q).unwrap().slot, 100);
        // Finalized ahead of confirmed: the query stops at the confirmed cursor.
        feed.on_finalized(102);
        let entry = cached(&feed, &q).unwrap();
        assert_eq!((entry.slot, sorted_keys(&entry)), (101, vec![account]));
        feed.on_block(&block(102, 101, vec![]));
        confirm(&feed, 102);
        assert_eq!(cached(&feed, &q).unwrap().slot, 102);
    }

    fn processor(feed: &GpaFeed) -> GpaProcessor {
        GpaProcessor::Cached {
            cache: inner(feed).cache.clone(),
            cached_query: None,
            normalized_query: None,
            new_accounts_for_query: Default::default(),
            cache_hits: 0,
            new_slot: 0,
            mint_resolver: Default::default(),
        }
    }

    fn request(
        commitment: solana_commitment_config::CommitmentConfig,
        min_context_slot: Option<u64>,
    ) -> RpcProgramAccountsConfig {
        let mut config = RpcProgramAccountsConfig::default();
        config.account_config.encoding = Some(UiAccountEncoding::Base64);
        config.account_config.commitment = Some(commitment);
        config.account_config.min_context_slot = min_context_slot;
        config
    }

    fn served_slot(feed: &GpaFeed, config: &RpcProgramAccountsConfig, latest: u64) -> Option<u64> {
        processor(feed)
            .fed_query(VOTE, config, latest)
            .map(|(slot, _)| slot)
    }

    fn pending(feed: &GpaFeed) -> Vec<u64> {
        inner(feed)
            .state()
            .pending
            .iter()
            .map(|p| p.cursor.slot)
            .collect()
    }

    #[test]
    fn postgres_first_serves_the_applied_slot_until_the_block_arrives() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![]);
        let account = Pubkey::new_unique();
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);

        let counted = metrics::CLOUDBREAK_GPA_CACHE_FEED_PG_FIRST_TOTAL.get();
        let gaps = || {
            metrics::CLOUDBREAK_GPA_CACHE_FEED_GRPC_PG_GAP_US
                .with_label_values(&["pg"])
                .get_sample_count()
        };
        let gaps_before = gaps();
        confirm(&feed, 101);
        assert!(metrics::CLOUDBREAK_GPA_CACHE_FEED_PG_FIRST_TOTAL.get() > counted);
        assert_eq!(pending(&feed), vec![101]);
        let entry = cached(&feed, &q).unwrap();
        assert_eq!(entry.slot, 100);
        assert!(entry.sorted.is_some());

        let confirmed = solana_commitment_config::CommitmentConfig::confirmed();
        assert_eq!(
            served_slot(&feed, &request(confirmed, None), 101),
            Some(100)
        );
        // A minContextSlot above the applied slot goes to Postgres.
        assert_eq!(
            served_slot(&feed, &request(confirmed, Some(101)), 101),
            None
        );

        assert!(feed.on_block(&block(101, 100, vec![write(account, VOTE, 1, vec![1])])));
        feed.on_pending_block();
        let entry = cached(&feed, &q).unwrap();
        assert_eq!((entry.slot, sorted_keys(&entry)), (101, vec![account]));
        assert!(pending(&feed).is_empty());
        assert!(gaps() > gaps_before);
        assert_eq!(
            served_slot(&feed, &request(confirmed, None), 101),
            Some(101)
        );
        // Nothing waits any more: a new block needs no catch-up task.
        assert!(!feed.on_block(&block(102, 101, vec![])));
    }

    #[test]
    fn pending_slots_accumulate_and_apply_as_their_blocks_link() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![]);
        let [a, b, c] = [(); 3].map(|_| Pubkey::new_unique());
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);
        confirm(&feed, 101);
        confirm(&feed, 102);
        assert_eq!(pending(&feed), vec![101, 102]);

        // 102 cannot link while its parent 101 is missing.
        assert!(feed.on_block(&block(102, 101, vec![write(b, VOTE, 1, vec![2])])));
        feed.on_pending_block();
        assert_eq!(cached(&feed, &q).unwrap().slot, 100);
        assert_eq!(pending(&feed), vec![101, 102]);

        confirm(&feed, 103);
        assert!(feed.on_block(&block(101, 100, vec![write(a, VOTE, 1, vec![1])])));
        feed.on_pending_block();
        let entry = cached(&feed, &q).unwrap();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!((entry.slot, sorted_keys(&entry)), (102, expected));
        assert_eq!(pending(&feed), vec![103]);

        assert!(feed.on_block(&block(103, 102, vec![write(c, VOTE, 1, vec![3])])));
        feed.on_pending_block();
        assert_eq!(cached(&feed, &q).unwrap().slot, 103);
        assert!(pending(&feed).is_empty());
        assert_eq!(inner(&feed).state().ring.len(), 3);
    }

    #[test]
    fn too_many_pending_slots_reset_the_ring_and_unfeed_the_queries() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![]);
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);
        let last = 100 + MAX_PENDING_SLOTS as u64;
        for slot in 101..=last {
            confirm(&feed, slot);
        }
        assert_eq!(pending(&feed).len(), MAX_PENDING_SLOTS);
        assert!(cached(&feed, &q).unwrap().sorted.is_some());

        confirm(&feed, last + 1);
        assert!(pending(&feed).is_empty());
        let entry = cached(&feed, &q).unwrap();
        assert_eq!(entry.slot, 100);
        assert!(entry.sorted.is_none());
        assert_eq!(inner(&feed).state().ring_base, last + 1);
        assert_eq!(inner(&feed).state().applied_slot(), Some(last + 1));
    }

    #[test]
    fn a_block_that_does_not_link_resets_the_ring() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![]);
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);
        let mut fork = block(101, 100, vec![]);
        fork.parent_blockhash = "other".to_string();
        feed.on_block(&fork);
        confirm(&feed, 101);

        assert!(pending(&feed).is_empty());
        let entry = cached(&feed, &q).unwrap();
        assert_eq!(entry.slot, 100);
        assert!(entry.sorted.is_none());
        assert_eq!(inner(&feed).state().ring_base, 101);
    }

    #[test]
    fn finalized_queries_wait_with_the_confirmed_cursor() {
        let feed = feed();
        let q = query(CommitmentLevel::Finalized, vec![]);
        let account = Pubkey::new_unique();
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);
        feed.on_finalized(100);
        confirm(&feed, 101);
        feed.on_finalized(101);
        assert_eq!(cached(&feed, &q).unwrap().slot, 100);
        let finalized = solana_commitment_config::CommitmentConfig::finalized();
        assert_eq!(
            served_slot(&feed, &request(finalized, None), 101),
            Some(100)
        );

        assert!(feed.on_block(&block(101, 100, vec![write(account, VOTE, 1, vec![1])])));
        feed.on_pending_block();
        let entry = cached(&feed, &q).unwrap();
        assert_eq!((entry.slot, sorted_keys(&entry)), (101, vec![account]));
    }

    #[test]
    fn health_restored_drops_the_fed_queries() {
        let feed = feed();
        let q = query(CommitmentLevel::Confirmed, vec![]);
        install(&feed, &q, 100, &[]);
        confirm(&feed, 100);
        feed.on_block(&block(101, 100, vec![]));
        feed.on_confirmed(101, Some("h101"), false);
        assert!(cached(&feed, &q).is_some());
        feed.on_block(&block(102, 101, vec![]));
        confirm(&feed, 102);
        assert!(cached(&feed, &q).is_none());
        assert_eq!(inner(&feed).cache_read().size, 0);
    }

    #[test]
    fn fed_query_serves_only_a_current_fed_entry() {
        let feed = feed();
        let processor = GpaProcessor::Cached {
            cache: inner(&feed).cache.clone(),
            cached_query: None,
            normalized_query: None,
            new_accounts_for_query: Default::default(),
            cache_hits: 0,
            new_slot: 0,
            mint_resolver: Default::default(),
        };
        let q = query(CommitmentLevel::Confirmed, vec![]);
        let mut config =
            cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig::default();
        config.account_config.encoding = Some(UiAccountEncoding::Base64);
        config.account_config.commitment =
            Some(solana_commitment_config::CommitmentConfig::confirmed());

        install(&feed, &q, 100, &[(Pubkey::new_unique(), vec![1])]);
        assert!(processor.fed_query(VOTE, &config, 100).is_none());
        confirm(&feed, 100);
        let (slot, accounts) = processor.fed_query(VOTE, &config, 100).unwrap();
        assert_eq!((slot, accounts.len()), (100, 1));
        assert!(processor.fed_query(VOTE, &config, 101).is_none());
        assert!(
            processor
                .fed_query(Pubkey::new_unique(), &config, 100)
                .is_none()
        );
    }

    #[test]
    fn cleanup_never_evicts_fed_queries() {
        let feed = feed();
        let fed = query(CommitmentLevel::Confirmed, vec![]);
        let other = NormalizedQuery::new(
            Pubkey::new_unique(),
            vec![],
            UiAccountEncoding::Base64,
            None,
            CommitmentLevel::Confirmed,
        );
        install(&feed, &fed, 100, &[(Pubkey::new_unique(), vec![1])]);
        install(&feed, &other, 101, &[(Pubkey::new_unique(), vec![2])]);

        // Ask for more than the whole cache, as a too-large query does.
        let mut evicted = Vec::new();
        inner(&feed)
            .cache_write()
            .cleanup_old_queries(1 << 31, &mut evicted);

        assert_eq!(evicted.len(), 1);
        assert!(cached(&feed, &fed).is_some());
        assert!(cached(&feed, &other).is_none());
    }
}
