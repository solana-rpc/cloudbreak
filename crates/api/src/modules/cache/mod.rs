// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use bytes::Bytes;
use cloudbreak_core::GpaCacheConfig;
use cloudbreak_core::modules::rpc_filter_type::{
    RpcFilterType, RpcProgramAccountsConfig, has_value_cmp,
};
use sea_orm::sqlx::Row;
use sea_orm::sqlx::postgres::PgRow;
use solana_account_decoder::UiAccountEncoding;
use solana_account_decoder::UiDataSliceConfig;
use solana_account_decoder::parse_account_data::AccountAdditionalDataV3;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::RpcKeyedAccount;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::RpcError;
use crate::methods::program;
use crate::methods::program::GpaDbQueryInput;
use crate::metrics;

/// A query that has been accepted for caching, carrying everything needed to
/// install it. Built on the request path, executed on the blocking pool by
/// [`FinalizeJob::run`].
struct FinalizeJob {
    cache: Arc<RwLock<GpaCache>>,
    normalized_query: NormalizedQuery,
    accounts: Vec<(Pubkey, Bytes)>,
    query_bytes: u64,
    slot: u64,
    cache_hits: u64,
    /// The entry this request read from, if any. Carried so that the worker —
    /// not the request — holds the last reference to the map it is about to
    /// replace, and therefore pays to free it. See [`FinalizeJob::run`].
    previous_query: Option<CachedQuery>,
    /// Trace context of the request that produced this query, so the worker's
    /// span still hangs off the originating trace.
    parent_cx: opentelemetry::Context,
}

impl FinalizeJob {
    /// Hands the insertion to the blocking pool and returns immediately.
    ///
    /// Installing a query is `O(accounts)`: building the account map, taking the
    /// cache write lock, and deallocating the version being replaced. The client
    /// is not waiting on any of it, so none of it belongs on the request path.
    /// The join handle is dropped on purpose — there is nothing to await, and
    /// losing an insertion is harmless because the next request repopulates it.
    fn spawn(self) {
        let inflight = metrics::FinalizeInFlightGuard::new(self.query_bytes as i64);

        tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();

            self.run();

            // Timed out here so every exit path in `run` is counted.
            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&["cache_finalize_query", "cached"])
                .observe(started.elapsed().as_micros() as f64);

            // Also what moves the guard into the closure: without this the guard
            // would drop as soon as `spawn` returns and the gauges would report
            // nothing as in flight.
            drop(inflight);
        });
    }

    /// Installs the query in the cache. Runs on the blocking pool, never on a
    /// request.
    ///
    /// The declaration order of `new_entry`, `replaced` and `cache_guard` is
    /// load-bearing: Rust drops locals in reverse order, so the write lock is
    /// always released before any account map is deallocated, on every exit path
    /// including the early returns.
    fn run(self) {
        let start_time = std::time::Instant::now();
        let Self {
            cache,
            normalized_query,
            accounts,
            query_bytes,
            slot,
            cache_hits,
            previous_query,
            parent_cx,
        } = self;

        let span = tracing::info_span!(
            "gpa_cache_finalize_query",
            cache_hits = cache_hits as i64,
            query_bytes = query_bytes as i64,
            query_accounts = accounts.len() as i64,
            wall_time = tracing::field::Empty,
            locked_micros = tracing::field::Empty,
        );
        // Must happen before `enter()`: `set_parent` rejects a span that has
        // already been started. Errors only mean there is no OpenTelemetry layer
        // or the span was filtered out, in which case there is no trace to attach
        // to anyway, so they are ignored (as in the crate's own examples).
        let _ = span.set_parent(parent_cx);
        let _span_guard = span.enter();

        let new_entry = CachedQuery {
            accounts: Arc::new(accounts.into_iter().collect()),
            slot,
            size: query_bytes,
            cache_hits,
        };

        // Versions of this query that are no longer reachable from the cache.
        // `previous_query` goes in first so this job holds the last reference to
        // the map being replaced.
        let mut replaced: Vec<CachedQuery> = Vec::new();
        replaced.extend(previous_query);

        let start_locked_time = std::time::Instant::now();
        let mut cache_guard = cache.write().expect("can't lock gpa cache rwlock");
        let lock_held_start = std::time::Instant::now();

        // This runs behind the request that produced the query, so a newer
        // version may already be cached. Installing an older snapshot would be
        // internally consistent but would force the next request to refresh more
        // accounts, so discard this one instead.
        if cache_guard
            .queries
            .get(&normalized_query)
            .is_some_and(|current| current.slot >= slot)
        {
            metrics::CLOUDBREAK_GPA_CACHE_FINALIZE_SKIPPED_TOTAL
                .with_label_values(&["stale_slot"])
                .inc();
            return;
        }

        // Cleanup cache if needed
        if let Some(bytes_freed) = cache_guard.cleanup_old_queries(query_bytes, &mut replaced)
            && bytes_freed < query_bytes
        {
            tracing::error!(target: "gpa_cache", "Failed to cleanup old queries, not enough bytes freed {}", query_bytes - bytes_freed);
            metrics::CLOUDBREAK_GPA_CACHE_FINALIZE_SKIPPED_TOTAL
                .with_label_values(&["cleanup_failed"])
                .inc();
            return;
        }

        // Insert the query into the main map (replacing the older query if existed)
        let older_query = cache_guard
            .queries
            .insert(normalized_query.clone(), new_entry);

        // Update map size, crediting back the bytes of the query we just
        // replaced (if any) so the counter tracks what is actually held.
        cache_guard.size += query_bytes;
        if let Some(older_query) = &older_query {
            cache_guard.size = cache_guard.size.saturating_sub(older_query.size);
        }

        // Mirror the same accounting for pinned bytes: credit the new query if
        // it is pinned, and credit back the replaced query if it was pinned.
        if cache_guard.is_pinned_size(query_bytes) {
            cache_guard.pinned_size += query_bytes;
        }
        if let Some(older_query) = &older_query
            && cache_guard.is_pinned_size(older_query.size)
        {
            cache_guard.pinned_size = cache_guard.pinned_size.saturating_sub(older_query.size);
        }

        cache_guard.insert_query_for_slot(
            normalized_query,
            slot,
            older_query.as_ref().map(|q| q.slot),
        );
        replaced.extend(older_query);

        cache_guard.update_size_metrics();

        // `finalize_query_locked` spans lock acquisition plus the critical
        // section, so it also reflects time spent queueing behind other
        // writers. `finalize_query_held` isolates the critical section itself.
        let locked_micros = start_locked_time.elapsed().as_micros() as i64;
        let held_micros = lock_held_start.elapsed().as_micros() as i64;
        metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&["finalize_query_locked", "cached"])
            .observe(locked_micros as f64);
        metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&["finalize_query_held", "cached"])
            .observe(held_micros as f64);

        drop(cache_guard);
        drop(replaced);

        span.record("wall_time", start_time.elapsed().as_millis() as i64);
        span.record("locked_micros", locked_micros);
    }
}

#[derive(Debug, Clone)]
pub struct GpaCache {
    /// Map of queries by their key, and stores the slot for which the query
    ///  was served and the list of accounts that were returned.
    ///
    /// Note: For now there is no account sharing between queries
    pub queries: HashMap<NormalizedQuery, CachedQuery>,
    /// Map of queries per slot. Used to clean up cache from old queries.
    /// BTreeMap to have a cheap way to grab and remove the oldest slot.
    pub queries_for_slot: BTreeMap<u64, Vec<NormalizedQuery>>,
    pub config: GpaCacheConfig,
    /// Size of the cache in bytes
    pub size: u64,
    /// Size in bytes of the currently pinned (non-evictable) queries, i.e. those
    /// larger than `config.max_bytes_query_cleanup`. Tracked incrementally so we
    /// can cap how much space pinned queries are allowed to collectively hold.
    pub pinned_size: u64,
}

#[derive(Debug, Clone)]
pub enum GpaProcessor {
    Standard,
    Cached {
        /// Pointer to the cache instance
        cache: Arc<RwLock<GpaCache>>,
        /// Pointer to the cache result for the query
        cached_query: Option<CachedQuery>,
        /// Pointer to the normalized query used as key for the cache
        normalized_query: Option<NormalizedQuery>,
        /// Because this comes from the result of `process_row`, it automatically
        /// handles the new accounts, closed accounts, and updated accounts compared
        /// to the previous cached query. (it will only read from cache for not closed
        /// or updated accounts)
        new_accounts_for_query: Arc<Mutex<Vec<(Pubkey, Bytes)>>>,
        /// Number of cache hits for the query
        cache_hits: u64,
        /// Slot for which the new query was served
        new_slot: u64,
    },
}

impl GpaProcessor {
    /// If there is no cache `config` present, the processor will be `Standard`
    /// so there will be no cache used.
    pub fn new(config: Option<GpaCacheConfig>) -> Self {
        if let Some(config) = config {
            Self::Cached {
                cache: Arc::new(RwLock::new(GpaCache::new(config))),
                cached_query: None,
                normalized_query: None,
                new_accounts_for_query: Arc::new(Mutex::new(Vec::new())),
                cache_hits: 0,
                new_slot: 0,
            }
        } else {
            Self::Standard
        }
    }

    pub fn get_type(&self) -> &str {
        match self {
            Self::Standard => "standard",
            Self::Cached { .. } => "cached",
        }
    }

    /// Builds the processor for a single request.
    ///
    /// Caching is **bypassed** (a `Standard` processor is returned) whenever the
    /// request carries a `ValueCmp` filter, even if the cache is configured.
    pub fn for_request(&self, filters: &[RpcFilterType]) -> Self {
        match self {
            Self::Standard => Self::Standard,
            // ValueCmp queries are not cacheable.
            Self::Cached { .. } if has_value_cmp(filters) => Self::Standard,
            Self::Cached { cache, .. } => Self::Cached {
                cache: cache.clone(),
                cached_query: None,
                normalized_query: None,
                new_accounts_for_query: Arc::new(Mutex::new(Vec::new())),
                cache_hits: 0,
                new_slot: 0,
            },
        }
    }

    pub fn load_sql(&mut self, input: &GpaDbQueryInput) -> String {
        match self {
            Self::Standard => program::load_sql(input),
            Self::Cached {
                cache,
                cached_query,
                normalized_query,
                new_slot,
                ..
            } => {
                let (normalized_query_result, cached_query_result) = cache
                    .read()
                    .expect("gpa cache rwlock poisoned")
                    .get_cached_query(input.program, &input.config);

                let cached_slot = cached_query_result.as_ref().map(|c| c.slot).unwrap_or(0);

                *cached_query = cached_query_result;
                *normalized_query = Some(normalized_query_result);
                *new_slot = input.latest_slot;

                let sql = include_str!("./gpa_with_cache.sql");
                let sql = sql.replace("-- {accounts_filters}", &input.accounts_filters);
                let sql = sql.replace("-- {snapshot_filters}", &input.snapshot_filters);
                let sql = sql.replace("$2", input.latest_slot.to_string().as_str());

                sql.replace("$3", cached_slot.to_string().as_str())
            }
        }
    }

    pub fn process_row(
        &self,
        row: PgRow,
        encoding: UiAccountEncoding,
        data_slice: Option<UiDataSliceConfig>,
        response_bytes: &mut u64,
        encode_span: &tracing::Span,
        additional_mint_data: Option<AccountAdditionalDataV3>,
    ) -> Result<MaybeJsonAccount, RpcError> {
        match self {
            Self::Standard => {
                let keyed = program::process_row(
                    row,
                    encoding,
                    data_slice,
                    response_bytes,
                    encode_span,
                    additional_mint_data,
                )?;

                Ok(MaybeJsonAccount::Fresh(keyed))
            }
            Self::Cached {
                cache,
                cached_query,
                normalized_query,
                ..
            } => match cached_query {
                Some(cached_query) => GpaCache::process_row(
                    row,
                    encoding,
                    data_slice,
                    response_bytes,
                    encode_span,
                    additional_mint_data,
                    cached_query,
                    cache,
                    normalized_query.as_ref(),
                ),
                // If the query is not cached, also process it normally
                None => {
                    let encoded_account = program::process_row(
                        row,
                        encoding,
                        data_slice,
                        response_bytes,
                        encode_span,
                        additional_mint_data,
                    )?;

                    Ok(MaybeJsonAccount::Fresh(encoded_account))
                }
            },
        }
    }

    /// Append the `(pubkey, encoded_bytes)` pairs into the accumulator. Called
    /// from `streaming.rs` after each batch flush.
    pub fn update_new_accounts_for_query(
        &mut self,
        new_accounts_batch: Vec<(Pubkey, Bytes)>,
        batch_cache_hits: u64,
    ) {
        match self {
            Self::Standard => {}
            Self::Cached {
                new_accounts_for_query,
                cache_hits,
                ..
            } => {
                *cache_hits += batch_cache_hits;

                new_accounts_for_query
                    .lock()
                    .expect("new_accounts_for_query mutex poisoned")
                    .extend(new_accounts_batch);
            }
        }
    }

    /// Hand the accumulated `(pubkey, bytes)` pairs to the background finalize
    /// thread, which commits them as the new `CachedQuery`.
    ///
    /// If the GpaProcessor is `Standard`, this is a no-op.
    ///
    /// Only queries larger than `config.min_bytes_per_query` are cached, and that
    /// is decided here rather than on the worker: the overwhelming majority of
    /// queries fall below the threshold, and rejecting them costs a single pass
    /// over the accumulated pairs. Everything expensive — building the account
    /// map, taking the cache write lock, cleaning up old queries to stay within
    /// `config.max_total_bytes`, and freeing the replaced version — happens on
    /// the blocking pool, off the request path. See [`FinalizeJob::run`].
    pub fn finalize_query(&mut self) {
        let Self::Cached {
            cache,
            cached_query,
            normalized_query,
            new_accounts_for_query,
            new_slot,
            cache_hits,
        } = self
        else {
            return;
        };

        let Some(normalized_query) = normalized_query.take() else {
            tracing::error!(target: "gpa_cache", "No normalized query found");
            return;
        };

        let accounts = std::mem::take(
            &mut *new_accounts_for_query
                .lock()
                .expect("new_accounts_for_query mutex poisoned"),
        );

        // Summing the encoded lengths is a cheap linear pass with no allocation,
        // unlike building the map, so the threshold is checked first. The config
        // is immutable after startup, so a read lock is enough and never blocks
        // other readers.
        let query_bytes: u64 = accounts.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        let min_bytes_per_query = cache
            .read()
            .expect("gpa cache rwlock poisoned")
            .config
            .min_bytes_per_query as u64;

        if query_bytes < min_bytes_per_query {
            return;
        }

        FinalizeJob {
            cache: cache.clone(),
            normalized_query,
            accounts,
            query_bytes,
            slot: *new_slot,
            cache_hits: *cache_hits,
            previous_query: cached_query.take(),
            parent_cx: tracing::Span::current().context(),
        }
        .spawn();
    }
}

#[derive(Debug, Clone)]
pub struct CachedQuery {
    /// JSON-encoded account bytes keyed by pubkey. Stored as `Bytes` so that
    /// on a future cache hit we can append it directly into the next response's
    /// `BytesMut` (just a memcpy) with no re-serialization. Each `Bytes` owns a
    /// tight, per-account allocation: freshly-serialized accounts are copied out
    /// of the shared streaming chunk when inserted (see `drain_pending_into_cache`
    /// in `http::streaming`) so a cache entry never pins a whole ~64 KB chunk,
    /// which keeps retained memory in line with the accounted [`Self::size`].
    pub accounts: Arc<HashMap<Pubkey, Bytes>>,
    /// Slot for which the cached query was served.
    pub slot: u64,
    /// Size of the cached query in bytes
    pub size: u64,
    /// Number of cache hits for the query
    pub cache_hits: u64,
}

/// Representation of a gpa query with all the parameters that will affect the response.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NormalizedQuery {
    pub program: Pubkey,
    /// Sorted (to avoid order affecting the hash)
    pub filters: Vec<RpcFilterType>,
    pub encoding: UiAccountEncoding,
    pub data_slice: Option<UiDataSliceConfig>,
    pub commitment: CommitmentLevel,
}

impl NormalizedQuery {
    /// Sorts the filters to avoid order affecting the hash
    pub fn new(
        program: Pubkey,
        mut filters: Vec<RpcFilterType>,
        encoding: UiAccountEncoding,
        data_slice: Option<UiDataSliceConfig>,
        commitment: CommitmentLevel,
    ) -> Self {
        // Sort using a discriminator plus the bytes for memcmp (and length for data size)
        // `ValueCmp` is unreachable: requests carrying a `ValueCmp` filter bypass
        // the cache entirely (see `GpaProcessor::for_request`), so they never
        // reach `NormalizedQuery`.
        filters.sort_by_cached_key(|f| match f {
            RpcFilterType::DataSize(n) => (0u8, *n, Vec::<u8>::new()),
            RpcFilterType::Memcmp(m) => (
                1u8,
                m.offset() as u64,
                m.bytes().map(|b| b.into_owned()).unwrap_or_default(),
            ),
            RpcFilterType::TokenAccountState => (2u8, 0, Vec::<u8>::new()),
            RpcFilterType::ValueCmp(_) => {
                unreachable!("ValueCmp queries bypass the cache and never reach NormalizedQuery")
            }
        });

        Self {
            program,
            filters,
            encoding,
            data_slice,
            commitment,
        }
    }
}

impl GpaCache {
    pub fn new(config: GpaCacheConfig) -> Self {
        let cache = Self {
            queries: HashMap::new(),
            queries_for_slot: BTreeMap::new(),
            config,
            size: 0,
            pinned_size: 0,
        };
        cache.update_size_metrics();
        cache
    }

    /// Publishes the current cache size and configured maximum to Prometheus.
    /// Utilization (0-100) is derived from these two gauges in Grafana.
    fn update_size_metrics(&self) {
        crate::metrics::CLOUDBREAK_GPA_CACHE_SIZE_BYTES.set(self.size as i64);
        crate::metrics::CLOUDBREAK_GPA_CACHE_MAX_BYTES.set(self.config.max_total_bytes as i64);
    }

    /// Whether a query of the given size is pinned (skipped by cleanup). A query
    /// is pinned when `max_bytes_query_cleanup` is set and the query is larger
    /// than that threshold.
    pub fn is_pinned_size(&self, size: u64) -> bool {
        self.config
            .max_bytes_query_cleanup
            .is_some_and(|max_evictable| size > max_evictable as u64)
    }

    /// Maximum number of bytes pinned queries are collectively allowed to hold.
    /// Returns `u64::MAX` (no cap) when `max_pinned_bytes_ratio` is unset.
    pub fn pinned_threshold(&self) -> u64 {
        match self.config.max_pinned_bytes_ratio {
            Some(ratio) => (self.config.max_total_bytes as f64 * ratio) as u64,
            None => u64::MAX,
        }
    }

    fn get_cached_query(
        &self,
        program: Pubkey,
        rpc_gpa_config: &RpcProgramAccountsConfig,
    ) -> (NormalizedQuery, Option<CachedQuery>) {
        let filters = rpc_gpa_config.filters.clone().unwrap_or_default();

        // get the default encoding
        let encoding = rpc_gpa_config
            .account_config
            .encoding
            .unwrap_or(UiAccountEncoding::Binary);

        let data_slice = rpc_gpa_config.account_config.data_slice;
        let commitment = rpc_gpa_config
            .account_config
            .commitment
            .map(|commitment_config| commitment_config.commitment)
            .unwrap_or(CommitmentLevel::Finalized);

        let query = NormalizedQuery::new(program, filters, encoding, data_slice, commitment);

        let cached_query = self.queries.get(&query);

        if let Some(cached_query) = cached_query {
            return (query, Some(cached_query.clone()));
        }

        (query, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_row(
        row: PgRow,
        encoding: UiAccountEncoding,
        data_slice: Option<UiDataSliceConfig>,
        response_bytes: &mut u64,
        encode_span: &tracing::Span,
        additional_mint_data: Option<AccountAdditionalDataV3>,
        cached_query: &CachedQuery,
        cache: &Arc<RwLock<GpaCache>>,
        normalized_query: Option<&NormalizedQuery>,
    ) -> Result<MaybeJsonAccount, RpcError> {
        encode_span.in_scope(|| {
            // We use owner field to detect if this is a row returning data or not (which means it's a cached row)
            let owner_bytes: Option<[u8; 32]> = row.get(1);

            match owner_bytes {
                Some(_) => {
                    // Response not in cache, process it normally
                    let keyed = program::process_row(
                        row,
                        encoding,
                        data_slice,
                        response_bytes,
                        encode_span,
                        additional_mint_data,
                    )?;

                    Ok(MaybeJsonAccount::Fresh(keyed))
                }
                None => {
                    // Cache hit: PG only sends (pubkey, NULL, NULL, slot, NULL, ...)
                    let pubkey = Pubkey::new_from_array(row.get(0));
                    let slot = row.get::<i64, _>(3) as u64;

                    if slot > cached_query.slot {
                        tracing::error!(target: "gpa_cache", "Slot {} is greater than cached slot {}", slot, cached_query.slot);
                    }

                    let bytes = cached_query.accounts.get(&pubkey).ok_or_else(|| {
                        // If the account was returned as cached from DB, should be in cache, so error if not
                        tracing::error!(target: "gpa_cache", "Account {} not found in cached query", pubkey);

                        // The cached query is internally inconsistent: evict it so
                        // the next request rebuilds it from scratch, and flag the
                        // failure with a dedicated status on the request counter.
                        if let Some(normalized_query) = normalized_query {
                            cache
                                .write()
                                .expect("gpa cache rwlock poisoned")
                                .remove_query(normalized_query);
                        }

                        metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                            .with_label_values(&["gPA", "cacheError"])
                            .inc();

                        RpcError::InternalError
                    })?;

                    Ok(MaybeJsonAccount::Cached {
                        pubkey,
                        bytes: bytes.clone(),
                    })
                }
            }
        })
    }

    /// It will first remove the query from the `queries_for_slot` bucket if it exists.
    ///
    /// Takes the replaced entry's slot rather than the entry itself so the caller
    /// retains ownership and can deallocate it after dropping the write lock.
    pub fn insert_query_for_slot(
        &mut self,
        normalized_query: NormalizedQuery,
        slot: u64,
        older_slot: Option<u64>,
    ) {
        // Remove old version of the query
        if let Some(prev_slot) = older_slot
            && let Some(queries_list) = self.queries_for_slot.get_mut(&prev_slot)
        {
            queries_list.retain(|q| q != &normalized_query);
            // If there is no more queries for the slot, remove the slot from the map
            if queries_list.is_empty() {
                self.queries_for_slot.remove(&prev_slot);
            }
        }

        // Insert the new query for the slot
        self.queries_for_slot
            .entry(slot)
            .or_default()
            .push(normalized_query);
    }

    /// Removes a single query from the cache, fixing up `size`, `pinned_size`
    /// and the per-slot bucket accounting.
    ///
    /// Used to self-heal when a cached query is found to be internally
    /// inconsistent (an account the DB reported as cached is missing from the
    /// in-memory `CachedQuery`), so a subsequent request re-populates it fresh.
    pub fn remove_query(&mut self, query: &NormalizedQuery) {
        let Some(removed) = self.queries.remove(query) else {
            return;
        };

        self.size = self.size.saturating_sub(removed.size);
        if self.is_pinned_size(removed.size) {
            self.pinned_size = self.pinned_size.saturating_sub(removed.size);
        }

        if let Some(bucket) = self.queries_for_slot.get_mut(&removed.slot) {
            bucket.retain(|q| q != query);
            if bucket.is_empty() {
                self.queries_for_slot.remove(&removed.slot);
            }
        }

        self.update_size_metrics();
    }

    /// it will delete the oldes queries until reach the `bytes_to_free` target.
    /// Returns the number of bytes freed.
    ///
    /// It will only cleanup if space is needed for the new query.
    ///
    /// Queries larger than `config.max_bytes_query_cleanup` (when set) are
    /// normally pinned: skipped during eviction and kept in the cache. The pin
    /// is soft, however: pinned queries are only allowed to collectively hold up
    /// to `pinned_threshold()` bytes. When pinned usage is over that cap, the
    /// oldest pinned queries are evicted (via this same oldest-first walk) until
    /// usage drops back under the cap. Because of pinning, cleanup may still free
    /// less than requested when the oldest slots hold mostly pinned queries that
    /// are within the cap.
    ///
    /// Evicted entries are appended to `evicted` rather than dropped here, so the
    /// caller can deallocate them once the write lock is released.
    pub fn cleanup_old_queries(
        &mut self,
        mut bytes_to_free: u64,
        evicted: &mut Vec<CachedQuery>,
    ) -> Option<u64> {
        let mut bytes_freed: u64 = 0;

        let available_bytes = match (self.config.max_total_bytes as u64).checked_sub(self.size) {
            Some(available_bytes) => available_bytes,
            None => {
                tracing::error!(target: "gpa_cache", "Cache size is greater than max total bytes");
                return None;
            }
        };

        // Size pressure: how many bytes we must evict to fit the new query.
        let size_cleanup_needed = available_bytes < bytes_to_free;
        bytes_to_free = bytes_to_free.saturating_sub(available_bytes);

        // Pinned pressure: pinned queries are over their collective cap.
        let pinned_threshold = self.pinned_threshold();
        let pinned_cleanup_needed = self.pinned_size > pinned_threshold;

        if !size_cleanup_needed && !pinned_cleanup_needed {
            return None;
        }

        let max_evictable = self.config.max_bytes_query_cleanup.map(|b| b as u64);

        // Walk slots oldest-first (`BTreeMap::retain` visits in ascending key
        // order), draining queries from each bucket in place. A slot whose
        // bucket becomes empty is dropped from the map.
        //
        // Two independent budgets drive eviction:
        //   - size: keep evicting (non-pinned) queries until `bytes_to_free` is
        //     met, leaving pinned queries alone.
        //   - pinned: while pinned usage is over `pinned_threshold`, evict the
        //     oldest pinned queries too until usage is back under the cap.
        //
        // Borrow `queries`/`size`/`pinned_size` separately from
        // `queries_for_slot` so the closure can mutate them while iterating.
        let queries = &mut self.queries;
        let size = &mut self.size;
        let pinned_size = &mut self.pinned_size;
        self.queries_for_slot.retain(|_slot, bucket| {
            if bytes_freed >= bytes_to_free && *pinned_size <= pinned_threshold {
                return true; // both budgets satisfied: leave remaining slots untouched
            }
            bucket.retain(|q| {
                let need_size = bytes_freed < bytes_to_free;
                let need_pinned = *pinned_size > pinned_threshold;
                if !need_size && !need_pinned {
                    return true;
                }

                let is_pinned = max_evictable.is_some_and(|max_evictable| {
                    queries.get(q).is_some_and(|c| c.size > max_evictable)
                });

                if is_pinned {
                    // Keep pinned queries unless we are over the pinned cap.
                    if !need_pinned {
                        return true;
                    }
                } else if !need_size {
                    // Non-pinned query, but there is no size pressure: keep it.
                    return true;
                }

                if let Some(cached) = queries.remove(q) {
                    *size = size.saturating_sub(cached.size);
                    if is_pinned {
                        *pinned_size = pinned_size.saturating_sub(cached.size);
                    }
                    bytes_freed = bytes_freed.saturating_add(cached.size);

                    // An evicted query is "used" if it ever served a cache hit.
                    // A high rate of "unused" evictions signals cache churn.
                    let used = if cached.cache_hits > 0 {
                        "used"
                    } else {
                        "unused"
                    };
                    crate::metrics::CLOUDBREAK_GPA_CACHE_EVICTIONS_TOTAL
                        .with_label_values(&[used])
                        .inc();
                    crate::metrics::CLOUDBREAK_GPA_CACHE_EVICTED_BYTES_TOTAL
                        .with_label_values(&[used])
                        .inc_by(cached.size);

                    evicted.push(cached);
                }
                false
            });
            !bucket.is_empty()
        });

        self.update_size_metrics();

        Some(bytes_freed + available_bytes)
    }
}

/// One row coming out of the encoding stage.
///
/// `Cached` means the row was a cache hit: the JSON bytes were already
/// computed by a previous response and live in the prior `CachedQuery`. The
/// streaming layer just appends those bytes verbatim.
///
/// `Fresh` means the row needs to be serialized into JSON now. The streaming
/// layer serializes it into a `BytesMut` and slices the resulting range into
/// a `Bytes` for the cache.
pub enum MaybeJsonAccount {
    Cached { pubkey: Pubkey, bytes: Bytes },
    Fresh(KeyedRpcAccount),
}

/// Pairs a pubkey with its encoded `RpcKeyedAccount` so the streaming layer
/// can index the freshly serialized bytes into the cache without re-parsing
/// the base58 pubkey out of `RpcKeyedAccount.pubkey: String`.
pub struct KeyedRpcAccount {
    pub pubkey: Pubkey,
    pub account: RpcKeyedAccount,
}
