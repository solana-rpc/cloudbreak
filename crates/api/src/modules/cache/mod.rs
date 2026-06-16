// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use bytes::Bytes;
use cloudbreak_core::GpaCacheConfig;
use sea_orm::sqlx::Row;
use sea_orm::sqlx::postgres::PgRow;
use solana_account_decoder::UiAccountEncoding;
use solana_account_decoder::UiDataSliceConfig;
use solana_account_decoder::parse_account_data::AccountAdditionalDataV3;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcProgramAccountsConfig;
use solana_rpc_client_api::filter::RpcFilterType;
use solana_rpc_client_api::response::RpcKeyedAccount;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::error::RpcError;
use crate::methods::program;
use crate::methods::program::GpaDbQueryInput;

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

    pub fn for_request(&self) -> Self {
        match self {
            Self::Standard => Self::Standard,
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
            Self::Cached { cached_query, .. } => match cached_query {
                Some(cached_query) => GpaCache::process_row(
                    row,
                    encoding,
                    data_slice,
                    response_bytes,
                    encode_span,
                    additional_mint_data,
                    cached_query,
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

    /// Commit the accumulated `(pubkey, bytes)` pairs as the new `CachedQuery`
    ///
    /// If the GpaProcessor is `Standard`, this is a no-op.
    ///
    /// It will only add the query to the cache if the query is larger than the
    /// `config.min_bytes_per_query`.
    ///
    /// If the insertion gets the cache size above the `config.max_total_bytes`,
    /// it will trigger the cache cleanup of oldest queries to ensure the cache
    /// size stays within the configured limit .
    pub fn finalize_query(&mut self) {
        let Self::Cached {
            cache,
            normalized_query,
            new_accounts_for_query,
            new_slot,
            cache_hits,
            cached_query: _,
        } = self
        else {
            return;
        };

        let Some(normalized_query) = normalized_query.take() else {
            tracing::error!(target: "gpa_cache", "No normalized query found");
            return;
        };

        let new_accounts_for_query = std::mem::take(
            &mut *new_accounts_for_query
                .lock()
                .expect("new_accounts_for_query mutex poisoned"),
        );

        let mut query_bytes = 0;
        let new_accounts_for_query_len = new_accounts_for_query.len();
        let accounts: HashMap<Pubkey, Bytes> = new_accounts_for_query
            .into_iter()
            .map(|(pubkey, bytes)| {
                query_bytes += bytes.len() as u64;
                (pubkey, bytes)
            })
            .collect();

        let new_entry = CachedQuery {
            accounts: Arc::new(accounts),
            slot: *new_slot,
            size: query_bytes,
            cache_hits: *cache_hits,
        };

        let mut cache_guard = cache.write().expect("can't lock gpa cache rwlock");

        // If query is smaller than the min_bytes_per_query, don't cache it
        if query_bytes < cache_guard.config.min_bytes_per_query as u64 {
            return;
        }

        // Cleanup cache if needed
        if let Some(bytes_freed) = cache_guard.cleanup_old_queries(query_bytes)
            && bytes_freed < query_bytes
        {
            tracing::error!(target: "gpa_cache", "Failed to cleanup old queries, not enough bytes freed {}", query_bytes - bytes_freed);
            return;
        }

        // Insert the query into the main map (replacing the older query if existed)
        let older_query = cache_guard
            .queries
            .insert(normalized_query.clone(), new_entry);

        // Update map size
        cache_guard.size += query_bytes;

        cache_guard.insert_query_for_slot(normalized_query.clone(), *new_slot, older_query);

        // Metrics
        tracing::info_span!(
            "gpa_cache_finalize_query",
            cache_hits = *cache_hits,
            query_bytes = query_bytes,
            query_accounts = new_accounts_for_query_len,
        );
    }
}

#[derive(Debug, Clone)]
pub struct CachedQuery {
    /// JSON-encoded account bytes keyed by pubkey. Stored as `Bytes` so that
    /// on a future cache hit we can append the slice directly into the next
    /// response's `BytesMut` (just a memcpy) with no re-serialization, and so
    /// that fresh slices coming out of the streaming pipeline share storage
    /// with the response chunks they were carved out of.
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
        filters.sort_by_cached_key(|f| match f {
            RpcFilterType::DataSize(n) => (0u8, *n, Vec::<u8>::new()),
            RpcFilterType::Memcmp(m) => (
                1u8,
                m.offset() as u64,
                m.bytes().map(|b| b.into_owned()).unwrap_or_default(),
            ),
            RpcFilterType::TokenAccountState => (2u8, 0, Vec::<u8>::new()),
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
        Self {
            queries: HashMap::new(),
            queries_for_slot: BTreeMap::new(),
            config,
            size: 0,
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
    pub fn insert_query_for_slot(
        &mut self,
        normalized_query: NormalizedQuery,
        slot: u64,
        older_query: Option<CachedQuery>,
    ) {
        // Remove old version of the query
        if let Some(prev) = older_query {
            let prev_slot = prev.slot;
            if let Some(queries_list) = self.queries_for_slot.get_mut(&prev_slot) {
                queries_list.retain(|q| q != &normalized_query);
                // If there is no more queries for the slot, remove the slot from the map
                if queries_list.is_empty() {
                    self.queries_for_slot.remove(&prev_slot);
                }
            }
        }

        // Insert the new query for the slot
        self.queries_for_slot
            .entry(slot)
            .or_default()
            .push(normalized_query);
    }

    /// it will delete the oldes queries until reach the `bytes_to_free` target.
    /// Returns the number of bytes freed.
    ///
    /// It will only cleanup if space is needed for the new query.
    ///
    /// Queries larger than `config.max_bytes_query_cleanup` (when set) are
    /// pinned: they are skipped during eviction and kept in the cache. Because
    /// of this, cleanup may free less than requested when the oldest slots hold
    /// mostly pinned queries.
    pub fn cleanup_old_queries(&mut self, mut bytes_to_free: u64) -> Option<u64> {
        let mut bytes_freed: u64 = 0;

        let available_bytes = match (self.config.max_total_bytes as u64).checked_sub(self.size) {
            Some(available_bytes) => available_bytes,
            None => {
                tracing::error!(target: "gpa_cache", "Cache size is greater than max total bytes");
                return None;
            }
        };
        if available_bytes >= bytes_to_free {
            return None;
        } else {
            bytes_to_free -= available_bytes;
        }

        let max_evictable = self.config.max_bytes_query_cleanup.map(|b| b as u64);

        // Walk slots oldest-first (`BTreeMap::retain` visits in ascending key
        // order), draining evictable queries from each bucket in place. Queries
        // larger than the threshold are pinned: left in their bucket and skipped.
        // A slot whose bucket becomes empty is dropped from the map.
        //
        // Borrow `queries`/`size` separately from `queries_for_slot` so the
        // closure can mutate them while iterating.
        let queries = &mut self.queries;
        let size = &mut self.size;
        self.queries_for_slot.retain(|_slot, bucket| {
            if bytes_freed >= bytes_to_free {
                return true; // enough freed: leave remaining slots untouched
            }
            bucket.retain(|q| {
                if bytes_freed >= bytes_to_free {
                    return true;
                }
                // Pin (keep) queries larger than the configured threshold.
                if let Some(max_evictable) = max_evictable
                    && queries.get(q).is_some_and(|c| c.size > max_evictable)
                {
                    return true;
                }
                if let Some(cached) = queries.remove(q) {
                    *size = size.saturating_sub(cached.size);
                    bytes_freed = bytes_freed.saturating_add(cached.size);
                }
                false
            });
            !bucket.is_empty()
        });

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
