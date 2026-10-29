// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! What the `/debug/modules/mint_cache` endpoints of the API and the indexer read: each half's
//! summary and the `?pubkey=` entry. Kept apart from the cache logic; the endpoint handlers in the
//! `api` and `index` crates only serialize these.

use std::collections::HashMap;

use serde::Serialize;
use solana_pubkey::Pubkey;

use super::api::{Applied, MintCacheApi, Reload, Status as ApiStatus};
use super::indexer::MintCacheIndexer;
use super::store::{MintStore, MutableMint, StoredMint};

/// The indexer's last block: its mint changes by kind and its mint writes with no change.
#[derive(Debug, Clone, Serialize)]
pub(super) struct LastBlock {
    pub(super) slot: u64,
    pub(super) written: HashMap<&'static str, usize>,
    pub(super) skipped: usize,
}

/// The `?pubkey=` answer of the `/debug/modules/mint_cache` endpoints. `slot` is `null` for an
/// immutable entry, which stores none.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MintEntryDebug {
    pub pubkey: String,
    /// `immutable`, `mutable`, `tombstone` or `missing`.
    pub map: &'static str,
    pub slot: Option<u64>,
    pub closed: bool,
    pub decimals: Option<u8>,
    pub has_config: bool,
}

impl MintStore {
    fn debug_entry(&self, pubkey: &Pubkey) -> MintEntryDebug {
        let (map, slot, state) = match self.get(pubkey) {
            StoredMint::Immutable { decimals } => {
                return MintEntryDebug {
                    pubkey: pubkey.to_string(),
                    map: "immutable",
                    slot: None,
                    closed: false,
                    decimals: Some(decimals),
                    has_config: false,
                };
            }
            StoredMint::Tombstone { slot } => ("tombstone", Some(slot), None),
            StoredMint::Mutable(MutableMint { slot, state }) => {
                ("mutable", Some(*slot), Some(state))
            }
            StoredMint::Missing => ("missing", None, None),
        };
        MintEntryDebug {
            pubkey: pubkey.to_string(),
            map,
            slot,
            closed: map == "tombstone",
            decimals: state.map(|s| s.decimals),
            has_config: state.is_some_and(|s| s.configs.is_some()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MintCacheApiSummary {
    status: ApiStatus,
    applied_through: Applied,
    unhealthy_since: Option<u64>,
    /// The reload waiting for `last_db_slot` to reach its target.
    pending_reload: Option<Reload>,
    immutable: usize,
    mutable: usize,
    tombstones: usize,
    buffer_blocks: usize,
    buffer_min_slot: Option<u64>,
    buffer_max_slot: Option<u64>,
}

impl MintCacheApi {
    pub fn summary(&self) -> Option<MintCacheApiSummary> {
        let inner = self.0.as_ref()?;
        let (buffer_blocks, buffer_min_slot, buffer_max_slot) = {
            let buffer = inner.buffer();
            (
                buffer.values().map(Vec::len).sum(),
                buffer.keys().next().copied(),
                buffer.keys().next_back().copied(),
            )
        };
        let (unhealthy_since, pending_reload) = {
            let reloads = inner.reloads();
            (reloads.unhealthy_since, reloads.pending)
        };
        let state = inner.read();
        Some(MintCacheApiSummary {
            status: state.status,
            applied_through: state.applied.clone(),
            unhealthy_since,
            pending_reload,
            immutable: state.store.immutable_len(),
            mutable: state.store.mutable_len(),
            tombstones: state.store.tombstones(),
            buffer_blocks,
            buffer_min_slot,
            buffer_max_slot,
        })
    }

    pub fn entry(&self, pubkey: &Pubkey) -> Option<MintEntryDebug> {
        Some(self.0.as_ref()?.read().store.debug_entry(pubkey))
    }
}

#[derive(Debug, Serialize)]
pub struct MintCacheIndexerSummary {
    /// `null` until the bootstrap wrote `mints`.
    last_db_slot: Option<u64>,
    last_slot: u64,
    immutable: usize,
    mutable: usize,
    tombstones: usize,
    /// Closes waiting for their mint; `null` after the bootstrap.
    closed_pending: Option<usize>,
    pending_to_db: usize,
    db_write_running: bool,
    last_block: Option<LastBlock>,
}

impl MintCacheIndexer {
    pub fn summary(&self) -> Option<MintCacheIndexerSummary> {
        let state = self
            .0
            .as_ref()?
            .lock()
            .expect("mint cache indexer mutex poisoned");
        Some(MintCacheIndexerSummary {
            last_db_slot: state.last_db_slot,
            last_slot: state.last_slot,
            immutable: state.store.immutable_len(),
            mutable: state.store.mutable_len(),
            tombstones: state.store.tombstones(),
            closed_pending: state.closed_pending.as_ref().map(HashMap::len),
            pending_to_db: state.pending_to_db.len(),
            db_write_running: state.db_write_running,
            last_block: state.last_block.clone(),
        })
    }

    pub fn entry(&self, pubkey: &Pubkey) -> Option<MintEntryDebug> {
        let state = self
            .0
            .as_ref()?
            .lock()
            .expect("mint cache indexer mutex poisoned");
        Some(state.store.debug_entry(pubkey))
    }
}
