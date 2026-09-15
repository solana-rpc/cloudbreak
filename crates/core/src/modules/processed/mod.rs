// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Processed commitment for getAccountInfo, getMultipleAccounts, getBalance,
//! getTokenAccountBalance and getTokenSupply.
//!
//! Postgres holds confirmed data only. This module subscribes to Yellowstone
//! blocks with accounts at processed commitment and keeps the blocks around the
//! Postgres confirmed slot in memory. Each request takes one [`ProcessedView`]:
//! the highest stored block and its parents down through the confirmed slot,
//! linked by parent slot and parent blockhash. A key written in the chain is
//! answered from memory, and a miss reads Postgres at `slot <= anchor_slot`.
//! When no chain can be proven, [`ProcessedAccounts::view`] returns `None` and
//! the request takes the confirmed path. The module has no Postgres dependency.
//! The confirmed slot and its blockhash arrive as an [`Anchor`] on a watch
//! channel from the API slot syncronizer.
//!
//! # Layout
//!
//! - `mod.rs`: the public surface and the constants.
//! - `ingest.rs`: builds a `SlotBlock` and classifies each account as live or
//!   closed. A write by an owner outside the API program filter is a close.
//! - `store.rs`: the block store. One block per slot, slot statuses, the
//!   anchor, conflicts, retention, the slot cap and view selection.
//! - `read.rs`: [`ProcessedView::lookup`], the one read function the API calls.
//! - `subscribe.rs`: the feed thread. The shared gRPC client in `crate::grpc`
//!   drives the single writer, which owns the store without a lock.
//!
//! # Runtime model
//!
//! [`ProcessedAccounts::spawn`] starts the `processed-feed` thread. After every
//! block, slot status or anchor change the writer prunes, selects a view and
//! publishes it behind an `RwLock`. A request clones one `Arc` and holds no
//! lock while it reads. A block leaves memory when the last view that pins it
//! drops, on the feed thread after the publish lock or in a request.
//!
//! The feed subscribes without `from_slot`, keeps retrying on every failure
//! and never changes node health. While it is down, requests take the
//! confirmed path.
//!
//! # View selection
//!
//! The head is the highest stored slot. Its walk follows parent links, each
//! checked by blockhash, down to the confirmed slot, whose blockhash comes from
//! `recent_blockhashes`. The walk fails, and no view is served, when a link is
//! missing, a blockhash differs, the head has no `block_time`, or a slot above
//! the confirmed slot is poisoned: dead, restarted by a second
//! `SLOT_CREATED_BANK`, without a `SLOT_CREATED_BANK` in this session, or at or
//! below the highest slot seen before the last reconnect. A second blockhash
//! for a stored slot, or a parent blockhash that does not match the stored
//! parent, deletes the conflicting block and its descendants and serves nothing
//! until the confirmed slot passes that slot.
//!
//! # Retention
//!
//! Only the Postgres confirmed slot prunes. Blocks below it stay for
//! `RETAINED_SLOTS_BELOW_CONFIRMED` slots and serve hot keys from memory. The
//! Postgres bound stays at the confirmed slot, so a retained write is the
//! newest one whenever the chain is linked. Above the confirmed slot at most
//! `MAX_SLOTS_ABOVE_CONFIRMED` slots are kept. Past that the lowest goes, which
//! breaks the walk until Postgres catches up.
//!
//! # Enable rules
//!
//! The API `[processed-accounts]` section with `enabled = true` turns it on.
//! Otherwise [`ProcessedAccounts::default()`] is a no-op handle: `spawn` does
//! nothing and `view` returns `None`.
//!
//! # Node requirements
//!
//! - A Yellowstone endpoint that allows processed commitment and
//!   `interslot_updates`, plus its x-token.
//! - `[slot-syncronizer]` enabled, which publishes the [`Anchor`].
//! - No owner map, no program filter change, no indexer change, no
//!   `replay_stored_slots`. Each API instance carries a full block feed.
//!
//! # Upstream invariants
//!
//! Correctness leans on yellowstone-grpc plugin and Agave behaviour. Recheck
//! each one on every plugin major version bump.
//!
//! - At most one entry per pubkey per block, the one with the highest
//!   `write_version`. The plugin's `ProcessingSlot::seal()` enforces it. Ingest
//!   inserts each pubkey without dedup, so if it breaks, a repeated pubkey keeps
//!   an arbitrary version from that block.
//! - One block per slot per stream. The plugin's block assembly enforces it. A
//!   second version is a conflict.
//! - Account writes are sent before the block seals. The plugin's message
//!   ordering enforces it. If it breaks, a block misses writes and serves stale
//!   data.
//! - `SLOT_CREATED_BANK` is sent for every bank creation. The plugin's
//!   `update_slot_status` enforces it. If it breaks, a restarted slot goes
//!   undetected and a block that mixes two attempts can be served.
//! - `parent_blockhash` equals the parent block's `blockhash`. Agave bank
//!   construction enforces it. If it breaks, nothing links and no view is served.

mod ingest;
mod read;
mod store;
mod subscribe;

pub use read::{Lookup, ProcessedView};

use std::sync::{Arc, RwLock};
use std::time::Duration;

use solana_pubkey::Pubkey;
use yellowstone_grpc_client::GeyserGrpcClient;

use crate::config::{AccountSelectorConfig, ProcessedAccountsConfig};

/// Bound on connecting and on each request.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Reconnects when the stream is silent this long. Above the 10 s server ping.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
const MAX_DECODING_MESSAGE_SIZE: usize = 256 * 1024 * 1024;
/// Stored slots above the confirmed slot.
pub(crate) const MAX_SLOTS_ABOVE_CONFIRMED: usize = 32;
/// Slots kept below the confirmed slot.
pub(crate) const RETAINED_SLOTS_BELOW_CONFIRMED: u64 = 4;

/// The Postgres confirmed slot and its blockhash, published by the API slot syncronizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub confirmed_slot: u64,
    /// Base58, as in `recent_blockhashes.blockhash` and `SubscribeUpdateBlock.blockhash`.
    pub confirmed_blockhash: String,
}

/// One account version held in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAccount {
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Arc<Vec<u8>>,
}

/// An account write in one block.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AccountEntry {
    Live(LiveAccount),
    /// Zero lamports, or an owner outside the program filter. Shadows every older version.
    Closed,
}

struct Shared {
    config: ProcessedAccountsConfig,
    program_filter: Arc<AccountSelectorConfig>,
    published: RwLock<Option<Arc<ProcessedView>>>,
}

impl Shared {
    /// Swaps in the new view. The old one drops after the write lock is released.
    fn publish(&self, next: Option<Arc<ProcessedView>>) {
        let _previous = {
            let mut guard = self.published.write().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *guard, next)
        };
    }
}

/// Cheap-clone handle to the processed view. `None` is the disabled handle.
#[derive(Clone, Default)]
pub struct ProcessedAccounts(Option<Arc<Shared>>);

impl ProcessedAccounts {
    /// Returns the disabled handle when the section is absent or disabled.
    /// Otherwise checks the endpoint and x-token. Does not connect.
    pub fn from_config(
        config: Option<&ProcessedAccountsConfig>,
        program_filter: Arc<AccountSelectorConfig>,
    ) -> anyhow::Result<Self> {
        let Some(config) = config.filter(|c| c.enabled) else {
            return Ok(Self::default());
        };
        GeyserGrpcClient::build_from_shared(config.endpoint.clone())?
            .x_token(config.x_token.clone())?;
        Ok(Self(Some(Arc::new(Shared {
            config: config.clone(),
            program_filter,
            published: RwLock::new(None),
        }))))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Starts the `processed-feed` thread. No-op when disabled.
    pub fn spawn(&self, anchor_rx: tokio::sync::watch::Receiver<Option<Anchor>>) {
        if let Some(shared) = &self.0 {
            subscribe::spawn_feed(shared.clone(), anchor_rx);
        }
    }

    /// The view for one request, or `None` when no chain can be proven.
    pub fn view(&self) -> Option<Arc<ProcessedView>> {
        self.0
            .as_ref()?
            .published
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::store::tests::{TestChain, anchor_at};
    use super::*;

    fn config(endpoint: &str) -> ProcessedAccountsConfig {
        ProcessedAccountsConfig {
            enabled: true,
            endpoint: endpoint.to_string(),
            x_token: None,
        }
    }

    fn handle(config: &ProcessedAccountsConfig) -> anyhow::Result<ProcessedAccounts> {
        ProcessedAccounts::from_config(Some(config), Arc::new(AccountSelectorConfig::default()))
    }

    #[test]
    fn from_config_gates_on_enabled_and_checks_the_endpoint() {
        let disabled = ProcessedAccounts::default();
        assert!(!disabled.is_enabled());
        let (_tx, rx) = tokio::sync::watch::channel(None);
        disabled.spawn(rx);
        assert!(disabled.view().is_none());

        let mut off = config("");
        off.enabled = false;
        assert!(!handle(&off).unwrap().is_enabled());
        let none = ProcessedAccounts::from_config(None, Arc::new(AccountSelectorConfig::default()));
        assert!(!none.unwrap().is_enabled());

        assert!(handle(&config("not a uri")).is_err());

        let enabled = handle(&config("http://grpc:10000")).unwrap();
        assert!(enabled.is_enabled());
        assert!(enabled.view().is_none());
    }

    #[test]
    fn view_returns_the_published_view() {
        let handle = handle(&config("http://grpc:10000")).unwrap();
        let shared = handle.0.as_ref().unwrap();
        let mut chain = TestChain::new();
        chain.linear(101, 101);
        chain.store.set_anchor(anchor_at(100));
        shared.publish(chain.event().map(Arc::new));
        assert_eq!(handle.view().unwrap().slot, 101);
        shared.publish(None);
        assert!(handle.view().is_none());
    }
}
