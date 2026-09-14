// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Processed commitment for account point reads: getAccountInfo,
//! getMultipleAccounts, getBalance and getTokenAccountBalance.
//!
//! Postgres holds confirmed data only. This module subscribes to Yellowstone
//! blocks with accounts at processed commitment and keeps the few blocks above
//! the Postgres confirmed slot, the anchor, in memory. Each request takes one
//! [`ProcessedView`]: a head block whose chain links by parent slot and parent
//! blockhash down to the anchor. A key written in the chain is answered from
//! memory, and a miss reads Postgres at `slot <= anchor_slot`. When no chain can
//! be proven, [`ProcessedAccounts::view`] returns a [`DegradeReason`] and the
//! caller serves the confirmed path. The module has no Postgres dependency. The
//! anchor arrives on a watch channel from the API slot syncronizer.
//!
//! # Layout
//!
//! - `mod.rs`: the public surface and metric registration.
//! - `ingest.rs`: builds a `SlotBlock` and classifies each account as live,
//!   closed or excluded by the API program filter.
//! - `store.rs`: the block store. Slot statuses, the anchor, the conflict latch,
//!   chain links and view selection. Pure and synchronous.
//! - `prune.rs`: retention, the span and memory caps, and the dropper thread.
//! - `read.rs`: [`ProcessedView::lookup`], the one read function the API calls.
//! - `subscribe.rs`: the gRPC session, the reconnect loop and the single writer.
//!
//! Nothing is persisted, so there is no persist file.
//!
//! # Runtime model
//!
//! [`ProcessedAccounts::spawn`] starts two OS threads. `processed-feed` runs the
//! subscribe loop and the single writer, which owns the store without a lock.
//! After every block, slot status or anchor change the writer prunes, selects a
//! view and publishes it behind an `RwLock`. A request clones one `Arc` and holds
//! no lock while it reads. `processed-dropper` frees evicted blocks off the
//! writer, and a block a request still pins is freed when the request drops it.
//!
//! Each `SlotBlock` adds its bytes to a shared counter when built and subtracts
//! them when dropped, so the memory cap covers blocks that requests pin. Bytes of
//! evicted blocks queued for the dropper do not count toward the cap.
//!
//! View selection fails closed. A head is served only when its chain links to
//! the anchor blockhash, it descends from the stream's confirmed block when that
//! block is unambiguous, and no slot on the chain is poisoned. A slot is poisoned
//! when it is dead, restarted by a second `SLOT_CREATED_BANK`, has no
//! `SLOT_CREATED_BANK` in this session, or is at or below the highest slot seen
//! before the last reconnect. A parent blockhash that matches no stored parent,
//! or two blockhashes for one slot, sets a conflict latch on the highest slot
//! involved. Every view degrades until the anchor reaches that slot.
//!
//! # Enable rules
//!
//! The API `[processed-accounts]` section with `enabled = true` turns it on.
//! Otherwise [`ProcessedAccounts::default()`] is a no-op handle: `spawn` does
//! nothing and `view` returns [`DegradeReason::Disabled`].
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
//!   second version, from a reconnect or another node, sets the conflict latch.
//! - Account writes are sent before the block seals. The plugin's message
//!   ordering enforces it. If it breaks, a block misses writes and serves stale
//!   data.
//! - `SLOT_CREATED_BANK` is sent for every bank creation. The plugin's
//!   `update_slot_status` enforces it. If it breaks, a restarted slot goes
//!   undetected and a block that mixes two attempts can be served.
//! - `parent_blockhash` equals the parent block's `blockhash`. Agave bank
//!   construction enforces it. If it breaks, nothing links and every view
//!   degrades.

mod ingest;
mod prune;
mod read;
mod store;
mod subscribe;

pub use read::ProcessedView;

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Instant;

use prometheus::{Registry, core::Collector};
use solana_pubkey::Pubkey;

use crate::config::{AccountSelectorConfig, ProcessedAccountsConfig};
use crate::metrics;

/// The Postgres confirmed state the overlay is anchored to, published by the
/// API slot syncronizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub confirmed_slot: u64,
    /// Base58, as in `recent_blockhashes.blockhash` and `SubscribeUpdateBlock.blockhash`.
    pub confirmed_blockhash: String,
    pub finalized_slot: u64,
    pub healthy: bool,
    /// Time of the last successful poll.
    pub polled_at: Instant,
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
    /// Zero lamports. Shadows every older version.
    Closed,
    /// Owner not selected by the API program filter.
    Excluded {
        owner: Pubkey,
    },
}

/// The newest state of a key along a view's chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup<'a> {
    Live(&'a LiveAccount),
    Closed,
    Excluded(Pubkey),
    /// Not written in the chain. Read Postgres at `slot <= anchor_slot`.
    Miss,
}

/// Why no processed view can be served. Every reason routes to the confirmed path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradeReason {
    Disabled,
    NotWarm,
    Unhealthy,
    AnchorStale,
    FinalizedAboveAnchor,
    Conflict,
    Unlinked,
    TooDeep,
    MemoryCap,
    HeadNotAhead,
}

impl DegradeReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NotWarm => "not_warm",
            Self::Unhealthy => "unhealthy",
            Self::AnchorStale => "anchor_stale",
            Self::FinalizedAboveAnchor => "finalized_above_anchor",
            Self::Conflict => "conflict",
            Self::Unlinked => "unlinked",
            Self::TooDeep => "too_deep",
            Self::MemoryCap => "memory_cap",
            Self::HeadNotAhead => "head_not_ahead",
        }
    }
}

type Published = Result<Arc<ProcessedView>, DegradeReason>;

struct Shared {
    config: ProcessedAccountsConfig,
    program_filter: Arc<AccountSelectorConfig>,
    live_bytes: Arc<AtomicUsize>,
    /// Bytes of evicted blocks waiting for the dropper.
    pending_drop_bytes: Arc<AtomicUsize>,
    published: RwLock<Published>,
    spawned: AtomicBool,
}

impl Shared {
    fn max_memory_bytes(&self) -> usize {
        self.config.max_memory_mb.saturating_mul(1024 * 1024)
    }

    /// Swaps in the new result. The old one drops after the write lock is released.
    fn publish(&self, next: Published) {
        let previous = {
            let mut guard = self.published.write().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *guard, next)
        };
        drop(previous);
    }
}

/// Cheap-clone handle to the processed overlay. `None` is the disabled handle.
#[derive(Clone, Default)]
pub struct ProcessedAccounts(Option<Arc<Shared>>);

impl ProcessedAccounts {
    /// Returns the disabled handle when the section is absent or disabled, and
    /// validates the section otherwise. Does not connect.
    pub fn from_config(
        config: Option<&ProcessedAccountsConfig>,
        program_filter: Arc<AccountSelectorConfig>,
    ) -> anyhow::Result<Self> {
        let Some(config) = config.filter(|c| c.enabled) else {
            return Ok(Self::default());
        };
        config.validate()?;
        Ok(Self(Some(Arc::new(Shared {
            config: config.clone(),
            program_filter,
            live_bytes: Arc::new(AtomicUsize::new(0)),
            pending_drop_bytes: Arc::new(AtomicUsize::new(0)),
            published: RwLock::new(Err(DegradeReason::NotWarm)),
            spawned: AtomicBool::new(false),
        }))))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Starts the `processed-feed` and `processed-dropper` threads. No-op when
    /// disabled or already spawned.
    pub fn spawn(&self, anchor_rx: tokio::sync::watch::Receiver<Option<Anchor>>) {
        let Some(shared) = &self.0 else {
            return;
        };
        if shared.spawned.swap(true, Ordering::SeqCst) {
            tracing::warn!("processed accounts feed already spawned");
            return;
        }
        let drop_tx = prune::spawn_dropper();
        subscribe::spawn_feed(shared.clone(), anchor_rx, drop_tx);
    }

    /// The view for one request, or why none can be served.
    pub fn view(&self) -> Result<Arc<ProcessedView>, DegradeReason> {
        let shared = self.0.as_ref().ok_or(DegradeReason::Disabled)?;
        let published = shared
            .published
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let view = published?;
        if view.anchor_polled_at.elapsed() > shared.config.anchor_max_age {
            return Err(DegradeReason::AnchorStale);
        }
        let held = shared
            .live_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(shared.pending_drop_bytes.load(Ordering::Relaxed));
        if held > shared.max_memory_bytes() {
            return Err(DegradeReason::MemoryCap);
        }
        Ok(view)
    }
}

/// Registers the processed metrics and `CURRENT_TOKIO_TASKS`. Collectors
/// already registered are skipped.
pub fn register_processed_metrics(registry: &Registry) {
    let collectors: Vec<Box<dyn Collector>> = vec![
        Box::new(metrics::CURRENT_TOKIO_TASKS.clone()),
        Box::new(metrics::PROCESSED_CONFIRM_LATENCY_MS.clone()),
        Box::new(metrics::PROCESSED_DEPTH_SLOTS.clone()),
        Box::new(metrics::PROCESSED_HEAD_REGRESSIONS_TOTAL.clone()),
        Box::new(metrics::PROCESSED_LIVE_BYTES.clone()),
        Box::new(metrics::PROCESSED_STORE_BLOCKS.clone()),
        Box::new(metrics::PROCESSED_HEAD_SLOT.clone()),
        Box::new(metrics::PROCESSED_ANCHOR_SLOT.clone()),
        Box::new(metrics::PROCESSED_HEAD_AGE_MS.clone()),
        Box::new(metrics::PROCESSED_BLOCK_INGEST_MS.clone()),
        Box::new(metrics::PROCESSED_BLOCK_BYTES.clone()),
        Box::new(metrics::PROCESSED_GRPC_RECONNECTS_TOTAL.clone()),
        Box::new(metrics::PROCESSED_CONFLICTS_TOTAL.clone()),
        Box::new(metrics::PROCESSED_DEAD_SLOTS_TOTAL.clone()),
        Box::new(metrics::PROCESSED_RESTARTED_SLOTS_TOTAL.clone()),
        Box::new(metrics::PROCESSED_EVICTIONS_TOTAL.clone()),
    ];
    for collector in collectors {
        match registry.register(collector) {
            Ok(()) | Err(prometheus::Error::AlreadyReg) => {}
            Err(e) => tracing::error!("Failed to register processed metric: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::store::tests::{TestChain, anchor_at};
    use super::*;
    use std::time::Duration;

    fn config(extra: &str) -> ProcessedAccountsConfig {
        toml::from_str(&format!(
            "enabled = true\nendpoint = \"http://grpc:10000\"\nmax-memory-mb = 64\nmax-decoding-mb = 64\n{extra}"
        ))
        .unwrap()
    }

    fn handle(config: &ProcessedAccountsConfig) -> anyhow::Result<ProcessedAccounts> {
        ProcessedAccounts::from_config(Some(config), Arc::new(AccountSelectorConfig::default()))
    }

    /// Publishes a view with head 101 over anchor 100, its bytes on the handle's counter.
    fn publish_view(handle: &ProcessedAccounts) {
        let shared = handle.0.as_ref().unwrap();
        let mut chain = TestChain::with_live_bytes(shared.live_bytes.clone());
        chain.linear(101, 101);
        chain.store.set_anchor(anchor_at(100));
        shared.publish(Ok(Arc::new(chain.store.select_view().unwrap())));
    }

    #[test]
    fn from_config_gates_on_enabled_and_validates() {
        let disabled = ProcessedAccounts::default();
        assert!(!disabled.is_enabled());
        let (_tx, rx) = tokio::sync::watch::channel(None);
        disabled.spawn(rx);
        assert_eq!(disabled.view().unwrap_err(), DegradeReason::Disabled);

        let mut off = config("");
        off.enabled = false;
        off.endpoint.clear();
        assert!(!handle(&off).unwrap().is_enabled());
        let none = ProcessedAccounts::from_config(None, Arc::new(AccountSelectorConfig::default()));
        assert!(!none.unwrap().is_enabled());

        let mut bad = config("");
        bad.endpoint.clear();
        assert!(handle(&bad).is_err());

        let enabled = handle(&config("")).unwrap();
        assert!(enabled.is_enabled());
        assert_eq!(enabled.view().unwrap_err(), DegradeReason::NotWarm);
    }

    #[test]
    fn view_degrades_on_stale_anchor() {
        let handle = handle(&config("anchor-max-age = \"0s\"\n")).unwrap();
        publish_view(&handle);
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(handle.view().unwrap_err(), DegradeReason::AnchorStale);
    }

    #[test]
    fn view_memory_cap_counts_pinned_but_not_pending_bytes() {
        let handle = handle(&config("")).unwrap();
        publish_view(&handle);
        let shared = handle.0.as_ref().unwrap();
        assert_eq!(handle.view().unwrap().slot, 101);

        // Bytes pinned elsewhere count toward the cap.
        let excess = shared.max_memory_bytes() + 1;
        shared.live_bytes.fetch_add(excess, Ordering::Relaxed);
        assert_eq!(handle.view().unwrap_err(), DegradeReason::MemoryCap);

        // The same bytes queued for the dropper do not.
        shared
            .pending_drop_bytes
            .fetch_add(excess, Ordering::Relaxed);
        assert_eq!(handle.view().unwrap().slot, 101);
        shared.live_bytes.fetch_sub(excess, Ordering::Relaxed);
        shared
            .pending_drop_bytes
            .fetch_sub(excess, Ordering::Relaxed);
    }

    #[test]
    fn register_metrics_is_idempotent() {
        let registry = Registry::new();
        register_processed_metrics(&registry);
        register_processed_metrics(&registry);
        assert!(
            registry
                .gather()
                .iter()
                .any(|family| family.name() == "cloudbreak_api_processed_live_bytes")
        );
    }
}
