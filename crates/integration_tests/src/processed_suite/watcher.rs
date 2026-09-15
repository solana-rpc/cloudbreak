// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Yellowstone processed subscription that feeds the block tree and the event channel.
//!
//! The subscription asks for blocks with accounts, a zero-length data slice so no account data
//! crosses the wire, and every slot status. It reconnects with backoff. A second
//! `SLOT_CREATED_BANK` for one slot within a session marks the slot restarted, which the tree
//! treats like a dead slot and which publishes a `Dead` event. With a reference set, a second task
//! checks the tree's canonical answers against `getBlocks` once confirmed passes.

use super::MAX_EXAMPLES;
use super::sources::Source;
use super::tree::{Block, BlockTree, Event, Touch};
use anyhow::{Result, anyhow};
use futures::StreamExt;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestAccountsDataSlice,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots, SubscribeUpdateBlock,
    subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

const KEEPALIVE: Duration = Duration::from_secs(10);
const STALL: Duration = Duration::from_secs(60);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const CHECK_EVERY: Duration = Duration::from_secs(20);
/// Slots below the watcher's confirmed slot left out of a getBlocks check.
const CHECK_MARGIN: u64 = 32;

pub struct WatcherConfig {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub window: Duration,
    pub max_slots: u64,
}

#[derive(Default)]
pub struct WatcherStats {
    pub blocks: AtomicU64,
    pub forks: AtomicU64,
    pub dead: AtomicU64,
    pub restarted: AtomicU64,
    pub confirmed: AtomicU64,
    pub reconnects: AtomicU64,
    pub tip: AtomicU64,
    pub checked_slots: AtomicU64,
    pub disagreements: AtomicU64,
    pub examples: Mutex<Vec<String>>,
}

#[derive(Clone)]
pub struct Watcher {
    tree: Arc<RwLock<BlockTree>>,
    events: broadcast::Sender<Event>,
    pub stats: Arc<WatcherStats>,
}

impl Watcher {
    /// Starts the feed, and the getBlocks check when an oracle is given.
    pub fn spawn(
        config: WatcherConfig,
        oracle: Option<(Source, reqwest::Client)>,
    ) -> (Self, Vec<JoinHandle<()>>) {
        let watcher = Self {
            tree: Arc::default(),
            events: broadcast::channel(8192).0,
            stats: Arc::default(),
        };
        let mut tasks = vec![tokio::spawn(feed(watcher.clone(), config))];
        if let Some((source, client)) = oracle {
            tasks.push(tokio::spawn(check_loop(watcher.clone(), source, client)));
        }
        (watcher, tasks)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Highest block slot seen, the processed tip.
    pub fn tip(&self) -> u64 {
        self.stats.tip.load(Ordering::Relaxed)
    }

    /// Runs `f` under the read lock. Keep `f` short, the feed waits on it.
    pub fn read<T>(&self, f: impl FnOnce(&BlockTree) -> T) -> T {
        f(&self.tree.read().expect("block tree lock"))
    }

    fn write<T>(&self, f: impl FnOnce(&mut BlockTree) -> T) -> T {
        f(&mut self.tree.write().expect("block tree lock"))
    }

    fn send(&self, event: Event, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
        let _ = self.events.send(event);
    }

    fn ingest_block(&self, update: SubscribeUpdateBlock, config: &WatcherConfig) {
        let received_at = Instant::now();
        let mut accounts: Vec<(Pubkey, u64, Pubkey, u64)> = update
            .accounts
            .iter()
            .filter_map(|a| {
                let key = Pubkey::try_from(a.pubkey.as_slice()).ok()?;
                let owner = Pubkey::try_from(a.owner.as_slice()).ok()?;
                Some((key, a.lamports, owner, a.write_version))
            })
            .collect();
        // One entry per key, the highest write_version.
        accounts.sort_by(|a, b| a.0.cmp(&b.0).then(b.3.cmp(&a.3)));
        accounts.dedup_by_key(|a| a.0);
        let events = self.write(|tree| {
            let touched: Arc<[Touch]> = accounts
                .into_iter()
                .map(|(key, lamports, owner, _)| Touch {
                    key,
                    lamports,
                    owner: tree.intern(owner),
                })
                .collect();
            let events = tree.insert(Block {
                slot: update.slot,
                blockhash: update.blockhash,
                parent_slot: update.parent_slot,
                parent_blockhash: update.parent_blockhash,
                received_at,
                touched,
            });
            tree.prune(received_at, config.window, config.max_slots);
            self.stats.tip.store(tree.tip(), Ordering::Relaxed);
            events
        });
        for event in events {
            let counter = match &event {
                Event::Fork { .. } => &self.stats.forks,
                _ => &self.stats.blocks,
            };
            self.send(event, counter);
        }
    }

    fn ingest_slot(&self, slot: u64, status: i32, config: &WatcherConfig) {
        let stats = &self.stats;
        let event = self.write(|tree| {
            let event = match SlotStatus::try_from(status) {
                Ok(SlotStatus::SlotConfirmed) => tree
                    .confirm(slot)
                    .then_some((Event::Confirmed { slot }, &stats.confirmed)),
                Ok(SlotStatus::SlotDead) => tree
                    .mark_dead(slot)
                    .then_some((Event::Dead { slot }, &stats.dead)),
                Ok(SlotStatus::SlotCreatedBank) => tree
                    .on_created_bank(slot)
                    .then_some((Event::Dead { slot }, &stats.restarted)),
                _ => None,
            };
            tree.prune(Instant::now(), config.window, config.max_slots);
            event
        });
        if let Some((event, counter)) = event {
            self.send(event, counter);
        }
    }
}

fn request() -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            "processed_suite_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )]),
        blocks: HashMap::from([(
            "processed_suite_blocks".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(false),
                include_accounts: Some(true),
                include_entries: Some(false),
            },
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![SubscribeRequestAccountsDataSlice {
            offset: 0,
            length: 0,
        }],
        ..SubscribeRequest::default()
    }
}

async fn feed(watcher: Watcher, config: WatcherConfig) {
    let mut backoff = Duration::from_secs(1);
    let mut first = true;
    loop {
        if !first {
            watcher.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        first = false;
        match stream_once(&watcher, &config).await {
            Ok(true) => backoff = Duration::from_secs(1),
            Ok(false) => eprintln!("watcher: stream ended before any block"),
            Err(e) => eprintln!("watcher: stream failed: {e}"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Runs one subscription. Ok(true) when it delivered at least one block.
async fn stream_once(watcher: &Watcher, config: &WatcherConfig) -> Result<bool> {
    let mut client = GeyserGrpcClient::build_from_shared(config.endpoint.clone())?
        .x_token(config.x_token.clone())?
        .max_decoding_message_size(256 * 1024 * 1024)
        .accept_compressed(CompressionEncoding::Zstd)
        .connect_timeout(KEEPALIVE)
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .tcp_keepalive(Some(KEEPALIVE))
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_timeout(KEEPALIVE)
        .connect()
        .await?;
    let (_sink, stream) = client.subscribe_with_request(Some(request())).await?;
    watcher.write(BlockTree::new_session);
    let mut stream = std::pin::pin!(stream);
    let mut delivered = false;
    loop {
        let next = tokio::time::timeout(STALL, stream.next()).await;
        let update = match next.map_err(|_| anyhow!("no update for {STALL:?}"))? {
            Some(update) => update?,
            None if delivered => return Ok(true),
            None => return Ok(false),
        };
        match update.update_oneof {
            Some(UpdateOneof::Block(block)) => {
                delivered = true;
                watcher.ingest_block(block, config);
            }
            Some(UpdateOneof::Slot(slot)) => watcher.ingest_slot(slot.slot, slot.status, config),
            _ => {}
        }
    }
}

async fn check_loop(watcher: Watcher, oracle: Source, client: reqwest::Client) {
    let mut next_lo = 0;
    loop {
        tokio::time::sleep(CHECK_EVERY).await;
        let (top, low) = watcher.read(|t| (t.highest_confirmed(), t.lowest_slot()));
        let (Some(top), Some(low)) = (top, low) else {
            continue;
        };
        let (lo, hi) = (next_lo.max(low), top.saturating_sub(CHECK_MARGIN));
        if lo > hi
            || oracle
                .slot_at(&client, "confirmed")
                .await
                .is_none_or(|s| s < hi)
        {
            continue;
        }
        let Some(slots) = oracle.blocks(&client, lo, hi).await else {
            continue;
        };
        let canonical: HashSet<u64> = slots.into_iter().collect();
        let (checked, bad) = watcher.read(|t| cross_check(t, lo, hi, &canonical));
        let stats = &watcher.stats;
        stats.checked_slots.fetch_add(checked, Ordering::Relaxed);
        stats
            .disagreements
            .fetch_add(bad.len() as u64, Ordering::Relaxed);
        let mut examples = stats.examples.lock().expect("examples lock");
        let room = MAX_EXAMPLES.saturating_sub(examples.len());
        examples.extend(bad.into_iter().take(room));
        next_lo = hi + 1;
    }
}

/// Stored slots in `[lo, hi]` whose canonical answer is known, and those that disagree with the
/// reference's confirmed block list.
pub fn cross_check(
    tree: &BlockTree,
    lo: u64,
    hi: u64,
    canonical: &HashSet<u64>,
) -> (u64, Vec<String>) {
    let mut checked = 0;
    let mut bad = Vec::new();
    for slot in tree.slots_in(lo, hi) {
        let Some(ours) = tree.canonical(slot) else {
            continue;
        };
        checked += 1;
        if ours != canonical.contains(&slot) {
            bad.push(format!(
                "slot {slot}: watcher canonical {ours}, getBlocks {}",
                !ours
            ));
        }
    }
    (checked, bad)
}
