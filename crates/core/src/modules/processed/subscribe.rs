// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The Yellowstone subscription and the single writer.
//!
//! One session subscribes to processed blocks with accounts and to slot
//! statuses with interslot updates. Connect and subscribe each run under
//! `connect-timeout`, so a server that accepts the connection but never answers
//! ends the attempt. Every block, `SLOT_CREATED_BANK`, `SLOT_DEAD`,
//! `SLOT_CONFIRMED` and anchor change is applied to the store, followed by
//! prune, view selection and publish. Anchor changes also apply while
//! connecting and backing off, so a slow feed does not age the anchor out. A
//! stream end, error or stall ends the session. The loop backs off from 100
//! ms, doubling up to `reconnect-backoff-max`, and resets after a session that
//! delivered a block. No feed error panics. The loop ends only when the anchor
//! sender is dropped.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::sync::watch;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterBlocks,
    SubscribeRequestFilterSlots, SubscribeUpdate, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

use super::ingest::SlotBlock;
use super::prune::Evicted;
use super::store::{BlockStore, StoreLimits};
use super::{Anchor, DegradeReason, Shared};
use crate::config::ProcessedAccountsConfig;
use crate::metrics;

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const KEEPALIVE: Duration = Duration::from_secs(10);

/// Starts `processed-feed`. Logs and returns when the thread cannot start.
pub(super) fn spawn_feed(
    shared: Arc<Shared>,
    anchor_rx: watch::Receiver<Option<Anchor>>,
    drop_tx: Option<mpsc::Sender<Evicted>>,
) {
    let spawned = std::thread::Builder::new()
        .name("processed-feed".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    tracing::error!("Failed to build processed-feed runtime: {e}");
                    return;
                }
            };
            runtime.block_on(Writer::new(shared, drop_tx).run(anchor_rx));
        });
    if let Err(e) = spawned {
        tracing::error!("Failed to start processed-feed thread: {e}");
    }
}

enum SessionEnd {
    Reconnect { delivered_block: bool },
    AnchorClosed,
}

struct Writer {
    shared: Arc<Shared>,
    store: BlockStore,
    drop_tx: Option<mpsc::Sender<Evicted>>,
}

impl Writer {
    fn new(shared: Arc<Shared>, drop_tx: Option<mpsc::Sender<Evicted>>) -> Self {
        let limits = StoreLimits {
            max_servable_depth: shared.config.max_servable_depth,
            max_overlay_slots: shared.config.max_overlay_slots,
            max_memory_bytes: shared.max_memory_bytes(),
        };
        let store = BlockStore::new(
            limits,
            shared.live_bytes.clone(),
            shared.pending_drop_bytes.clone(),
        );
        Self {
            shared,
            store,
            drop_tx,
        }
    }

    async fn run(mut self, mut anchor_rx: watch::Receiver<Option<Anchor>>) {
        let _guard = metrics::TokioTaskCounterGuard::new("processed_writer");
        if let Some(anchor) = anchor_rx.borrow_and_update().clone() {
            self.store.set_anchor(anchor);
        }
        let mut backoff = INITIAL_BACKOFF;
        let max_backoff = self
            .shared
            .config
            .reconnect_backoff_max
            .max(INITIAL_BACKOFF);

        loop {
            match self.session(&mut anchor_rx).await {
                SessionEnd::AnchorClosed => break,
                SessionEnd::Reconnect { delivered_block } => {
                    metrics::PROCESSED_GRPC_RECONNECTS_TOTAL.inc();
                    if delivered_block {
                        backoff = INITIAL_BACKOFF;
                    }
                }
            }
            let slept = self
                .with_anchors(tokio::time::sleep(backoff), &mut anchor_rx)
                .await;
            if slept.is_none() {
                break;
            }
            backoff = (backoff * 2).min(max_backoff);
        }

        tracing::warn!("processed anchor channel closed, processed feed stopped");
        self.shared.publish(Err(DegradeReason::AnchorStale));
    }

    /// Awaits `future` while applying anchor changes. `None` when the anchor channel closed.
    async fn with_anchors<F: Future>(
        &mut self,
        future: F,
        anchor_rx: &mut watch::Receiver<Option<Anchor>>,
    ) -> Option<F::Output> {
        tokio::pin!(future);
        loop {
            tokio::select! {
                output = &mut future => return Some(output),
                changed = anchor_rx.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    self.apply_anchor(anchor_rx);
                }
            }
        }
    }

    async fn session(&mut self, anchor_rx: &mut watch::Receiver<Option<Anchor>>) -> SessionEnd {
        let shared = self.shared.clone();
        let config = &shared.config;
        let timeout = config.connect_timeout;
        let subscribing = async {
            let mut client = tokio::time::timeout(timeout, connect(config))
                .await
                .map_err(|_| anyhow::anyhow!("connect timed out after {timeout:?}"))??;
            let (subscribe_tx, stream) =
                tokio::time::timeout(timeout, client.subscribe_with_request(Some(request())))
                    .await
                    .map_err(|_| anyhow::anyhow!("subscribe timed out after {timeout:?}"))??;
            anyhow::Ok((client, subscribe_tx, stream))
        };
        let (_client, _subscribe_tx, stream) = match self.with_anchors(subscribing, anchor_rx).await
        {
            None => return SessionEnd::AnchorClosed,
            Some(Ok(subscription)) => subscription,
            Some(Err(e)) => {
                tracing::error!("Failed to subscribe processed feed: {e:#}");
                return SessionEnd::Reconnect {
                    delivered_block: false,
                };
            }
        };
        tokio::pin!(stream);

        tracing::info!("processed feed subscribed");
        self.store.new_session();
        let stall_timeout = config.stall_timeout;
        let stall = tokio::time::sleep(stall_timeout);
        tokio::pin!(stall);
        let mut delivered_block = false;

        loop {
            tokio::select! {
                message = stream.next() => match message {
                    Some(Ok(update)) => {
                        stall.as_mut().reset(tokio::time::Instant::now() + stall_timeout);
                        delivered_block |= self.apply_update(update);
                    }
                    Some(Err(status)) => {
                        tracing::warn!("processed feed stream error: {status}");
                        return SessionEnd::Reconnect { delivered_block };
                    }
                    None => {
                        tracing::warn!("processed feed stream ended");
                        return SessionEnd::Reconnect { delivered_block };
                    }
                },
                changed = anchor_rx.changed() => {
                    if changed.is_err() {
                        return SessionEnd::AnchorClosed;
                    }
                    self.apply_anchor(anchor_rx);
                }
                () = &mut stall => {
                    tracing::warn!("processed feed stalled for {stall_timeout:?}");
                    return SessionEnd::Reconnect { delivered_block };
                }
            }
        }
    }

    fn apply_anchor(&mut self, anchor_rx: &mut watch::Receiver<Option<Anchor>>) {
        let anchor = anchor_rx.borrow_and_update().clone();
        if let Some(anchor) = anchor {
            self.store.set_anchor(anchor);
            self.after_event();
        }
    }

    /// Applies one update. True when it was a block.
    fn apply_update(&mut self, update: SubscribeUpdate) -> bool {
        match update.update_oneof {
            Some(UpdateOneof::Block(block)) => {
                let received_at = Instant::now();
                let block = SlotBlock::from_update(
                    block,
                    &self.shared.program_filter,
                    &self.shared.live_bytes,
                    received_at,
                );
                metrics::PROCESSED_BLOCK_BYTES.observe(block.bytes as f64);
                self.store.on_block(block);
                self.after_event();
                metrics::PROCESSED_BLOCK_INGEST_MS
                    .observe(received_at.elapsed().as_secs_f64() * 1_000.0);
                true
            }
            Some(UpdateOneof::Slot(slot)) => {
                match SlotStatus::try_from(slot.status) {
                    Ok(SlotStatus::SlotCreatedBank) => {
                        self.store.on_created_bank(slot.slot, slot.parent)
                    }
                    Ok(SlotStatus::SlotDead) => self.store.on_dead(slot.slot),
                    Ok(SlotStatus::SlotConfirmed) => self.store.on_confirmed(slot.slot),
                    _ => return false,
                }
                self.after_event();
                false
            }
            _ => false,
        }
    }

    fn after_event(&mut self) {
        let links = self.store.prune();
        let result = self.store.select_view_with(&links).map(Arc::new);
        self.store.observe_publish(&result);
        self.shared.publish(result);
        for evicted in self.store.take_evicted() {
            let send_failed = match &self.drop_tx {
                Some(tx) => tx.send(evicted).is_err(),
                None => false,
            };
            if send_failed {
                tracing::warn!("processed-dropper is gone, evicted blocks drop on the feed thread");
                self.drop_tx = None;
            }
        }
    }
}

async fn connect(
    config: &ProcessedAccountsConfig,
) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
    Ok(
        GeyserGrpcClient::build_from_shared(config.endpoint.clone())?
            .x_token(config.x_token.clone())?
            .max_decoding_message_size(config.max_decoding_mb.saturating_mul(1024 * 1024))
            .accept_compressed(CompressionEncoding::Zstd)
            .connect_timeout(config.connect_timeout)
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .tcp_keepalive(Some(KEEPALIVE))
            .http2_keep_alive_interval(KEEPALIVE)
            .keep_alive_timeout(KEEPALIVE)
            .connect()
            .await?,
    )
}

fn request() -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::from([(
            "processed_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )]),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::from([(
            "processed_blocks".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(false),
                include_accounts: Some(true),
                include_entries: Some(false),
            },
        )]),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::ProcessedAccounts;
    use super::super::store::tests::anchor_at;
    use super::*;
    use crate::config::AccountSelectorConfig;

    #[test]
    fn request_subscribes_processed_blocks_and_interslot_slots() {
        let request = request();
        assert_eq!(request.commitment, Some(CommitmentLevel::Processed as i32));
        assert_eq!(request.from_slot, None);
        let blocks = &request.blocks["processed_blocks"];
        assert_eq!(blocks.include_accounts, Some(true));
        assert_eq!(blocks.include_transactions, Some(false));
        assert_eq!(blocks.include_entries, Some(false));
        assert!(blocks.account_include.is_empty());
        let slots = &request.slots["processed_slots"];
        assert_eq!(slots.interslot_updates, Some(true));
        assert_eq!(slots.filter_by_commitment, Some(false));
        assert!(request.accounts.is_empty() && request.transactions.is_empty());
    }

    #[tokio::test]
    async fn silent_server_times_out_while_anchors_keep_applying() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });

        let config: ProcessedAccountsConfig = toml::from_str(&format!(
            "enabled = true\nendpoint = \"http://127.0.0.1:{port}\"\nconnect-timeout = \"300ms\"\n"
        ))
        .unwrap();
        let handle = ProcessedAccounts::from_config(
            Some(&config),
            Arc::new(AccountSelectorConfig::default()),
        )
        .unwrap();
        let shared = handle.0.clone().unwrap();
        let mut writer = Writer::new(shared, None);

        let (anchor_tx, mut anchor_rx) = watch::channel(None);
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            anchor_tx.send(Some(anchor_at(100))).unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let end = tokio::time::timeout(Duration::from_secs(5), writer.session(&mut anchor_rx))
            .await
            .expect("session must end on the connect timeout");
        assert!(matches!(
            end,
            SessionEnd::Reconnect {
                delivered_block: false
            }
        ));
        assert_eq!(writer.store.anchor_slot(), Some(100));

        sender.abort();
        server.abort();
    }
}
