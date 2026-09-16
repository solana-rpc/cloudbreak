// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The feed thread and the single writer.
//!
//! The writer runs the shared gRPC client on its own OS thread with a
//! current-thread runtime. One session subscribes to processed blocks with
//! accounts and to slot statuses with interslot updates. Every block,
//! `SLOT_CREATED_BANK`, `SLOT_DEAD` and anchor change is applied to the store,
//! followed by a prune, a selection of the latest chained blocks and a
//! publish. A stream end,
//! error or stall ends the session, and the client reconnects with no
//! `from_slot`. The client never gives up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use tokio::sync::watch;
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::Status;

use super::ingest::SlotBlock;
use super::store::BlockStore;
use super::{
    Anchor, CONNECT_TIMEOUT, MAX_DECODING_MESSAGE_SIZE, RECONNECT_BACKOFF, STALL_TIMEOUT, Shared,
};
use crate::grpc::{
    GrpcClientOptions, SessionEnd, Subscriber, blocks_with_accounts_request,
    subscribe_with_reconnection,
};

/// Starts `processed-feed`. Logs and returns when the thread cannot start.
pub(super) fn spawn_feed(shared: Arc<Shared>, anchor_rx: watch::Receiver<Option<Anchor>>) {
    let options = GrpcClientOptions {
        endpoint: shared.config.endpoint.clone(),
        x_token: shared.config.x_token.clone(),
        timeout: CONNECT_TIMEOUT,
        max_decoding_message_size: MAX_DECODING_MESSAGE_SIZE,
        reconnect_backoff: RECONNECT_BACKOFF,
        reconnect_give_up: None,
        reconnect_from_slot_retain: Duration::ZERO,
    };
    let writer = FeedWriter {
        shared,
        store: BlockStore::new(),
        anchor_rx,
    };
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
            runtime.block_on(subscribe_with_reconnection(options, writer));
        });
    if let Err(e) = spawned {
        tracing::error!("Failed to start processed-feed thread: {e}");
    }
}

struct FeedWriter {
    shared: Arc<Shared>,
    store: BlockStore,
    anchor_rx: watch::Receiver<Option<Anchor>>,
}

impl Subscriber for FeedWriter {
    fn request(&self, _replay: bool) -> SubscribeRequest {
        blocks_with_accounts_request(CommitmentLevel::Processed, true, None)
    }

    fn cancelled(&self) -> bool {
        false
    }

    fn on_connect_failed(&mut self) {}

    async fn on_connect(&mut self, _client: &mut GeyserGrpcClient<impl Interceptor + Send>) {}

    async fn session(
        &mut self,
        stream: impl Stream<Item = Result<SubscribeUpdate, Status>> + Send,
    ) -> SessionEnd {
        tracing::info!("processed feed subscribed");
        self.store.new_session();
        self.apply_anchor();
        let mut stream = std::pin::pin!(stream);
        let stall = tokio::time::sleep(STALL_TIMEOUT);
        tokio::pin!(stall);
        let mut received_block = false;
        loop {
            tokio::select! {
                message = stream.next() => match message {
                    Some(Ok(update)) => {
                        stall.as_mut().reset(tokio::time::Instant::now() + STALL_TIMEOUT);
                        received_block |= self.apply_update(update);
                    }
                    Some(Err(status)) => {
                        tracing::warn!("processed feed stream error: {status}");
                        break if received_block {
                            SessionEnd::Healthy
                        } else {
                            SessionEnd::Failed
                        };
                    }
                    None => {
                        tracing::warn!("processed feed stream ended");
                        break SessionEnd::Healthy;
                    }
                },
                changed = self.anchor_rx.changed() => {
                    if changed.is_ok() {
                        self.apply_anchor();
                    }
                }
                () = &mut stall => {
                    tracing::warn!("processed feed stalled for {STALL_TIMEOUT:?}");
                    break SessionEnd::Healthy;
                }
            }
        }
    }
}

impl FeedWriter {
    /// Applies one update. True when it was a block.
    fn apply_update(&mut self, update: SubscribeUpdate) -> bool {
        match update.update_oneof {
            Some(UpdateOneof::Block(block)) => {
                let received_at = Instant::now();
                let block = SlotBlock::from_update(block, &self.shared.program_filter, received_at);
                self.store.on_block(block);
                self.publish_latest();
                true
            }
            Some(UpdateOneof::Slot(slot)) => {
                match SlotStatus::try_from(slot.status) {
                    Ok(SlotStatus::SlotCreatedBank) => {
                        self.store.on_created_bank(slot.slot, slot.parent)
                    }
                    Ok(SlotStatus::SlotDead) => self.store.on_dead(slot.slot),
                    _ => return false,
                }
                self.publish_latest();
                false
            }
            _ => false,
        }
    }

    /// Applies the current anchor when there is one.
    fn apply_anchor(&mut self) {
        let anchor = self.anchor_rx.borrow_and_update().clone();
        if let Some(anchor) = anchor {
            self.store.set_anchor(anchor);
            self.publish_latest();
        }
    }

    /// Prunes, selects and publishes.
    fn publish_latest(&mut self) {
        self.store.prune();
        self.shared
            .set_latest(self.store.latest_chained_blocks().map(Arc::new));
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{config, handle};
    use super::*;

    fn writer() -> FeedWriter {
        let handle = handle(&config("http://grpc:10000")).unwrap();
        FeedWriter {
            shared: handle.0.expect("enabled handle"),
            store: BlockStore::new(),
            anchor_rx: watch::channel(None).1,
        }
    }

    #[test]
    fn request_subscribes_processed_blocks_and_interslot_slots() {
        let request = writer().request(false);
        assert_eq!(request.commitment, Some(CommitmentLevel::Processed as i32));
        assert_eq!(request.from_slot, None);
        let blocks = &request.blocks["accounts_blocks"];
        assert_eq!(blocks.include_accounts, Some(true));
        assert_eq!(blocks.include_transactions, Some(false));
        assert_eq!(blocks.include_entries, Some(false));
        assert!(blocks.account_include.is_empty());
        let slots = &request.slots["accounts_slots"];
        assert_eq!(slots.interslot_updates, Some(true));
        assert_eq!(slots.filter_by_commitment, Some(false));
        assert!(request.accounts.is_empty() && request.transactions.is_empty());
    }
}
