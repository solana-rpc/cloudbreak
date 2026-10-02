// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The API gRPC feed thread and the single writer.
//!
//! The writer runs the shared gRPC client on its own OS thread with a
//! current-thread runtime. One session subscribes to processed blocks with
//! accounts and to slot statuses with interslot updates. With processed accounts
//! enabled, every block, `SLOT_CREATED_BANK`, `SLOT_DEAD` and anchor change is
//! applied to the store, followed by a prune, a selection of the latest chained
//! blocks and a publish. `SLOT_CONFIRMED` and `SLOT_FINALIZED` go to the slot lag
//! tracker. A stream end, error or stall ends the session, and the client
//! reconnects with no `from_slot`. The client never gives up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use tokio::sync::watch;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::Status;

use super::ingest::SlotBlock;
use super::store::BlockStore;
use super::{
    Anchor, CONNECT_TIMEOUT, MAX_DECODING_MESSAGE_SIZE, RECONNECT_BACKOFF, STALL_TIMEOUT, Shared,
};
use crate::config::ApiGrpcConfig;
use crate::grpc::{
    GrpcClientOptions, SessionEnd, Subscriber, blocks_with_accounts_request,
    subscribe_with_reconnection,
};
use crate::modules::slot_lag::{SlotCommitment, SlotLag, SlotSource};

/// Starts `grpc-feed`. Logs and returns when the thread cannot start.
pub(super) fn spawn_feed(
    grpc: &ApiGrpcConfig,
    shared: Option<Arc<Shared>>,
    anchor_rx: watch::Receiver<Option<Anchor>>,
    slot_lag: SlotLag,
) {
    let options = GrpcClientOptions {
        endpoint: grpc.endpoint.clone(),
        x_token: grpc.x_token.clone(),
        timeout: CONNECT_TIMEOUT,
        max_decoding_message_size: MAX_DECODING_MESSAGE_SIZE,
        reconnect_backoff: RECONNECT_BACKOFF,
        reconnect_give_up: None,
        reconnect_from_slot_retain: Duration::ZERO,
    };
    let writer = FeedWriter {
        processed: shared.map(|shared| ProcessedWriter {
            shared,
            store: BlockStore::new(),
        }),
        anchor_rx,
        slot_lag,
    };
    let spawned = std::thread::Builder::new()
        .name("grpc-feed".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    tracing::error!("Failed to build grpc-feed runtime: {e}");
                    return;
                }
            };
            runtime.block_on(subscribe_with_reconnection(options, writer));
        });
    if let Err(e) = spawned {
        tracing::error!("Failed to start grpc-feed thread: {e}");
    }
}

struct FeedWriter {
    /// `None` when processed accounts are disabled: blocks are received and dropped.
    processed: Option<ProcessedWriter>,
    anchor_rx: watch::Receiver<Option<Anchor>>,
    slot_lag: SlotLag,
}

struct ProcessedWriter {
    shared: Arc<Shared>,
    store: BlockStore,
}

impl Subscriber for FeedWriter {
    fn request(&self, _replay: bool) -> SubscribeRequest {
        blocks_with_accounts_request(CommitmentLevel::Processed, true, None)
    }

    fn cancelled(&self) -> bool {
        false
    }

    fn on_connect_failed(&mut self) {}

    async fn on_connect(&mut self, _client: &mut GeyserGrpcClient) {}

    async fn session(
        &mut self,
        stream: impl Stream<Item = Result<SubscribeUpdate, Status>> + Send,
    ) -> SessionEnd {
        tracing::info!("grpc feed subscribed");
        if let Some(processed) = &mut self.processed {
            processed.store.new_session();
        }
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
                        tracing::warn!("grpc feed stream error: {status}");
                        break if received_block {
                            SessionEnd::Healthy
                        } else {
                            SessionEnd::Failed
                        };
                    }
                    None => {
                        tracing::warn!("grpc feed stream ended");
                        break SessionEnd::Healthy;
                    }
                },
                changed = self.anchor_rx.changed() => {
                    if changed.is_ok() {
                        self.apply_anchor();
                    }
                }
                () = &mut stall => {
                    tracing::warn!("grpc feed stalled for {STALL_TIMEOUT:?}");
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
                if let Some(processed) = &mut self.processed {
                    let received_at = Instant::now();
                    let block = SlotBlock::from_update(
                        block,
                        &processed.shared.program_filter,
                        received_at,
                    );
                    processed.store.on_block(block);
                    processed.publish_latest();
                }
                true
            }
            Some(UpdateOneof::Slot(slot)) => {
                let status = SlotStatus::try_from(slot.status);
                let commitment = match status {
                    Ok(SlotStatus::SlotConfirmed) => Some(SlotCommitment::Confirmed),
                    Ok(SlotStatus::SlotFinalized) => Some(SlotCommitment::Finalized),
                    _ => None,
                };
                if let Some(commitment) = commitment {
                    self.slot_lag
                        .record(SlotSource::Grpc, commitment, slot.slot, Instant::now());
                }
                let Some(processed) = &mut self.processed else {
                    return false;
                };
                match status {
                    Ok(SlotStatus::SlotCreatedBank) => {
                        processed.store.on_created_bank(slot.slot, slot.parent)
                    }
                    Ok(SlotStatus::SlotDead) => processed.store.on_dead(slot.slot),
                    _ => return false,
                }
                processed.publish_latest();
                false
            }
            _ => false,
        }
    }

    /// Applies the current anchor when there is one and processed accounts are enabled.
    fn apply_anchor(&mut self) {
        let anchor = self.anchor_rx.borrow_and_update().clone();
        if let (Some(processed), Some(anchor)) = (&mut self.processed, anchor) {
            processed.store.set_anchor(anchor);
            processed.publish_latest();
        }
    }
}

impl ProcessedWriter {
    /// Prunes, selects and publishes.
    fn publish_latest(&mut self) {
        self.store.prune();
        self.shared
            .set_latest(self.store.latest_chained_blocks().map(Arc::new));
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::handle;
    use super::*;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateSlot;

    fn writer() -> FeedWriter {
        FeedWriter {
            processed: Some(ProcessedWriter {
                shared: handle().0.expect("enabled handle"),
                store: BlockStore::new(),
            }),
            anchor_rx: watch::channel(None).1,
            slot_lag: SlotLag::default(),
        }
    }

    fn slot_update(slot: u64, status: SlotStatus) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: slot.checked_sub(1),
                status: status as i32,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn slot_statuses_without_processed_accounts_are_not_blocks() {
        let mut writer = writer();
        writer.processed = None;
        assert!(!writer.apply_update(slot_update(10, SlotStatus::SlotConfirmed)));
        assert!(!writer.apply_update(slot_update(10, SlotStatus::SlotCreatedBank)));
        writer.apply_anchor();
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
