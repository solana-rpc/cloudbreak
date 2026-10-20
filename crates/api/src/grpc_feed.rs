// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The API gRPC feed: a processed-commitment subscription (`modules::grpc`) whose blocks go to
//! the processed store and the mint cache. The slot syncronizer anchor goes to the processed
//! store. The task ends only when the subscription task ends, which is its panic on give-up.

use std::sync::{Arc, Mutex, atomic::AtomicBool};

use cloudbreak_core::ApiGrpcConfig;
use cloudbreak_core::modules::grpc::subscribe_grpc_with_reconnection;
use cloudbreak_core::modules::mint_cache::MintCache;
use cloudbreak_core::modules::processed::{Anchor, ProcessedAccounts};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, subscribe_update::UpdateOneof};

/// Starts the feed when `[grpc]` is enabled and the processed store or the mint cache needs it.
pub fn spawn(
    grpc: Option<&ApiGrpcConfig>,
    processed: &ProcessedAccounts,
    mint_cache: &MintCache,
    mut anchor_rx: watch::Receiver<Option<Anchor>>,
) -> Option<JoinHandle<()>> {
    let grpc = grpc.filter(|g| g.enabled)?.to_grpc_config();
    let mut writer = processed.writer();
    if !writer.is_enabled() && !mint_cache.is_enabled() {
        return None;
    }
    let mint_cache = mint_cache.clone();
    let (tx, mut rx) = mpsc::channel(grpc.sources_channel_size);
    let rx_len = Arc::new(Mutex::new(0));
    let last_slot_received = Arc::new(Mutex::new(0));
    // Detached: `lib.rs` panics when the consumer loop below ends.
    drop(subscribe_grpc_with_reconnection(
        grpc,
        CommitmentLevel::Processed,
        tx,
        rx_len.clone(),
        last_slot_received.clone(),
        Arc::new(AtomicBool::new(false)),
        None,
    ));

    Some(tokio::spawn(async move {
        let mut anchor_open = writer.is_enabled();
        loop {
            tokio::select! {
                update = rx.recv() => {
                    // The sender drops only when the subscription task ends.
                    let Some(update) = update else { break };
                    *rx_len.lock().expect("Failed to lock buffer_channel_rx_len") = rx.len();
                    let Some(UpdateOneof::Block(block)) = update.update_oneof else {
                        continue;
                    };
                    {
                        let mut last = last_slot_received
                            .lock()
                            .expect("Failed to lock last_slot_received");
                        *last = (*last).max(block.slot);
                    }
                    mint_cache.api().on_block(&block);
                    writer.on_block(block);
                }
                changed = anchor_rx.changed(), if anchor_open => {
                    if changed.is_err() {
                        anchor_open = false;
                        continue;
                    }
                    if let Some(anchor) = anchor_rx.borrow_and_update().clone() {
                        writer.set_anchor(anchor);
                    }
                }
            }
        }
    }))
}
