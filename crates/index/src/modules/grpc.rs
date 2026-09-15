// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::grpc::{
    GrpcClientOptions, SessionEnd, Subscriber, blocks_with_accounts_request,
    subscribe_with_reconnection,
};
use cloudbreak_core::{EnvironmentInfo, IndexConfig};
use futures::{Stream, StreamExt};
use sea_orm::DatabaseConnection;
use std::{
    ops::Add,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::mpsc::Sender,
    task::JoinHandle,
    time::{Instant, timeout},
};
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::{
    geyser::{CommitmentLevel, SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof},
    tonic::Status,
};

use crate::metrics;

async fn store_grpc_version(version_json: &str, db: &DatabaseConnection) {
    let grpc_version = serde_json::from_str::<serde_json::Value>(version_json)
        .ok()
        .and_then(|v| {
            v.get("version")
                .and_then(|inner| inner.get("version").and_then(|s| s.as_str()))
                .or_else(|| v.get("version").and_then(|s| s.as_str()))
                .map(str::to_string)
        });

    match grpc_version {
        Some(grpc_version) => {
            if let Err(e) = EnvironmentInfo::upsert_grpc_version(db, &grpc_version).await {
                tracing::error!("Failed to upsert grpc version: {:?}", e);
            }
        }
        None => tracing::error!(
            "Failed to parse grpc version from response: {}",
            version_json
        ),
    }
}

/// Creates a persistent Yellowstone GRPC connection with automatic reconnection.
/// Spawns a background task to handle the stream and forwards updates to the buffer channel.
/// Automatically reconnects on stream timeouts , stream `None` or errors (only after exceeding
///  the `max_grpc_errors` count).
///
/// The reconnect loop, the give-up window, the backoff and the `from_slot` replay live in
/// `cloudbreak_core::grpc`. Connect/subscribe failures, and streams that error before delivering
/// any block (e.g. the server returns "failed to get replay response" for a `from_slot` it no
/// longer has), all keep the window open: the loop backs off by `config.grpc.reconnect_backoff`
/// between attempts, drops `from_slot` after `reconnect_from_slot_retain` (resubscribing from the
/// live tip), and panics once the failing window exceeds `config.grpc.reconnect_give_up`. A stream
/// that received blocks and then ended (error, inactivity timeout, or stream `None`) is treated as
/// a healthy run and reconnects immediately.
pub fn subscribe_grpc_with_reconnection(
    config: IndexConfig,
    buffer_channel_tx: Sender<SubscribeUpdate>,
    buffer_channel_rx_len: Arc<Mutex<usize>>,
    last_slot_received: Arc<Mutex<u64>>,
    cancel: Arc<AtomicBool>,
    db: DatabaseConnection,
) -> JoinHandle<()> {
    let grpc_timeout = Duration::from_secs(config.grpc.timeout);
    let options = GrpcClientOptions {
        endpoint: config.grpc.endpoint.clone(),
        x_token: Some(config.grpc.x_token.clone().unwrap_or_default()),
        timeout: grpc_timeout,
        max_decoding_message_size: usize::MAX,
        reconnect_backoff: config.grpc.reconnect_backoff,
        reconnect_give_up: Some(config.grpc.reconnect_give_up),
        reconnect_from_slot_retain: config.grpc.reconnect_from_slot_retain,
    };
    let subscriber = IndexerSubscriber {
        grpc_timeout,
        max_grpc_errors: config.grpc.max_grpc_errors,
        buffer_channel_tx,
        buffer_channel_rx_len,
        last_slot_received,
        cancel,
        db,
    };
    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("grpc");
        subscribe_with_reconnection(options, subscriber).await;
    })
}

struct IndexerSubscriber {
    grpc_timeout: Duration,
    max_grpc_errors: usize,
    buffer_channel_tx: Sender<SubscribeUpdate>,
    buffer_channel_rx_len: Arc<Mutex<usize>>,
    last_slot_received: Arc<Mutex<u64>>,
    cancel: Arc<AtomicBool>,
    db: DatabaseConnection,
}

impl Subscriber for IndexerSubscriber {
    /// A replay starts at the slot after the last received one.
    fn request(&self, replay: bool) -> SubscribeRequest {
        let from_slot = replay.then(|| {
            let last = *self
                .last_slot_received
                .lock()
                .expect("Failed to lock last_slot_received");
            (last != 0).then_some(last + 1)
        });
        blocks_with_accounts_request(CommitmentLevel::Confirmed, false, from_slot.flatten())
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    fn on_attempt_failed(&mut self) {
        metrics::increment_grpc_errors();
    }

    async fn on_connect(&mut self, client: &mut GeyserGrpcClient<impl Interceptor + Send>) {
        match client.get_version().await {
            Ok(response) => store_grpc_version(&response.version, &self.db).await,
            Err(e) => tracing::error!("Failed to get grpc version: {:?}", e),
        }
    }

    async fn session(
        &mut self,
        stream: impl Stream<Item = Result<SubscribeUpdate, Status>> + Send,
    ) -> SessionEnd {
        let _guard = metrics::TokioTaskCounterGuard::new("grpc");

        let mut stream = std::pin::pin!(stream);

        let mut log_first_message = true;
        let mut last_block_received_at = Instant::now();
        let mut grpc_current_errors = 0;

        // Health signals for the outer give-up window: a run only clears the
        // window if it delivered a block; a stream that *errored* before any
        // block is a failed attempt (engages backoff). A no-block inactivity
        // timeout / stream `None` is neither (reconnects immediately, as-is).
        let mut received_block = false;
        let mut stream_errored = false;

        let mut buffer_channel_size =
            self.buffer_channel_tx.max_capacity() - self.buffer_channel_tx.capacity();

        // Add a timeout in case we stop receiving updates for 30 more seconds than the grpc timeout
        // If we reach it, we break the loop and try to reconnect
        while let Some(update) = timeout(
            self.grpc_timeout.add(Duration::from_secs(30)),
            stream.next(),
        )
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!(
                "GRPC timeout: {:?} - grpc_errors_count: {}",
                elapsed,
                grpc_current_errors,
            );
            metrics::increment_grpc_timeout_errors();

            // If the timeout is reached, we return None to break the loop
            None
        }) {
            if self.cancelled() {
                tracing::info!("GRPC subscription cancelled mid-stream");
                // Shutting down, not a failed attempt.
                return SessionEnd::Healthy;
            }

            metrics::GRPC_TOTAL_UPDATES_RECEIVED.inc();

            if Instant::now().duration_since(last_block_received_at) > Duration::from_secs(30) {
                tracing::error!("No block received in the last 30 seconds");
                grpc_current_errors += 1;
                metrics::increment_grpc_errors();

                if grpc_current_errors >= self.max_grpc_errors {
                    break;
                }
            }

            buffer_channel_size =
                self.buffer_channel_tx.max_capacity() - self.buffer_channel_tx.capacity();

            metrics::GRPC_BUFFER_CHANNEL_SIZE_SENDER.set(buffer_channel_size as i64);

            match update {
                Ok(update) => {
                    if let Some(UpdateOneof::Block(block)) = &update.update_oneof {
                        last_block_received_at = Instant::now();
                        received_block = true;

                        if log_first_message {
                            tracing::info!(
                                "Starting a new indexer service run - slot: {}",
                                block.slot
                            );
                            log_first_message = false;
                        }
                    }

                    self.buffer_channel_tx
                        .send(update)
                        .await
                        .expect("Failed to send update to buffer channel");
                }
                Err(e) => {
                    stream_errored = true;
                    tracing::error!(
                        "GRPC error: {:?} buffer_channel_size: {} (sender: {}) - grpc_errors_count: {}",
                        e,
                        *self
                            .buffer_channel_rx_len
                            .lock()
                            .expect("Failed to lock buffer_channel_rx_len"),
                        buffer_channel_size,
                        grpc_current_errors,
                    );
                    grpc_current_errors += 1;
                    metrics::increment_grpc_errors();

                    if grpc_current_errors >= self.max_grpc_errors {
                        break;
                    }
                }
            }
        }

        tracing::error!(
            "Breaking out of grpc subscription loop at slot: {} - buffer_channel_size: {} (sender: {})",
            *self
                .last_slot_received
                .lock()
                .expect("Failed to lock last_slot_received"),
            *self
                .buffer_channel_rx_len
                .lock()
                .expect("Failed to lock buffer_channel_rx_len"),
            buffer_channel_size,
        );

        // A failed attempt = the stream errored before delivering any block.
        // A run that saw a block, or ended via inactivity timeout / stream
        // `None` without erroring, is not penalized (reconnects immediately).
        if stream_errored && !received_block {
            SessionEnd::Failed
        } else {
            SessionEnd::Healthy
        }
    }
}
