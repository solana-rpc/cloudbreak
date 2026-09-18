// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Yellowstone gRPC subscription with reconnection, shared by the indexer and
//! the API processed feed.
//!
//! [`subscribe_with_reconnection`] connects, runs [`Subscriber::on_connect`],
//! subscribes with [`Subscriber::request`] and hands the stream to
//! [`Subscriber::session`] until it ends. A failed connect or subscribe, or a
//! session that errored before it delivered a block, opens the give-up window.
//! While the window is open every attempt first waits `reconnect_backoff`,
//! lets the request replay only while the window is younger than
//! `reconnect_from_slot_retain`, and, with `reconnect_give_up` set, panics
//! once the window is older than that. `reconnect_give_up = None` keeps
//! retrying. A session that delivered a block, or ended without an error,
//! closes the window and reconnects at once. The loop returns only when
//! [`Subscriber::cancelled`] is true.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::{FutureExt, Stream};
use tokio::time::Instant;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
    SubscribeUpdate,
};
use yellowstone_grpc_proto::tonic::{Status, codec::CompressionEncoding};

const KEEPALIVE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct GrpcClientOptions {
    pub endpoint: String,
    /// Sent as the `x-token` header when set.
    pub x_token: Option<String>,
    /// Bound on connecting and on each request.
    pub timeout: Duration,
    pub max_decoding_message_size: usize,
    /// Wait before an attempt while the give-up window is open.
    pub reconnect_backoff: Duration,
    /// Panics when the give-up window is older than this. `None` keeps retrying.
    pub reconnect_give_up: Option<Duration>,
    /// The request may replay from a slot only while the give-up window is younger than this.
    pub reconnect_from_slot_retain: Duration,
}

/// How a session ended. `Failed` is an error before any block, which keeps the
/// give-up window open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEnd {
    Healthy,
    Failed,
}

/// The caller side of one subscription.
pub trait Subscriber: Send {
    /// The request for one session. `replay` is true when the loop allows a
    /// reconnect to set `from_slot`.
    fn request(&self, replay: bool) -> SubscribeRequest;

    /// True when the loop must stop.
    fn cancelled(&self) -> bool;

    /// Runs after a connect or subscribe failure.
    fn on_connect_failed(&mut self);

    /// Runs on every new connection, before the subscribe.
    fn on_connect(&mut self, client: &mut GeyserGrpcClient) -> impl Future<Output = ()> + Send;

    /// Consumes one stream until it ends, errors or stalls.
    fn session(
        &mut self,
        stream: impl Stream<Item = Result<SubscribeUpdate, Status>> + Send,
    ) -> impl Future<Output = SessionEnd> + Send;
}

/// Runs the subscriber until it cancels, reconnecting as the module doc describes.
pub async fn subscribe_with_reconnection<S: Subscriber>(
    options: GrpcClientOptions,
    mut subscriber: S,
) {
    let mut reconnect_failed_since: Option<Instant> = None;
    let mut is_reconnect = false;

    loop {
        if subscriber.cancelled() {
            tracing::info!("GRPC subscription cancelled");
            return;
        }

        if let Some(started) = reconnect_failed_since {
            if options
                .reconnect_give_up
                .is_some_and(|give_up| started.elapsed() >= give_up)
            {
                tracing::error!(
                    "Failed to (re)connect to Yellowstone GRPC after {:?}",
                    started.elapsed()
                );
                panic!(
                    "Failed to (re)connect to Yellowstone GRPC after {:?}",
                    started.elapsed()
                );
            }
            tokio::time::sleep(options.reconnect_backoff).await;
        }

        let mut client = match connect(&options).await {
            Ok(client) => client,
            Err(e) => {
                reconnect_failed_since.get_or_insert_with(Instant::now);
                tracing::error!("Failed to connect to Yellowstone GRPC: {:?}", e);
                subscriber.on_connect_failed();
                continue;
            }
        };
        subscriber.on_connect(&mut client).await;

        // Replay only while the window is young. The server may not have older slots buffered.
        let keep_from_slot = reconnect_failed_since
            .is_none_or(|started| started.elapsed() < options.reconnect_from_slot_retain);
        let request = subscriber.request(is_reconnect && keep_from_slot);
        let from_slot = request.from_slot;

        let (_subscribe_tx, stream) = match client.subscribe_with_request(Some(request)).await {
            Ok(subscription) => {
                if let Some(slot) = from_slot {
                    tracing::info!(
                        "Reconnected to Yellowstone GRPC replaying from slot {}",
                        slot
                    );
                }
                subscription
            }
            Err(e) => {
                reconnect_failed_since.get_or_insert_with(Instant::now);
                tracing::error!(
                    "Failed to subscribe to Yellowstone GRPC (from_slot {:?}): {:?}",
                    from_slot,
                    e
                );
                subscriber.on_connect_failed();
                continue;
            }
        };

        match AssertUnwindSafe(subscriber.session(stream))
            .catch_unwind()
            .await
        {
            Ok(SessionEnd::Failed) => {
                reconnect_failed_since.get_or_insert_with(Instant::now);
            }
            Ok(SessionEnd::Healthy) => reconnect_failed_since = None,
            Err(_) => {
                tracing::error!("GRPC subscription session panicked");
                reconnect_failed_since.get_or_insert_with(Instant::now);
            }
        }

        is_reconnect = true;
    }
}

/// The blocks-with-accounts subscription with a slot status stream.
pub fn blocks_with_accounts_request(
    commitment: CommitmentLevel,
    interslot_updates: bool,
    from_slot: Option<u64>,
) -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::from([(
            "accounts_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(interslot_updates),
            },
        )]),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::from([(
            "accounts_blocks".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(false),
                include_accounts: Some(true),
                include_entries: Some(false),
                cuckoo_account_include: None,
            },
        )]),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(commitment as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot,
    }
}

async fn connect(
    options: &GrpcClientOptions,
) -> Result<GeyserGrpcClient, yellowstone_grpc_client::GeyserGrpcBuilderError> {
    GeyserGrpcClient::build_from_shared(options.endpoint.clone())
        .expect("Failed to build GeyserGrpcClient")
        .x_token(options.x_token.clone())
        .expect("Failed to set x-token")
        .max_decoding_message_size(options.max_decoding_message_size)
        .accept_compressed(CompressionEncoding::Zstd)
        .connect_timeout(options.timeout)
        .timeout(options.timeout)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("Failed to set tls config")
        .tcp_keepalive(Some(KEEPALIVE))
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_timeout(KEEPALIVE)
        .connect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counts failed attempts against a closed port and cancels after `stop_after`.
    struct Counting {
        attempts: usize,
        stop_after: usize,
    }

    impl Subscriber for Counting {
        fn request(&self, _replay: bool) -> SubscribeRequest {
            SubscribeRequest::default()
        }

        fn cancelled(&self) -> bool {
            self.attempts >= self.stop_after
        }

        fn on_connect_failed(&mut self) {
            self.attempts += 1;
        }

        async fn on_connect(&mut self, _client: &mut GeyserGrpcClient) {}

        async fn session(
            &mut self,
            _stream: impl Stream<Item = Result<SubscribeUpdate, Status>> + Send,
        ) -> SessionEnd {
            SessionEnd::Healthy
        }
    }

    fn options(reconnect_give_up: Option<Duration>) -> GrpcClientOptions {
        // The binaries install the provider at startup. Tests connect without one.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        GrpcClientOptions {
            endpoint: "http://127.0.0.1:1".to_string(),
            x_token: None,
            timeout: Duration::from_millis(500),
            max_decoding_message_size: 1024,
            reconnect_backoff: Duration::from_millis(1),
            reconnect_give_up,
            reconnect_from_slot_retain: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn without_give_up_the_loop_keeps_retrying_until_cancelled() {
        let counting = Counting {
            attempts: 0,
            stop_after: 3,
        };
        let done = tokio::time::timeout(
            Duration::from_secs(10),
            subscribe_with_reconnection(options(None), counting),
        )
        .await;
        assert!(done.is_ok(), "the loop must return once cancelled");
    }

    #[tokio::test]
    #[should_panic(expected = "Failed to (re)connect to Yellowstone GRPC")]
    async fn with_give_up_the_loop_panics_once_the_window_expires() {
        let counting = Counting {
            attempts: 0,
            stop_after: 100,
        };
        subscribe_with_reconnection(options(Some(Duration::ZERO)), counting).await;
    }
}
