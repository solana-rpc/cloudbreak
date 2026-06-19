// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! JSON-RPC streaming response body construction.
//!
//! This module wraps a `Stream<Item = Result<RpcKeyedAccount, RpcError>>` in
//! a hyper-compatible `BoxBody` that emits the JSON-RPC envelope incrementally:
//!
//! ```
//! {"jsonrpc":"2.0","result":[<acc1>,<acc2>,...,<accN>],"id":<id>}
//! ```
//!
//! Serialized accounts are coalesced into ~32 KB chunks (with a 64 KB
//! pre-allocated buffer) before being yielded as `Bytes` so that hyper produces
//! a small number of fat HTTP body frames rather than one frame per account.

use std::convert::Infallible;
use std::ops::Range;
use std::time::Duration;

use async_stream::stream;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Frame;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::RpcResponseContext;
use tokio::time::Instant;

use crate::error::RpcError;
use crate::methods::program::{GpaStreamingResponse, encoding_to_string};
use crate::metrics;
use crate::modules::cache::MaybeJsonAccount;

/// Target chunk size before flushing the JSON buffer to a `Bytes` frame.
pub const STREAM_FLUSH_THRESHOLD: usize = 32 * 1024;

/// Initial buffer capacity
pub const STREAM_BUFFER_PREALLOC: usize = 64 * 1024;

/// Build a streaming JSON-RPC success body for a `getProgramAccounts` request.
///
/// Receives the stream of batches of encoded accounts and generates the JSON body
/// in a streaming fashion.
///
/// Note: It commits to a success response only after correctly processing the first batch.
pub async fn gpa_streaming_response_body(
    id: serde_json::Value,
    gpa_response: GpaStreamingResponse,
    gpa_global_start_time: Instant,
    subscription_id: String,
) -> Result<UnsyncBoxBody<Bytes, Infallible>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gpa_streaming");
    let mut accounts_stream = gpa_response.accounts_stream;
    let metrics_data = gpa_response.metrics_data;
    let mut gpa_processor = gpa_response.gpa_processor;
    let program = gpa_response.program;
    let encoding = gpa_response.encoding;

    let first_batch = match accounts_stream.next().await {
        Some(Err(e)) => return Err(e),
        Some(Ok(b)) => b,
        None => Vec::new(),
    };

    let streaming_response_body_wrapper =
        StreamingResponseBodyWrapper::new(gpa_response.context_slot, id);

    let body_stream = stream! {
        let json_span = tracing::info_span!(
            "json_encoding",
            program = %program,
            encoding = encoding_to_string(&encoding),
            accounts = tracing::field::Empty,
            wall_time = tracing::field::Empty,
            json_bytes = tracing::field::Empty,
            total_wall_time = tracing::field::Empty,
        );

        let mut json_encode_ms = Duration::from_millis(0);
        let mut json_bytes = 0u64;
        let mut accounts_count = first_batch.len() as u64;
        json_bytes += streaming_response_body_wrapper.start.len() as u64;

        // Yield with the start of the JSON array
        yield Ok::<_, Infallible>(Frame::data(Bytes::from(streaming_response_body_wrapper.start)));

        let ProcessedFirstBatch {
            mut buf,
            mut pending_fresh,
            mut pending_cached,
        } = match process_first_gpa_batch(first_batch, &mut json_encode_ms) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to serialize first batch; truncating body: {}", e);
                metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                    .with_label_values(&["gPA", "error"])
                    .inc();
                return;
            }
        };

        while let Some(item) = accounts_stream.next().await {
            let batch = match item {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("Account stream errored mid-response; truncating body: {e}" );
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gPA", "error"])
                        .inc();

                    // Leaving the body partial intentionally: client will hit a JSON parse error
                    // rather than receive a silently-truncated valid JSON.
                    return;
                }
            };

            accounts_count += batch.len() as u64;

            for acc in batch {
                let account_start_time = Instant::now();
                buf.put_u8(b',');

                match acc {
                    MaybeJsonAccount::Cached { pubkey, bytes } => {
                        // Cache hit: append the pre-serialized bytes
                        buf.extend_from_slice(&bytes);
                        pending_cached.push((pubkey, bytes));
                    }
                    MaybeJsonAccount::Fresh(keyed) => {
                        let start = buf.len();
                        let write_result = {
                            let mut w = (&mut buf).writer();
                            serde_json::to_writer(&mut w, &keyed.account)
                        };
                        if let Err(e) = write_result {
                            tracing::error!("Failed to serialize account in streaming body; truncating: {e}");
                            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                                .with_label_values(&["gPA", "error"])
                                .inc();
                            return;
                        }
                        let end = buf.len();
                        pending_fresh.push((keyed.pubkey, start..end));
                    }
                }

                json_encode_ms += account_start_time.elapsed();

                if buf.len() >= STREAM_FLUSH_THRESHOLD {
                    let frozen = buf.split().freeze();
                    json_bytes += frozen.len() as u64;

                    drain_pending_into_cache(
                        &frozen,
                        &mut pending_fresh,
                        &mut pending_cached,
                        &mut gpa_processor,
                    );

                    if buf.capacity() < STREAM_BUFFER_PREALLOC {
                        buf.reserve(STREAM_BUFFER_PREALLOC - buf.capacity());
                    }

                    yield Ok(Frame::data(frozen));
                }
            }
        }

        if !buf.is_empty() {
            let frozen = buf.split().freeze();
            json_bytes += frozen.len() as u64;

            drain_pending_into_cache(
                &frozen,
                &mut pending_fresh,
                &mut pending_cached,
                &mut gpa_processor,
            );

            yield Ok(Frame::data(frozen));
        }

        json_bytes += streaming_response_body_wrapper.end.len() as u64;

        if let Some(metrics_data) = metrics_data {
            metrics_data.record_metrics(
                json_encode_ms.as_millis() as f64,
                gpa_global_start_time.elapsed().as_millis() as f64,
                json_bytes,
                subscription_id,
            );
        }

        json_span.record("accounts", accounts_count as i64);
        json_span.record("wall_time", json_encode_ms.as_millis() as i64);
        json_span.record("json_bytes", json_bytes as i64);
        json_span.record("total_wall_time", gpa_global_start_time.elapsed().as_millis() as i64);

        // Commit the accumulated `(pubkey, bytes)` pairs as the new cached query
        gpa_processor.finalize_query();

        // Close the JSON array
        yield Ok(Frame::data(Bytes::from(streaming_response_body_wrapper.end)));
    };

    Ok(BodyExt::boxed_unsync(StreamBody::new(body_stream)))
}

struct ProcessedFirstBatch {
    buf: BytesMut,
    pending_fresh: Vec<(Pubkey, Range<usize>)>,
    pending_cached: Vec<(Pubkey, Bytes)>,
}

fn process_first_gpa_batch(
    batch: Vec<MaybeJsonAccount>,
    json_encode_ms: &mut Duration,
) -> Result<ProcessedFirstBatch, serde_json::Error> {
    let start_time = Instant::now();
    let mut buf = BytesMut::with_capacity(STREAM_BUFFER_PREALLOC);
    let mut pending_fresh: Vec<(Pubkey, Range<usize>)> = Vec::new();
    let mut pending_cached: Vec<(Pubkey, Bytes)> = Vec::new();

    let mut first = true;
    for acc in batch {
        if !first {
            buf.put_u8(b',');
        }
        first = false;

        match acc {
            MaybeJsonAccount::Cached { pubkey, bytes } => {
                buf.extend_from_slice(&bytes);
                pending_cached.push((pubkey, bytes));
            }
            MaybeJsonAccount::Fresh(keyed) => {
                let start = buf.len();
                {
                    let mut w = (&mut buf).writer();
                    serde_json::to_writer(&mut w, &keyed.account)?;
                }
                let end = buf.len();
                pending_fresh.push((keyed.pubkey, start..end));
            }
        }
    }

    *json_encode_ms += start_time.elapsed();

    Ok(ProcessedFirstBatch {
        buf,
        pending_fresh,
        pending_cached,
    })
}

/// Convert the pending `Range`s into `Bytes` slices of the just-frozen
/// chunk and hand the merged `(pubkey, bytes)` set to the processor's
/// accumulator. The `Range`s only stay valid until the next `split()`, so
/// this must be called before reusing `buf`.
fn drain_pending_into_cache(
    frozen: &Bytes,
    pending_fresh: &mut Vec<(Pubkey, Range<usize>)>,
    pending_cached: &mut Vec<(Pubkey, Bytes)>,
    gpa_processor: &mut crate::modules::cache::GpaProcessor,
) {
    if pending_fresh.is_empty() && pending_cached.is_empty() {
        return;
    }

    let cache_hits = pending_cached.len() as u64;

    let mut new_pairs: Vec<(Pubkey, Bytes)> =
        Vec::with_capacity(pending_fresh.len() + pending_cached.len());
    new_pairs.append(pending_cached);
    for (pk, range) in pending_fresh.drain(..) {
        // `Bytes::slice` is O(1): pointer math + atomic refcount inc on
        // the same allocation backing `frozen`. The slice keeps that
        // allocation alive for as long as the cache entry holds it.
        new_pairs.push((pk, frozen.slice(range)));
    }

    gpa_processor.update_new_accounts_for_query(new_pairs, cache_hits);
}

/// Contains the start and end of the JSON array (or context-wrapped value) and the id
struct StreamingResponseBodyWrapper {
    /// Start of the JSON array (or context-wrapped value)
    start: String,
    /// End of the JSON array (and the context wrapper, if any) and add the id
    end: String,
}

impl StreamingResponseBodyWrapper {
    /// When `context_slot` is set, the JSON-RPC `result` becomes `{"context":{"slot":N},"value":[...]}` instead of a bare array.
    /// We serialize a real `RpcResponseContext` so any future fields (`apiVersion`, etc.) flow through automatically.
    fn new(context_slot: Option<u64>, request_id: serde_json::Value) -> Self {
        let request_id = serde_json::to_string(&request_id).unwrap_or_else(|_| "null".to_string());

        let (start, end) = match context_slot {
            Some(slot) => {
                let ctx_json = serde_json::to_string(&RpcResponseContext::new(slot))
                    .unwrap_or_else(|_| format!(r#"{{"slot":{slot}}}"#));
                (
                    format!(r#"{{"jsonrpc":"2.0","result":{{"context":{ctx_json},"value":["#),
                    format!(r#"]}},"id":{request_id}}}"#),
                )
            }
            None => (
                r#"{"jsonrpc":"2.0","result":["#.to_string(),
                format!(r#"],"id":{request_id}}}"#),
            ),
        };

        Self { start, end }
    }
}
