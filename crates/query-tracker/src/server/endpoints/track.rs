// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `POST /track` — the single ingest endpoint.
//!
//! Body is a [`TrackBatch`]; (a single query is just a batch of length one). On a database error we
//! return `500` (not `200`) so the client keeps the batch and retries — demand
//! must never be silently lost because a flush hit a transient DB issue.

use crate::modules::ingest;
use crate::server::{AppState, json, text};
use cloudbreak_core::modules::query_tracker_api::TrackBatch;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::{Body, Bytes, Incoming};
use hyper::{Request, Response, StatusCode};
use std::error::Error;
use std::sync::Arc;
use tracing::error;

pub async fn handle(req: Request<Incoming>, state: &Arc<AppState>) -> Response<Full<Bytes>> {
    let body = match read_body(req.into_body(), state.config.max_track_body_size).await {
        Ok(body) => body,
        Err((status, message)) => return text(status, message),
    };

    let batch: TrackBatch = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => {
            error!(target: "query_tracker_server", "invalid /track body: {e}");
            return text(StatusCode::BAD_REQUEST, "invalid JSON body");
        }
    };

    match ingest::apply_batch(&state.store, batch).await {
        Ok(resp) => json(StatusCode::OK, &resp),
        Err(e) => {
            error!(target: "query_tracker_server", "failed to apply track batch: {e:?}");
            text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to persist observations",
            )
        }
    }
}

/// Reads the whole body. Stops with 413 at more than `limit` bytes, and with 400 on a read error.
async fn read_body<B>(body: B, limit: usize) -> Result<Bytes, (StatusCode, &'static str)>
where
    B: Body,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    match Limited::new(body, limit).collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(e) if e.is::<LengthLimitError>() => {
            error!(target: "query_tracker_server", "/track body exceeds {limit} bytes");
            Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body size exceeds allowed maximum",
            ))
        }
        Err(e) => {
            error!(target: "query_tracker_server", "failed to read /track body: {e}");
            Err((StatusCode::BAD_REQUEST, "failed to read body"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::StreamBody;
    use hyper::body::Frame;
    use std::convert::Infallible;

    const LIMIT: usize = 1024;

    #[tokio::test]
    async fn body_at_limit_is_read() {
        let body = Full::new(Bytes::from(vec![b'1'; LIMIT]));
        assert_eq!(read_body(body, LIMIT).await.unwrap().len(), LIMIT);
    }

    #[tokio::test]
    async fn body_over_limit_gets_413() {
        let body = Full::new(Bytes::from(vec![b'1'; LIMIT + 1]));
        let (status, _) = read_body(body, LIMIT).await.unwrap_err();
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn endless_body_gets_413() {
        let frames = futures::stream::repeat_with(|| {
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(&[b'1'; 256])))
        });
        let (status, _) = read_body(StreamBody::new(frames), LIMIT).await.unwrap_err();
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }
}
