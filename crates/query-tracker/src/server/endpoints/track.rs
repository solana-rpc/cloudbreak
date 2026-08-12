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
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::{Request, Response, StatusCode};
use std::sync::Arc;
use tracing::error;

pub async fn handle(req: Request<Incoming>, state: &Arc<AppState>) -> Response<Full<Bytes>> {
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            error!(target: "query_tracker_server", "failed to read /track body: {e}");
            return text(StatusCode::BAD_REQUEST, "failed to read body");
        }
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
