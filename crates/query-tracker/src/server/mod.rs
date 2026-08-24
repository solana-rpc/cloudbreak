// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Server — the tracker's HTTP edge (plain `hyper`, no JSON-RPC).
//!
//! JSON-RPC bought us nothing here: there is exactly one internal producer (the
//! API) and a handful of typed calls, so its envelope, id/versioning and error
//! objects were pure overhead. Plain HTTP with typed structs
//! ([`cloudbreak_core::modules::query_tracker_api`]) is simpler to write, read
//! and debug, and the request/response bodies are still strongly typed.
//!
//! One server on the configured port serves both the functional endpoints
//! (`endpoints/`: `POST /track`) and the operational endpoints (`operational/`:
//! `GET /debug/*`, `GET /metrics`, `GET /health`). Each endpoint is its own
//! file, so the routing table below and the folder tree tell you the full API.

pub mod endpoints;
pub mod operational;

use crate::modules::store::Store;
use cloudbreak_core::QueryTrackerConfig;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Serialize;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Shared handler state.
pub struct AppState {
    pub store: Store,
    pub config: QueryTrackerConfig,
}

/// Serve every HTTP endpoint (ingest, debug, metrics, health) on `addr` until
/// the process exits.
pub async fn serve(addr: SocketAddr, state: Arc<AppState>) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!(target: "query_tracker_server", "query tracker API listening on http://{addr}");
            l
        }
        Err(e) => {
            error!(target: "query_tracker_server", "failed to bind API server to {addr}: {e}");
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!(target: "query_tracker_server", "accept failed: {e:?}");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let state = state.clone();
        let service = service_fn(move |req: Request<Incoming>| {
            let state = state.clone();
            async move { Ok::<_, Infallible>(route(req, state).await) }
        });
        tokio::spawn(async move {
            let builder = auto::Builder::new(TokioExecutor::new());
            if let Err(e) = builder.serve_connection(io, service).await {
                error!(target: "query_tracker_server", "connection error: {e:?}");
            }
        });
    }
}

async fn route(req: Request<Incoming>, state: Arc<AppState>) -> Response<Full<Bytes>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, cloudbreak_core::modules::query_tracker_api::TRACK_PATH) => {
            endpoints::track::handle(req, &state).await
        }
        (&Method::GET, "/debug/candidates") => {
            operational::debug_candidates::handle(&state, req.uri().query()).await
        }
        (&Method::GET, "/debug/created") => {
            operational::debug_created::handle(&state, req.uri().query()).await
        }
        (&Method::GET, "/debug/discrepancies") => {
            operational::debug_discrepancies::handle(&state, req.uri().query()).await
        }
        (&Method::GET, "/metrics") => operational::metrics::handle(),
        (&Method::GET, "/health") => operational::health::handle(),
        _ => text(StatusCode::NOT_FOUND, "Not Found"),
    }
}

/// Build a JSON response, falling back to a 500 if serialization fails.
pub fn json<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(value) {
        Ok(body) => Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap(),
        Err(e) => {
            error!(target: "query_tracker_server", "failed to serialize response: {e}");
            text(StatusCode::INTERNAL_SERVER_ERROR, "serialization error")
        }
    }
}

pub fn text(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
