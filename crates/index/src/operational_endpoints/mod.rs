// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Operational HTTP server: Prometheus metrics plus the per-module debug endpoints.
//!
//! The server (transport + routing) lives here, decoupled from the metric collectors in
//! [`crate::metrics`]. Each route is implemented in its own submodule; `/metrics` is just one of
//! them.

use std::convert::Infallible;

use cloudbreak_core::IndexConfig;
use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::{rt::TokioIo, server::conn::auto};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::metrics::TokioTaskCounterGuard;

mod accounts_owner_map;
pub mod finalizer;
mod params;
mod prometheus;
pub mod self_healing;

/// Routes an operational HTTP request to the matching endpoint handler.
async fn route(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => prometheus::handle(),
        "/debug/modules/finalizer" => finalizer::handle(req.uri().query()).await,
        "/debug/modules/self_healing" => self_healing::handle(req.uri().query()).await,
        "/debug/accounts_owner_map" => accounts_owner_map::handle(),
        _ => Ok(not_found()),
    }
}

/// Initializes metrics state, registers the Prometheus collectors, and spawns the operational
/// HTTP server (Prometheus metrics + debug endpoints).
pub fn serve(config: &IndexConfig) -> anyhow::Result<()> {
    crate::metrics::setup(config);
    crate::metrics::register_collectors();

    let address = config.get_prom_metrics_collector_endpoint();

    tokio::spawn(async move {
        // NOTE: keep the "metrics_server" task label for dashboard/alert continuity.
        let _guard = TokioTaskCounterGuard::new("metrics_server");

        let listener = match TcpListener::bind(address).await {
            Ok(l) => {
                info!("Operational endpoints server started at http://{address} (/metrics, /debug/...)");
                l
            }
            Err(e) => {
                error!("Failed to bind operational endpoints server: {e:?}");
                return;
            }
        };

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    error!("Operational endpoints accept failed: {e:?}");
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let service = service_fn(route);

            tokio::spawn(async move {
                let _guard = TokioTaskCounterGuard::new("metrics_server");

                let builder = auto::Builder::new(hyper_util::rt::TokioExecutor::new());
                let conn = builder.serve_connection(io, service);
                if let Err(e) = conn.await {
                    error!("Operational endpoints connection failed: {e:?}");
                }
            });
        }
    });

    Ok(())
}

fn not_found() -> Response<Full<Bytes>> {
    Response::builder()
        .status(404)
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}

pub(crate) fn json_ok<T: Serialize>(value: &T) -> Result<Response<Full<Bytes>>, Infallible> {
    let body = serde_json::to_vec(value).unwrap_or_default();
    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}

pub(crate) fn json_error(status: u16, message: &str) -> Response<Full<Bytes>> {
    let body = serde_json::json!({ "error": message }).to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}
