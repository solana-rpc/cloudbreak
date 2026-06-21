// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{convert::Infallible, net::SocketAddr};

use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::{rt::TokioIo, server::conn::auto};
use prometheus::{IntGauge, Registry, TextEncoder};
use tokio::net::TcpListener;
use tracing::{error, info};

lazy_static::lazy_static! {
    static ref METRICS_REGISTRY: Registry = Registry::new();

    /// Current number of indexes present on the `snapshot_accounts` table.
    pub static ref SNAPSHOT_ACCOUNTS_INDEXES: IntGauge = {
        let gauge = IntGauge::new(
            "query_tracker_snapshot_accounts_indexes_total",
            "Current number of indexes on the snapshot_accounts table",
        )
        .expect("failed to create snapshot_accounts indexes gauge");
        METRICS_REGISTRY
            .register(Box::new(gauge.clone()))
            .expect("failed to register snapshot_accounts indexes gauge");
        gauge
    };
}

fn metrics_handler() -> Result<Response<Full<Bytes>>, Infallible> {
    let metrics = TextEncoder::new()
        .encode_to_string(&METRICS_REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {error}");
            String::new()
        });

    Ok(Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(metrics)))
        .unwrap())
}

async fn handle_metrics_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => metrics_handler(),
        "/health" => Ok(Response::builder()
            .status(200)
            .body(Full::new(Bytes::from("OK")))
            .unwrap()),
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

pub async fn serve_metrics(addr: SocketAddr) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!("Prometheus server started at http://{}/metrics", addr);
            l
        }
        Err(e) => {
            error!("Failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!("Prometheus accept failed: {e:?}");
                continue;
            }
        };

        let io = TokioIo::new(stream);
        let service = service_fn(move |req: Request<Incoming>| handle_metrics_request(req));

        tokio::spawn(async move {
            let builder = auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            let conn = builder.serve_connection(io, service);
            if let Err(e) = conn.await {
                error!("Prometheus connection failed: {e:?}");
            }
        });
    }
}
