// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::convert::Infallible;

use http_body_util::Full;
use hyper::{Response, body::Bytes};
use prometheus::TextEncoder;
use tracing::error;

/// `GET /metrics` — exposes the Prometheus collectors in text exposition format.
pub(crate) fn handle() -> Result<Response<Full<Bytes>>, Infallible> {
    let metrics = TextEncoder::new()
        .encode_to_string(&crate::metrics::METRICS_REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {error}");
            String::new()
        });

    Ok(Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(metrics)))
        .unwrap())
}
