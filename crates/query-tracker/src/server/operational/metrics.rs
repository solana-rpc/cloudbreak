// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /metrics` — Prometheus text exposition of the tracker's metrics.
//!
//! The metric definitions live in `stats::metrics`; this only renders them.

use crate::stats::metrics;
use http_body_util::Full;
use hyper::Response;
use hyper::body::Bytes;

pub fn handle() -> Response<Full<Bytes>> {
    Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(metrics::encode())))
        .unwrap()
}
