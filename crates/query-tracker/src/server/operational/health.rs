// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /health` — liveness probe; returns `200 OK` while the service runs.

use crate::server::text;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};

pub fn handle() -> Response<Full<Bytes>> {
    text(StatusCode::OK, "OK")
}
