// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::convert::Infallible;

use cloudbreak_core::modules::account_owner_map::AccountOwnerMap;
use http_body_util::Full;
use hyper::{Response, body::Bytes};

/// `GET /debug/accounts_owner_map` — dumps the in-memory account→owner map.
pub(crate) fn handle() -> Result<Response<Full<Bytes>>, Infallible> {
    let body = AccountOwnerMap::debug_accounts_owner_map();

    Ok(Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}
