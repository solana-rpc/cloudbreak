// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Functional endpoints — one file per endpoint (filename = endpoint path).
//!
//! These are the tracker's business API (the reason the service exists).
//! Introspection (`/debug/*`) and ops (`/metrics`, `/health`) live under
//! `operational/` instead.
//!
//! - `track` — `POST /track` (ingest).

pub mod track;
