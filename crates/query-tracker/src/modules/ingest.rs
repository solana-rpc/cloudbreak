// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Ingest — demand in.
//!
//! Turns a batch of wire [`TrackObservation`](cloudbreak_core::modules::query_tracker_api::TrackObservation)s (from the API client) into
//! durable demand in the [`Store`]. Each observation is resolved to an
//! [`IndexIdentity`]; non-indexable ones (token program, no usable filters,
//! unparseable pubkey) are counted as skipped. Everything else folds into the
//! pattern's row (demand/cost/failures + variety fingerprints).
//!
//! A database error aborts the batch with an error so the caller can signal the
//! client to keep the data and retry — the client must never silently lose
//! demand because a flush failed.

use crate::modules::store::Store;
use crate::stats::metrics;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use cloudbreak_core::modules::query_tracker_api::{TrackBatch, TrackResponse};
use sea_orm::DbErr;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use tracing::error;

pub async fn apply_batch(store: &Store, batch: TrackBatch) -> Result<TrackResponse, DbErr> {
    let mut accepted = 0usize;
    let mut skipped = 0usize;

    for obs in batch.observations {
        let program = match Pubkey::from_str(&obs.program) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    target: "query_tracker_ingest",
                    "skipping observation with invalid program '{}': {e}", obs.program
                );
                skipped += 1;
                continue;
            }
        };

        let Some(identity) = IndexIdentity::parse(program, obs.config.as_ref()) else {
            // Not indexable (token program / no index-defining filters).
            skipped += 1;
            continue;
        };

        // Keep the representative request as an example for the row (used by the
        // EXPLAIN probe and shown on the debug endpoints). Best-effort: a
        // serialization failure just means no example, never a lost observation.
        let example_request = obs
            .config
            .as_ref()
            .and_then(|c| serde_json::to_value(c).ok());

        store
            .record_demand(
                &identity,
                obs.count,
                obs.total_cost_us,
                obs.failed_count,
                &obs.value_fingerprints,
                example_request,
            )
            .await?;

        accepted += 1;
    }

    metrics::OBSERVATIONS_TOTAL.inc_by(accepted as u64);
    Ok(TrackResponse { accepted, skipped })
}
