// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Query tracker service.
//!
//! Persistence-first, demand-driven auto-indexer. All decision state lives in
//! the `index_patterns` table (see the migration), so the service is fully
//! restartable and never loses history. The crate is organised as:
//!
//! - [`server`] — HTTP edge: ingest (`POST /track`), debug (`GET /debug/*`),
//!   `GET /metrics` and `GET /health`.
//! - [`modules`] — the core demand → index pipeline: `store` (the only SQL, plus
//!   prioritization), `ingest`, `indexer_backpressure`, `creation`, `eviction`.
//! - [`stats`] — observability/statistics that measure and explain rather than
//!   decide: `variety`, `discrepancy`, `explain`, `metrics`.
//!
//! [`run`] wires them together: it loads config, connects the DB, starts the
//! HTTP server, and spawns the background loops enabled by config.
//!
//! # Example: the path a `/track` request follows
//!
//! Suppose the API served a `getProgramAccounts` call filtered by
//! `(program, memcmp@offset)` and forwards it as one observation:
//!
//! 1. **Edge** — the batch hits [`server::endpoints::track`], which parses the
//!    JSON body into a `TrackBatch` and hands it to `ingest`. A DB error here
//!    returns `500` so the client keeps the batch and retries (demand is never
//!    silently dropped).
//! 2. **Ingest** — [`modules::ingest::apply_batch`] resolves each observation to
//!    an `IndexIdentity` (the canonical key from `cloudbreak-core`). Non-indexable
//!    ones (token program, no usable filters) are counted as *skipped*.
//! 3. **Store** — [`modules::store::Store::record_demand`] upserts the pattern's
//!    row in `index_patterns`: bumps demand/cost/failure counters, resurrects an
//!    `evicted` pattern back to `candidate`, and folds the request's value
//!    fingerprints into the per-index HyperLogLog sketch
//!    ([`stats::variety`]) so we can tell "one query hammered a million times"
//!    apart from "a million distinct queries".
//! 4. **Creation** — separately, the [`modules::creation`] loop wakes on its
//!    timer, checks [`modules::indexer_backpressure`] and the index cap, asks the
//!    store for the top candidates ranked by `PriorityMode`
//!    ([`modules::store::prioritization`]), and — if the program is allowed —
//!    issues the `CREATE INDEX` **pair** on `accounts` + `snapshot_accounts`,
//!    marking the pattern `created`. Below the fill target the top candidate is
//!    built freely; in the buffer band above it a **creation-time value guard**
//!    only builds a candidate that out-scores the index it would displace (or
//!    when nothing is reclaimable to displace, builds anyway up to the cap and
//!    lets eviction reclaim later), so the table climbs toward `max-auto-indexes`
//!    only for genuinely valuable indexes.
//! 5. **Eviction & stats** — the [`modules::eviction`] loop later refreshes
//!    supply (`idx_scan`) from Postgres, records the demand-vs-supply verdict
//!    ([`stats::discrepancy`]), updates per-index [`stats::metrics`], and may
//!    drop the idlest pairs. The opt-in [`stats::explain`] pass adds a third,
//!    planner-level signal.
//!
//!    A pair is only ever dropped when **all** of these hold (any one failing
//!    defers the drop, never loses the pattern):
//!    - `track_counts` is on — otherwise `idx_scan` is frozen and the whole pass
//!      is skipped so stale stats can't drive a drop;
//!    - a `max-auto-indexes` cap is configured *and* the capped table
//!      (`snapshot_accounts`) is at/above `eviction-fill-threshold` — below the
//!      fill line nothing is dropped. Once the count reaches `max-auto-indexes`,
//!      index creation is paused until eviction makes room again; all index
//!      candidates are kept queued in `index_patterns` with no loss of data;
//!    - the pair is demand-idle for `index-min-idle` (and supply-idle too when
//!      `use-supply-for-eviction` is on) and older than `index-min-age-grace`
//!      (demand-idle alone stops a still-demanded index from being dropped and
//!      avoids the drop→slow→rebuild churn loop);
//!    - the indexer is not under backpressure ([`modules::indexer_backpressure`]).
//!
//!    When above the fill target (`eviction-fill-threshold`), the least-valuable
//!    eligible pairs (ascending `PriorityMode` score) are dropped just until the
//!    table is back at the target — unconditionally, since entry into the buffer
//!    band was already value-gated at creation.

pub mod error;
pub mod modules;
pub mod server;
pub mod stats;

pub use error::{QueryTrackerError, QueryTrackerResult};

use crate::modules::store::Store;
use crate::server::AppState;
use cloudbreak_core::{QueryTrackerServiceConfig, TryLoadConfig};
use sea_orm::{ConnectOptions, Database};
use std::sync::Arc;
use tracing::info;

pub async fn run(config_path: &str) -> cloudbreak_core::Result<()> {
    let config = QueryTrackerServiceConfig::try_load(config_path)?;
    let server_addr = config.server_addr();

    stats::metrics::init();

    let database = Database::connect(ConnectOptions::from(config.database.clone())).await?;
    let store = Store::new(database);
    let qt = config.query_tracker.clone();

    info!(
        target: "query_tracker",
        "query tracker starting (create-indexes: {}, priority: {:?}, threshold: {}, eviction: {}, explain: {})",
        qt.create_database_indexes, qt.priority_mode, qt.index_generation_threshold,
        qt.index_eviction_enabled, qt.explain_enabled
    );

    if qt.create_database_indexes {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { modules::creation::run(store, cfg).await });
    } else {
        info!(target: "query_tracker", "index creation disabled; recording demand only");
    }

    if qt.index_eviction_enabled {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { modules::eviction::run(store, cfg).await });
    }

    if qt.priority_mode.uses_windowed_rate() {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { modules::score_roll::run(store, cfg).await });
    }

    if qt.explain_enabled {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { stats::explain::run(store, cfg).await });
    }

    let state = Arc::new(AppState { store, config: qt });
    tokio::spawn(async move { server::serve(server_addr, state).await });

    info!(target: "query_tracker", "query tracker running on http://{server_addr}. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    info!(target: "query_tracker", "shutdown signal received; stopping query tracker");

    Ok(())
}
