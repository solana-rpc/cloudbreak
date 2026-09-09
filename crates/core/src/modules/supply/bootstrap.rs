// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The anchor seed and the two-pass bootstrap resolve. The seed sets the
//! capitalization anchor. The resolve reads each startup touch's balance at or
//! below the anchor slot by pubkey from `snapshot_accounts`, folds the window
//! delta onto the anchor, and flips the tracker Live. Pass two runs under the
//! block-writes lock so no live touch races the flip.

use crate::modules::supply::SEED_TIMEOUT;
use crate::modules::supply::persist::persist_supply_row;
use crate::modules::supply::prev::fetch_startup_balances;
use crate::modules::supply::tracker::{SupplyCommit, SupplyTracker};
use sea_orm::DatabaseConnection;
use solana_pubkey::Pubkey;

/// Anchors the total on the snapshot bank capitalization and persists it as the
/// first supply row. No-op when the tracker is disabled.
pub async fn seed_anchor(
    db: &DatabaseConnection,
    tracker: &SupplyTracker,
    slot: u64,
    capitalization: u64,
) {
    if !tracker.is_enabled() {
        return;
    }
    tracker.set_startup_total(slot, capitalization);
    let seed = SupplyCommit {
        slot,
        total: capitalization,
        non_circulating: None,
    };
    persist_supply_row(db, &seed, SEED_TIMEOUT).await;
}

/// Resolves the startup balances and flips the tracker Live. A read failure or an
/// unresolved account leaves the tracker Bootstrapping, so it never publishes on
/// top of a hole.
pub async fn finish_bootstrap(db: &DatabaseConnection, tracker: &SupplyTracker) {
    let Some(startup_slot) = tracker.startup_slot() else {
        return;
    };
    if tracker.bootstrap_failed() {
        tracing::error!(
            target: "supply_bootstrap",
            "supply bootstrap poisoned by failed startup account writes, supply stays bootstrapping"
        );
        return;
    }

    let start = tokio::time::Instant::now();
    let touched = tracker.startup_touched_pubkeys();
    let touched_count = touched.len();

    let mut balances = match resolve(db, &touched, startup_slot).await {
        Ok(balances) => balances,
        Err(e) => {
            log_error(e);
            return;
        }
    };

    // Pass two under the lock: pick up any touch recorded after pass one.
    let _write_guard = tracker.lock_block_writes().await;
    let late: Vec<Pubkey> = tracker
        .startup_touched_pubkeys()
        .into_iter()
        .filter(|pubkey| !balances.contains_key(pubkey))
        .collect();
    match resolve(db, &late, startup_slot).await {
        Ok(late_balances) => balances.extend(late_balances),
        Err(e) => {
            log_error(e);
            return;
        }
    }

    let Some(commit) = tracker.finish_bootstrap(&balances) else {
        if tracker.bootstrap_failed() {
            tracing::error!(target: "supply_bootstrap", "supply bootstrap poisoned, stays bootstrapping");
        } else {
            tracing::error!(target: "supply_bootstrap", "supply bootstrap left unresolved accounts, stays bootstrapping");
        }
        return;
    };

    tracing::info!(
        target: "supply_bootstrap",
        "supply bootstrap reconciled {} touched accounts against startup slot {} in {} secs - slot: {}, total: {}",
        touched_count,
        startup_slot,
        start.elapsed().as_secs_f64(),
        commit.slot,
        commit.total
    );

    persist_supply_row(db, &commit, SEED_TIMEOUT).await;
}

const RESOLVE_CHUNK: usize = 5_000;

async fn resolve(
    db: &DatabaseConnection,
    pubkeys: &[Pubkey],
    startup_slot: u64,
) -> Result<std::collections::HashMap<Pubkey, u64>, sea_orm::DbErr> {
    let mut out = std::collections::HashMap::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(RESOLVE_CHUNK) {
        let resolved = fetch_startup_balances(db, chunk, startup_slot, SEED_TIMEOUT).await?;
        out.extend(resolved);
    }
    Ok(out)
}

fn log_error(e: impl std::fmt::Debug) {
    tracing::error!(
        target: "supply_bootstrap",
        "failed to resolve startup balances, supply stays bootstrapping: {:?}",
        e
    );
}
