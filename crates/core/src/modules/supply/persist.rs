// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The write path and the `from_config` constructor. `from_config` validates the
//! node requirements (owner map off, owner partitioning off, snapshot present,
//! empty programs filter, the stake and pubkey indexes), clears a prior run's
//! rows, and pre-sizes the cache. `persist_supply_row` is the single upsert used
//! by the seed, the bootstrap resolve, and the block path.

use crate::modules::supply::tracker::{SUPPLY_RING_SLOTS, SupplyCommit, SupplyTracker};
use crate::{IndexConfig, metrics};
use rust_decimal::Decimal;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value,
};
use std::time::Duration;

/// Pre-sized bucket target for the hot-accounts map: 2^22 buckets hold the ~2.55M
/// peak (1.1M unpinned cap plus ~1.45M pinned stake) at the 7/8 load factor.
const SUPPLY_CACHE_BUCKETS: usize = 1 << 22;

/// Failure-pin budget: the number of live-write-failure accounts pinned before
/// the tracker fails closed. About 200k entries, ~13 MB.
const SUPPLY_FAIL_PIN_CAP: usize = 200_000;

/// Builds the tracker from config. Returns the disabled handle when the `[supply]`
/// section is absent or off. When on it panics on any unmet node requirement, so
/// a misconfigured node never serves a wrong total.
pub async fn from_config(db: &DatabaseConnection, config: &IndexConfig) -> SupplyTracker {
    let Some(supply) = config.supply.as_ref().filter(|s| s.enabled) else {
        return SupplyTracker::default();
    };

    // Config-level requirements.
    if !config.programs.supports_simulation() {
        panic!("[supply] requires an empty [programs] filter so every account write is counted");
    }
    let Some(snapshot) = config.snapshot.as_ref() else {
        panic!("[supply] requires the [snapshot] section for the capitalization anchor");
    };
    if config.accounts_owner_map_enabled {
        panic!(
            "[supply] requires accounts-owner-map-enabled = false; the hot cache replaces the owner map"
        );
    }
    if !snapshot.pg_indexes.idx_snapshot_accounts_pubkey_slot {
        panic!("[supply] requires snapshot pg-indexes idx-snapshot-accounts-pubkey-slot = true");
    }
    if !snapshot.pg_indexes.idx_snapshot_accounts_stake_owner {
        panic!("[supply] requires snapshot pg-indexes idx-snapshot-accounts-stake-owner = true");
    }

    // Catalog requirements: the indexer config has no partitioning knob, so the
    // catalog is the truth. Both tables must be plain (relkind 'r', not 'p'), and
    // the pubkey-slot and stake-owner indexes must exist on `accounts`.
    for table in ["accounts", "snapshot_accounts"] {
        let relkind = table_relkind(db, table).await;
        match relkind.as_deref() {
            Some("r") => {}
            Some("p") => panic!(
                "[supply] requires {table} to be de-partitioned (owner partitioning off); found a partitioned table"
            ),
            other => panic!("[supply] could not read relkind for {table}: {other:?}"),
        }
    }
    for index in ["idx_accounts_pubkey_slot", "idx_accounts_stake_owner"] {
        if !index_exists(db, index).await {
            panic!("[supply] requires index {index} on accounts; create it via the migration flags");
        }
    }

    // Clear a prior run's rows so a stale total is never served before the seed.
    clear_prior_run(db).await;

    let tracker = SupplyTracker::new(
        SUPPLY_CACHE_BUCKETS,
        supply.hot_accounts,
        SUPPLY_FAIL_PIN_CAP,
        supply.pin_stake_accounts,
    );
    tracing::info!(
        target: "supply_tracker",
        "supply enabled: cache pre-sized to {} buckets, unpinned cap {}, pin-stake {}",
        SUPPLY_CACHE_BUCKETS,
        supply.hot_accounts,
        supply.pin_stake_accounts
    );
    tracker
}

async fn table_relkind(db: &DatabaseConnection, table: &str) -> Option<String> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT relkind::text AS relkind FROM pg_class \
             WHERE relname = $1 AND relnamespace = 'public'::regnamespace",
            [Value::from(table)],
        ))
        .await
        .ok()
        .flatten()?;
    row.try_get::<String>("", "relkind").ok()
}

async fn index_exists(db: &DatabaseConnection, index: &str) -> bool {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT 1 AS ok FROM pg_class WHERE relname = $1 AND relkind = 'i'",
        [Value::from(index)],
    ))
    .await
    .ok()
    .flatten()
    .is_some()
}

async fn clear_prior_run(db: &DatabaseConnection) {
    if let Err(e) = db
        .execute(Statement::from_string(
            DatabaseBackend::Postgres,
            "DELETE FROM supply; DELETE FROM non_circulating_accounts WHERE id = 1".to_string(),
        ))
        .await
    {
        tracing::error!(target: "supply_tracker", "failed to clear prior supply rows: {:?}", e);
    }
}

/// Upserts one supply row and prunes the ring. Used by the snapshot seed
/// (`non_circulating` None), the bootstrap resolve, and every committed block.
pub async fn persist_supply_row(
    db: &DatabaseConnection,
    commit: &SupplyCommit,
    query_timeout: Duration,
) {
    let statement = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "WITH upsert AS ( \
            INSERT INTO supply (slot, total, non_circulating_lamports) VALUES ($1, $2, $3) \
            ON CONFLICT (slot) DO UPDATE SET \
                total = EXCLUDED.total, \
                non_circulating_lamports = EXCLUDED.non_circulating_lamports, \
                updated_at = now() \
         ) \
         DELETE FROM supply WHERE slot < $4",
        [
            Value::from(commit.slot as i64),
            Value::from(Decimal::from(commit.total)),
            Value::from(commit.non_circulating.map(Decimal::from)),
            Value::from(commit.slot.saturating_sub(SUPPLY_RING_SLOTS) as i64),
        ],
    );
    let result = tokio::time::timeout(query_timeout, db.execute(statement))
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!(target: "supply_tracker", "persist_supply_row timeout: {}", elapsed);
            Err(sea_orm::DbErr::RecordNotInserted)
        });
    if let Err(e) = result {
        tracing::error!(target: "supply_tracker", "persist_supply_row failed for slot {}: {}", commit.slot, e);
        metrics::SUPPLY_QUERY_ERRORS.inc();
    }
}
