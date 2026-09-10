// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The write path and the `from_config` constructor. `from_config` validates the
//! node requirements (owner map off, owner partitioning off, snapshot present,
//! empty programs filter, the pubkey indexes), clears a prior run's rows, and
//! pre-sizes the cache. `persist_supply_row` is the single upsert used by the
//! seed, the bootstrap resolve, and the block path.

use crate::IndexConfig;
use crate::metrics::SUPPLY_DB_MICROSECONDS;
use crate::modules::non_circulating::NonCirculatingTracker;
use crate::modules::supply::tracker::{SUPPLY_RING_SLOTS, SupplyCommit, SupplyTracker};
use rust_decimal::Decimal;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value};
use std::time::Duration;
use tokio::time::Instant;

/// Failure-pin budget: the number of live-write-failure accounts pinned before
/// the tracker fails closed. About 200k entries, ~13 MB.
const SUPPLY_FAIL_PIN_CAP: usize = 200_000;

/// Builds the tracker from config. Returns the disabled handle when the `[supply]`
/// section is absent or off. When on it panics on any unmet node requirement, so
/// a misconfigured node never serves a wrong total.
pub async fn from_config(
    db: &DatabaseConnection,
    config: &IndexConfig,
    non_circulating: NonCirculatingTracker,
) -> SupplyTracker {
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

    // Catalog requirements. The indexer config has no partitioning knob, so the
    // catalog is the truth for the table kinds and the `accounts` index.
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
    if !index_exists(db, "idx_accounts_pubkey_slot").await {
        panic!(
            "[supply] requires index idx_accounts_pubkey_slot on accounts; create it via the migration flags"
        );
    }

    // Clear a prior run's rows so a stale total is never served before the seed.
    if let Err(e) = db.execute_unprepared("DELETE FROM supply").await {
        tracing::error!(target: "supply_tracker", "failed to clear prior supply rows: {:?}", e);
    }

    let tracker = SupplyTracker::new(
        db.clone(),
        Duration::from_secs(config.database.save_block_queries_timeout),
        supply.hot_accounts,
        SUPPLY_FAIL_PIN_CAP,
        non_circulating,
    );
    tracing::info!(
        target: "supply_tracker",
        "supply enabled: cache pre-sized for {} accounts, capacity {}",
        supply.hot_accounts,
        tracker.summary().map_or(0, |summary| summary.capacity)
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
    let started = Instant::now();
    let result = tokio::time::timeout(query_timeout, db.execute(statement))
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!(target: "supply_tracker", "persist_supply_row timeout: {}", elapsed);
            Err(sea_orm::DbErr::RecordNotInserted)
        });
    SUPPLY_DB_MICROSECONDS
        .with_label_values(&["persist_row"])
        .observe(started.elapsed().as_micros() as f64);
    if let Err(e) = result {
        tracing::error!(target: "supply_tracker", "persist_supply_row failed for slot {}: {}", commit.slot, e);
    }
}
