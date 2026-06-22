// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    Statement, Value,
    prelude::Expr,
    sea_query::{Alias, OnConflict},
};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout},
};
use yellowstone_grpc_proto::{geyser::CommitmentLevel, prelude::UnixTimestamp};
use cloudbreak_core::{IndexConfig, modules::account_owner_map::AccountOwnerMap};
use cloudbreak_entity::{accounts, service_health, slots};

use crate::metrics;

/// The service health is set to healthy when the snapshot is processed successfully. And
///  to unhealthy when we get a slot gap.
///
/// By default we set the service as unhealthy on migration.
pub async fn update_service_health(db: &DatabaseConnection, healthy: bool) {
    let query = service_health::Entity::insert(service_health::ActiveModel {
        id: Set(1), //It will always write to the one default record
        healthy: Set(healthy),
        last_updated_at: NotSet,
    })
    .on_conflict(
        OnConflict::columns([service_health::Column::Id])
            .update_columns([
                service_health::Column::Healthy,
                service_health::Column::LastUpdatedAt,
            ])
            .to_owned(),
    )
    .exec_without_returning(db);

    let result = timeout(Duration::from_secs(30), query)
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!("update_service_health timeout ERROR: {}", elapsed);
            metrics::increment_db_errors();
            Err(sea_orm::DbErr::RecordNotInserted)
        });

    match result {
        Ok(result) => {
            tracing::debug!("update_service_health: updated service health: {}", result);
        }
        Err(e) => {
            tracing::error!(
                "update_service_health: failed to update service health: {}",
                e
            );
            metrics::increment_db_errors();
        }
    }
}

const INSERT_CLOSED_ACCOUNTS_BATCH_SIZE: usize = 500;

/// Inserts the closed accounts into the "accounts" table.
///
/// This is used to insert the closed accounts with empty data, 0 lamports and the current slot. But using the previous owner, so that
/// it can provide and out of the box mask, for closed accounts at the confirmed commitment level(just checking lamports > 0).
///
/// Note: If the `AccountOwnerMap` is enabled, the closed accounts will be saved using the `AccountOwnerMap` instead of the database scan.
pub fn insert_closed_accounts(
    db: DatabaseConnection,
    pubkeys: Vec<Vec<u8>>,
    slot: u64,
    config: &IndexConfig,
    accounts_owner_map: AccountOwnerMap,
) -> Option<JoinHandle<()>> {
    let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);

    let handle = tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("insert_closed_accounts");

        let start_time = Instant::now();

        if accounts_owner_map.is_enabled() {
            let result = accounts_owner_map.save_closed_accounts(pubkeys, slot).await;
            match result {
                Ok(res) => {
                    tracing::debug!("saved {} closed accounts", res.rows_affected());
                }
                Err(e) => {
                    tracing::error!(target: "save_closed_accounts_with_map", "failed to save closed accounts with map: {}", e);
                    metrics::increment_db_errors();
                }
            }
        } else {
            let insert_closed_account_sql = include_str!("db/insertClosedAccount.sql");

            let batches = pubkeys
                .chunks(INSERT_CLOSED_ACCOUNTS_BATCH_SIZE)
                .map(|batch| batch.to_vec())
                .collect::<Vec<_>>();
            for batch in batches {
                let query = db.execute(Statement::from_sql_and_values(
                    sea_orm::DatabaseBackend::Postgres,
                    insert_closed_account_sql,
                    vec![
                        Value::Array(
                            sea_orm::sea_query::ArrayType::Bytes,
                            Some(Box::new(
                                batch
                                    .into_iter()
                                    .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
                                    .collect(),
                            )),
                        ),
                        Value::BigInt(Some(slot as i64)),
                    ],
                ));

                let result = timeout(query_timeout, query)
                    .await
                    .unwrap_or_else(|elapsed| {
                        tracing::error!("insert_closed_accounts timeout ERROR: {}", elapsed);
                        metrics::increment_db_errors();
                        Err(sea_orm::DbErr::RecordNotInserted)
                    });

                match result {
                    Ok(res) => {
                        tracing::debug!(
                            target: "insert_closed_account",
                            "inserted {} closed accounts for slot {}",
                            res.rows_affected(),
                            slot
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "insert_closed_account: failed to insert closed accounts batch for slot {}: {}",
                            slot,
                            e
                        );
                        metrics::increment_db_errors();
                    }
                }
            }
        }

        metrics::INSERT_CLOSED_ACCOUNTS_PER_SLOT_HISTOGRAM
            .observe(start_time.elapsed().as_micros() as f64 / 1000.0);
    });

    Some(handle)
}

/// Deletes the last special "closed" version inserted for the set of closed accounts for the given slot
pub async fn cleanup_closed_accounts(
    db: &DatabaseConnection,
    pubkeys: Vec<Vec<u8>>,
    slot: u64,
    config: &IndexConfig,
) {
    let query_timeout = Duration::from_secs(config.database.finalize_slot_queries_timeout);

    let start_time = Instant::now();
    let cleanup_closed_accounts_sql = include_str!("db/closedAccountscleanup.sql");

    if pubkeys.is_empty() {
        return;
    }

    let query = db.execute(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        cleanup_closed_accounts_sql,
        vec![
            Value::Array(
                sea_orm::sea_query::ArrayType::Bytes,
                Some(Box::new(
                    pubkeys
                        .into_iter()
                        .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
                        .collect(),
                )),
            ),
            Value::BigInt(Some(slot as i64)),
        ],
    ));

    let result = timeout(query_timeout, query)
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!("cleanup_closed_accounts timeout ERROR: {}", elapsed);
            metrics::increment_db_errors();
            Err(sea_orm::DbErr::RecordNotInserted)
        });

    match result {
        Ok(res) => {
            tracing::debug!(
                target: "cleanup_closed_accounts",
                "cleaned up {} closed accounts for slot {}",
                res.rows_affected(),
                slot
            );
        }
        Err(e) => {
            tracing::error!(
                "cleanup_closed_accounts: failed to cleanup closed accounts for slot {}: {}",
                slot,
                e
            );
            metrics::increment_db_errors();
        }
    }

    metrics::record_finalize_slot(
        start_time.elapsed().as_secs_f64(),
        "cleanup_closed_accounts",
    );
}

/// Cleans up older versions (slot less than the received slot) of the accounts from the database (for the given table)
pub async fn cleanup_accounts(
    db: &DatabaseConnection,
    pubkeys: Vec<Vec<u8>>,
    slot: u64,
    table_name: &str,
    new_accounts_in_slot: Arc<Mutex<usize>>,
    metrics_tag: &str,
    config: &IndexConfig,
) {
    let start_time = Instant::now();
    let pubkeys_len = pubkeys.len();
    let cleanup_sql = include_str!("db/cleanup.sql");
    let cleanup_sql = cleanup_sql.replace("accounts_table_name", table_name);
    let query_timeout = Duration::from_secs(config.database.finalize_slot_queries_timeout);

    let query = db.execute(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        cleanup_sql,
        vec![
            Value::BigInt(Some(slot as i64)),
            Value::Array(
                sea_orm::sea_query::ArrayType::Bytes,
                Some(Box::new(
                    pubkeys
                        .into_iter()
                        .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
                        .collect(),
                )),
            ),
        ],
    ));

    let result = timeout(query_timeout, query)
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!("cleanup_accounts timeout ERROR: {}", elapsed);
            metrics::increment_db_errors();
            Err(sea_orm::DbErr::RecordNotInserted)
        });

    match result {
        Ok(res) => {
            tracing::debug!(
                "finalize_slot: deleted {} old account versions for slot {} - accounts_in_batch: {}",
                res.rows_affected(),
                slot,
                pubkeys_len
            );

            let mut new_accounts_in_slot = new_accounts_in_slot
                .lock()
                .expect("Failed to lock new_accounts_in_slot");
            let deleted = res.rows_affected() as usize;

            metrics::FINALIZE_SLOT_DELETED_ACCOUNTS.observe(deleted as f64);

            if table_name != "snapshot_accounts" {
                if pubkeys_len >= deleted {
                    *new_accounts_in_slot += pubkeys_len - deleted;
                } else {
                    tracing::debug!(
                        target: "finalize_slot_debug",
                        "finalize_slot: deleted more accounts than the batch size({}) for {} - slot {}: {}",
                        pubkeys_len,
                        table_name,
                        slot,
                        deleted
                    );
                }
            }
        }
        Err(e) => {
            tracing::error!("finalize_slot: failed to finalize slot {}: {}", slot, e);
            metrics::increment_db_errors();
        }
    }

    metrics::record_finalize_slot(start_time.elapsed().as_secs_f64(), metrics_tag);
}

pub async fn insert_slot(
    slot: u64,
    block_time: Option<UnixTimestamp>,
    commitment: CommitmentLevel,
    db: &DatabaseConnection,
    config: &IndexConfig,
) {
    let query_timeout = Duration::from_secs(config.database.finalize_slot_queries_timeout);

    let block_time = block_time.unwrap_or_default().timestamp;

    let query = slots::Entity::insert(slots::ActiveModel {
        slot: Set(slot as i64),
        commitment: Set(commitment as i32),
        block_time: Set(block_time),
    })
    .on_conflict(
        OnConflict::columns([slots::Column::Commitment])
            .update_columns([slots::Column::Slot, slots::Column::BlockTime])
            .action_cond_where(
                Condition::all().add(
                    Expr::col((Alias::new("excluded"), slots::Column::Slot))
                        .gt(Expr::col((slots::Entity, slots::Column::Slot))),
                ),
            )
            .to_owned(),
    )
    .exec_without_returning(db);

    let result = timeout(query_timeout, query)
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!("insert_slot timeout ERROR: {}", elapsed);
            metrics::increment_db_errors();
            Err(sea_orm::DbErr::RecordNotInserted)
        });

    match result {
        Ok(res) => tracing::debug!("insert_slot: inserted slot {}", res),
        Err(e) => {
            tracing::error!("insert_slot: failed to insert slot {}: {}", slot, e);
            metrics::increment_db_errors();
        }
    }
}

/// The latest persisted slot for each commitment level, plus the finalized→confirmed lag.
///
/// The `slots` table holds exactly one row per commitment (its primary key), updated to the
/// highest slot seen, so each value is a single point lookup.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct ChainTips {
    pub confirmed_slot: Option<u64>,
    pub finalized_slot: Option<u64>,
    /// `confirmed_slot - finalized_slot` (how many slots finalized lags behind confirmed).
    pub finalized_behind_confirmed: Option<u64>,
}

/// Reads the last confirmed/finalized slots from the `slots` table (best-effort: DB errors and
/// missing rows surface as `None`).
///
/// Both commitments are fetched in a single round-trip (`WHERE commitment IN (confirmed,
/// finalized)`) and then split out of the returned rows.
pub async fn get_chain_tips(db: &DatabaseConnection) -> ChainTips {
    let rows = slots::Entity::find()
        .filter(slots::Column::Commitment.is_in([
            CommitmentLevel::Confirmed as i32,
            CommitmentLevel::Finalized as i32,
        ]))
        .all(db)
        .await
        .unwrap_or_default();

    let slot_for = |commitment: CommitmentLevel| {
        rows.iter()
            .find(|model| model.commitment == commitment as i32)
            .map(|model| model.slot as u64)
    };
    let confirmed_slot = slot_for(CommitmentLevel::Confirmed);
    let finalized_slot = slot_for(CommitmentLevel::Finalized);
    let finalized_behind_confirmed = match (confirmed_slot, finalized_slot) {
        (Some(confirmed), Some(finalized)) => Some(confirmed.saturating_sub(finalized)),
        _ => None,
    };
    ChainTips {
        confirmed_slot,
        finalized_slot,
        finalized_behind_confirmed,
    }
}

pub async fn insert_accounts_chunk(
    db: &DatabaseConnection,
    chunk: Vec<accounts::ActiveModel>,
    byte_size: usize,
    config: &IndexConfig,
) {
    let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);

    let start_time = Instant::now();
    let chunk_len = chunk.len();

    let result = timeout(
        query_timeout,
        accounts::Entity::insert_many(chunk).exec_without_returning(db),
    )
    .await
    .unwrap_or_else(|elapsed| {
        tracing::error!("upsert_accounts_batched timeout ERROR: {}", elapsed);
        metrics::increment_db_errors();
        Err(sea_orm::DbErr::RecordNotInserted)
    });

    match result {
        Ok(res) => tracing::debug!("upsert_accounts_batched: {}", res),
        Err(e) => {
            tracing::error!("upsert_accounts_batched ERROR: {}", e);
            metrics::increment_db_errors();
        }
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    if elapsed > 0.250 {
        tracing::debug!(target: "slow_chunk", "slow chunk: len: {}, size: {}", chunk_len, byte_size);
    }
    metrics::record_chunk_processing(elapsed, "block");
}
