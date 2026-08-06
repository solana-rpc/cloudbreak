// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use cloudbreak_core::SnapshotPgIndexesConfig;
use cloudbreak_entity::snapshot_accounts::{self};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DatabaseConnection, EntityTrait, Statement,
    TransactionTrait, Value,
};
use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccount;

use crate::metrics;
use crate::stake_data::SnapshotStakeData;

pub const INSERT_SNAPSHOT_ACCOUNT_VERSIONS_TEMP_TABLE_BATCH_SIZE: usize = 1_000;

pub async fn upsert_accounts_batched(
    db: &DatabaseConnection,
    chunk: Vec<SubscribeUpdateAccount>,
) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let result = snapshot_accounts::Entity::insert_many(
        chunk
            .iter()
            .cloned()
            .map(|account_update| {
                let account = account_update
                    .account
                    .ok_or_else(|| anyhow::anyhow!("account is None"))?;

                if account.pubkey.is_empty() {
                    return Err(anyhow::anyhow!(
                        "pubkey is empty for account: {:?}",
                        account
                    ));
                }

                Ok(snapshot_accounts::ActiveModel {
                    pubkey: Set(account.pubkey),
                    owner: Set(account.owner),
                    lamports: Set(account.lamports as i64),
                    slot: Set(account_update.slot as i64),
                    executable: Set(account.executable),
                    rent_epoch: Set(Decimal::from(account.rent_epoch)),
                    data: Set(account.data),
                    write_version: Set(account.write_version as i64),
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?,
    )
    .exec_without_returning(db)
    .await;

    match result {
        Ok(result) => {
            tracing::debug!(target: "upsert_chunk", "upserted chunk - rows affected: {}", result);

            let elapsed = start_time.elapsed().as_secs_f64();
            metrics::record_snapshot_batch_insert_time(elapsed);
        }
        Err(e) => {
            tracing::error!("failed to upsert chunk: {}", e);
            metrics::increment_db_snapshot_errors();
            return Err(anyhow::anyhow!("failed to upsert chunk: {}", e));
        }
    }

    Ok(())
}

/// We create the table indexes after the snapshot if processed (instead on migration) for performance reasons.
/// Each index is gated by the corresponding flag in `[pg-indexes]` from the snapshot config.
pub async fn create_database_indexes(
    db: &DatabaseConnection,
    cfg: &SnapshotPgIndexesConfig,
) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    tracing::info!(target: "create_database_indexes", ?cfg, "start creating database indexes");

    if cfg.idx_snapshot_accounts_pubkey {
        db.execute_unprepared(
            r#"
                CREATE INDEX idx_snapshot_accounts_pubkey ON snapshot_accounts USING HASH (pubkey);
            "#,
        )
        .await?;
        tracing::info!(target: "create_database_indexes", "created idx_snapshot_accounts_pubkey (hash) in {} seconds (total accumulated)", start_time.elapsed().as_secs_f64());
    }

    if cfg.idx_snapshot_accounts_token_mint {
        db.execute_unprepared(
            r#"
                CREATE INDEX idx_snapshot_accounts_token_mint
                ON snapshot_accounts (token_mint)
                WHERE owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea
                OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea;
            "#
        )
        .await?;
        tracing::info!(target: "create_database_indexes", "created idx_snapshot_accounts_token_mint in {} seconds (total accumulated)", start_time.elapsed().as_secs_f64());
    }

    if cfg.idx_snapshot_accounts_token_owner {
        db.execute_unprepared(
            r#"
                CREATE INDEX idx_snapshot_accounts_token_owner
                ON snapshot_accounts (token_owner)
                WHERE owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea
                OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea;
            "#
        )
        .await?;
        tracing::info!(target: "create_database_indexes", "created idx_snapshot_accounts_token_owner in {} seconds (total accumulated)", start_time.elapsed().as_secs_f64());
    }

    if cfg.idx_snapshot_accounts_pubkey_slot {
        // Used for the insertClosedAccount query (for looking for the latest version of the account)
        db.execute_unprepared(
            r#"
                CREATE INDEX idx_snapshot_accounts_pubkey_slot ON snapshot_accounts (pubkey, slot DESC);
            "#,
        )
        .await?;
        tracing::info!(target: "create_database_indexes", "created idx_snapshot_accounts_pubkey_slot in {} seconds (total accumulated)", start_time.elapsed().as_secs_f64());
    }

    if cfg.idx_snapshot_accounts_token_delegate {
        db.execute_unprepared(
            r#"
                CREATE INDEX idx_snapshot_accounts_token_delegate
                ON snapshot_accounts (SUBSTRING(data FROM 77 FOR 32))
                WHERE (owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea
                    OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea)
                AND SUBSTRING(data FROM 73 FOR 1) = '\x01'::bytea;
            "#
        )
        .await?;
        tracing::info!(target: "create_database_indexes", "created idx_snapshot_accounts_token_delegate in {} seconds (total accumulated)", start_time.elapsed().as_secs_f64());
    }

    tracing::info!(target: "create_database_indexes", "finished creating database indexes in {} seconds", start_time.elapsed().as_secs_f64());

    Ok(())
}

pub async fn clean_up_closed_accounts(database: &DatabaseConnection) -> Result<(), anyhow::Error> {
    let sql = include_str!("db/cleanup_closed_accounts.sql");

    tracing::info!(target: "clean_up_closed_accounts", "start cleaning up closed accounts");
    let start_time = Instant::now();
    let rows = database.execute_unprepared(sql).await?;

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "clean_up_closed_accounts", "cleaned up {} closed accounts in {} seconds", rows.rows_affected(), elapsed);

    Ok(())
}

pub async fn create_temp_snapshot_account_versions_table(
    database: &DatabaseConnection,
) -> Result<(), anyhow::Error> {
    tracing::info!(target: "create_temp_snapshot_account_versions_table", "start creating temp snapshot account versions table");

    let sql = "CREATE UNLOGGED TABLE IF NOT EXISTS temp_snapshot_account_versions (pubkey BYTEA NOT NULL, slot BIGINT NOT NULL, owner BYTEA NOT NULL);";

    let result = database.execute_unprepared(sql).await;

    match result {
        Ok(result) => {
            tracing::info!(target: "create_temp_snapshot_account_versions_table", "create_temp_snapshot_account_versions_table: {}", result.rows_affected());
        }
        Err(e) => {
            tracing::error!("failed to create temp closed accounts table: {}", e);
            metrics::increment_db_snapshot_errors();
            return Err(anyhow::anyhow!(
                "failed to create temp snapshot account versions table: {}",
                e
            ));
        }
    }

    Ok(())
}

pub struct SnapshotAccountVersion {
    pub pubkey: Vec<u8>,
    pub slot: u64,
    pub owner: Vec<u8>,
}

pub async fn insert_into_temp_snapshot_account_versions(
    database: &DatabaseConnection,
    snapshot_account_versions: Vec<SnapshotAccountVersion>,
) -> Result<(), anyhow::Error> {
    let sql = include_str!("db/insert_into_snapshot_account_versions.sql");
    let start_time = Instant::now();

    let result = database
        .execute(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            sql,
            vec![
                Value::Array(
                    sea_orm::sea_query::ArrayType::Bytes,
                    Some(Box::new(
                        snapshot_account_versions
                            .iter()
                            .map(|snapshot_account_version| {
                                Value::Bytes(Some(Box::new(
                                    snapshot_account_version.pubkey.clone(),
                                )))
                            })
                            .collect(),
                    )),
                ),
                Value::Array(
                    sea_orm::sea_query::ArrayType::BigInt,
                    Some(Box::new(
                        snapshot_account_versions
                            .iter()
                            .map(|snapshot_account_version| {
                                Value::BigInt(Some(snapshot_account_version.slot as i64))
                            })
                            .collect(),
                    )),
                ),
                Value::Array(
                    sea_orm::sea_query::ArrayType::Bytes,
                    Some(Box::new(
                        snapshot_account_versions
                            .iter()
                            .map(|snapshot_account_version| {
                                Value::Bytes(Some(Box::new(snapshot_account_version.owner.clone())))
                            })
                            .collect(),
                    )),
                ),
            ],
        ))
        .await;

    match result {
        Ok(result) => {
            tracing::debug!(target: "insert_into_snapshot_account_versions", "inserted into snapshot account versions: {}", result.rows_affected());
            metrics::SNAPSHOT_CLEAN_UP_DUPLICATED_ACCOUNTS_BATCH_TIME
                .observe(start_time.elapsed().as_secs_f64());
        }
        Err(e) => {
            tracing::error!(
                "failed to insert into temp snapshot account versions: {}",
                e
            );
            metrics::increment_db_snapshot_errors();

            return Err(anyhow::anyhow!(
                "failed to insert into temp snapshot account versions: {}",
                e
            ));
        }
    }

    Ok(())
}

pub async fn clean_up_duplicated_accounts(
    database: &DatabaseConnection,
) -> Result<(), anyhow::Error> {
    add_primary_key_to_temp_snapshot_account_versions_table(database).await?;
    create_accounts_to_delete_table(database).await?;

    let rows_affected = execute_cleanup_duplicated_accounts_tx(database)
        .await
        .inspect_err(|_| {
            metrics::increment_db_snapshot_errors();
        })?;

    tracing::info!(target: "clean_up_duplicated_accounts", "cleaned up {} duplicated accounts", rows_affected);

    Ok(())
}

pub async fn add_primary_key_to_temp_snapshot_account_versions_table(
    database: &DatabaseConnection,
) -> Result<(), anyhow::Error> {
    tracing::info!(target: "add_primary_key_to_temp_snapshot_account_versions_table", "start adding primary key to temp snapshot account versions table");
    let start_time = Instant::now();

    database
        .execute_unprepared(
            "ALTER TABLE temp_snapshot_account_versions ADD PRIMARY KEY (pubkey, slot);",
        )
        .await
        .inspect_err(|_| {
            metrics::increment_db_snapshot_errors();
        })?;

    tracing::info!(target: "clean_up_duplicated_accounts", "added primary key to temp snapshot account versions table in {} seconds", start_time.elapsed().as_secs_f64());

    Ok(())
}

pub async fn execute_cleanup_duplicated_accounts_tx(
    database: &DatabaseConnection,
) -> Result<u64, sea_orm::DbErr> {
    tracing::info!(target: "execute_cleanup_duplicated_accounts_tx", "start executing cleanup duplicated accounts transaction");
    let sql = include_str!("db/cleanup_duplicated_accounts.sql");

    let txn = database.begin().await?;

    let start_time = Instant::now();

    // This is done to force postgres to use the primary key for the cleanup query, to enable partition pruning
    txn.execute_unprepared("SET LOCAL enable_hashjoin = off")
        .await?;
    txn.execute_unprepared("SET LOCAL enable_mergejoin = off")
        .await?;

    tracing::info!(target: "execute_cleanup_duplicated_accounts_tx", "executed SET LOCAL enable_hashjoin = off and SET LOCAL enable_mergejoin = off in {} seconds", start_time.elapsed().as_secs_f64());

    let result = txn.execute_unprepared(sql).await?;

    tracing::info!(target: "execute_cleanup_duplicated_accounts_tx", "executed cleanup duplicated accounts query in {} seconds", start_time.elapsed().as_secs_f64());

    txn.commit().await?;

    tracing::info!(target: "execute_cleanup_duplicated_accounts_tx", "committed transaction in {} seconds", start_time.elapsed().as_secs_f64());

    Ok(result.rows_affected())
}

pub async fn create_accounts_to_delete_table(
    database: &DatabaseConnection,
) -> Result<(), anyhow::Error> {
    tracing::info!(target: "create_accounts_to_delete_table", "start creating accounts to delete table");

    let sql = include_str!("db/create_accounts_to_delete_table.sql");
    let start_time = Instant::now();

    let rows = database.execute_unprepared(sql).await.inspect_err(|_| {
        metrics::increment_db_snapshot_errors();
    })?;

    tracing::info!(target: "create_accounts_to_delete_table", "created accounts to delete table in {} seconds - rows affected: {}", start_time.elapsed().as_secs_f64(), rows.rows_affected());

    Ok(())
}

/// Clusters the snapshot accounts hash partitions (`snapshot_accounts_pN`) discovered from the
/// database. Waits if there are elements in the buffer to avoid overloading the DB. Partitions
/// larger than the threshold are skipped.
///
/// Only hash sub-partitions (named `snapshot_accounts_pN`) are clustered here — LIST partitions
/// for individual programs are not, by design.
pub async fn cluster_snapshot_accounts_table(
    database: &DatabaseConnection,
    buffer_size: Arc<Mutex<usize>>,
    partition_clustering_threshold: Option<u64>,
) -> Result<(), anyhow::Error> {
    tracing::info!(target: "cluster_snapshot_accounts_table", "start clustering snapshot accounts table");

    let partition_sizes = get_snapshot_accounts_partition_sizes(database).await?;

    // Filter for hash-partition naming pattern `snapshot_accounts_p<digits>` so we only cluster
    // those (and not the LIST partitions which use base58 program names).
    let mut hash_partitions: Vec<(&String, &u64)> = partition_sizes
        .iter()
        .filter(|(name, _)| is_hash_partition_name(name))
        .collect();
    hash_partitions.sort_by_key(|(name, _)| (*name).clone());

    tracing::info!(target: "cluster_snapshot_accounts_table", "discovered {} hash partitions to consider for clustering", hash_partitions.len());

    let start_time = Instant::now();
    let mut last_print_time = Instant::now();

    for (idx, (partition_name, partition_size)) in hash_partitions.iter().enumerate() {
        if let Some(partition_clustering_threshold) = partition_clustering_threshold
            && **partition_size > partition_clustering_threshold
        {
            tracing::info!(target: "cluster_snapshot_accounts_table", "skipping clustering for partition {partition_name} because it's too large ({} bytes)", partition_size);
            continue;
        }

        let sql = format!("CLUSTER {partition_name} USING {partition_name}_pkey;");

        // wait if there are elements in the buffer to avoid overloading the DB
        // todo: make this configurable
        while *buffer_size.lock().expect("Failed to lock buffer_size") > 5 {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        database.execute_unprepared(&sql).await.inspect_err(|e| {
            tracing::error!("failed to cluster snapshot accounts table: {}", e);
            metrics::increment_db_snapshot_errors();
        })?;

        if last_print_time.elapsed().as_secs() > 60 {
            tracing::info!(target: "cluster_snapshot_accounts_table", "executed cluster snapshot accounts table - partition {} ({}) in {} seconds (total accumulated)", idx, partition_name, start_time.elapsed().as_secs_f64());
            last_print_time = Instant::now();
        }
    }

    Ok(())
}

/// Matches the `snapshot_accounts_p<digits>` naming convention used for hash partitions.
fn is_hash_partition_name(name: &str) -> bool {
    let Some(suffix) = name.strip_prefix("snapshot_accounts_p") else {
        return false;
    };
    !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
}

async fn get_snapshot_accounts_partition_sizes(
    database: &DatabaseConnection,
) -> Result<HashMap<String, u64>, anyhow::Error> {
    let sql = r#"
    WITH RECURSIVE partition_tree AS (
        SELECT 
            c.oid,
            c.relname as partition_name
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class p ON i.inhparent = p.oid
        WHERE p.relname = 'snapshot_accounts'
        
        UNION ALL
        
        SELECT 
            c.oid,
            c.relname as partition_name
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN partition_tree pt ON i.inhparent = pt.oid
    )
    SELECT 
        partition_name,
        pg_total_relation_size(oid) as size_bytes
    FROM partition_tree
    ORDER BY size_bytes DESC
    "#;

    let rows = database
        .query_all(Statement::from_string(
            sea_orm::DatabaseBackend::Postgres,
            sql.to_string(),
        ))
        .await?;

    let partition_sizes = rows
        .iter()
        .map(|row| {
            let name: String = row.try_get("", "partition_name")?;
            let size: i64 = row.try_get("", "size_bytes")?;
            Ok((name, size as u64))
        })
        .collect::<Result<HashMap<String, u64>, anyhow::Error>>()?;

    Ok(partition_sizes)
}

/// Upsert the per-voter stake rows extracted from a snapshot bank into the
/// `epoch_stakes` table and prune epochs older than (current - 2). The latest
/// epoch is read by the API on startup and via a poll loop.
pub async fn persist_epoch_stakes(
    db: &DatabaseConnection,
    data: &SnapshotStakeData,
) -> Result<(), anyhow::Error> {
    if data.voters.is_empty() {
        tracing::warn!(
            target: "persist_epoch_stakes",
            "snapshot stake data for epoch {} is empty; skipping",
            data.epoch
        );
        return Ok(());
    }

    let start_time = Instant::now();
    let txn = db.begin().await?;

    // Build a multi-row VALUES clause. Each row has 5 placeholders:
    // (epoch, vote_pubkey, node_pubkey, activated_stake, in_epoch_set).
    let mut values_sql = String::new();
    let mut params: Vec<Value> = Vec::with_capacity(data.voters.len() * 5);
    for (idx, row) in data.voters.iter().enumerate() {
        if idx > 0 {
            values_sql.push_str(", ");
        }
        let base = idx * 5;
        values_sql.push_str(&format!(
            "(${}::BIGINT, ${}::BYTEA, ${}::BYTEA, ${}::BIGINT, ${}::BOOLEAN)",
            base + 1,
            base + 2,
            base + 3,
            base + 4,
            base + 5,
        ));
        params.push(Value::from(data.epoch as i64));
        params.push(Value::from(row.vote_pubkey.to_bytes().to_vec()));
        params.push(Value::from(row.node_pubkey.to_bytes().to_vec()));
        params.push(Value::from(row.activated_stake as i64));
        params.push(Value::from(row.in_epoch_set));
    }

    let upsert_sql = format!(
        "INSERT INTO epoch_stakes \
            (epoch, vote_pubkey, node_pubkey, activated_stake, in_epoch_set) \
         VALUES {values_sql} \
         ON CONFLICT (epoch, vote_pubkey) DO UPDATE SET \
            node_pubkey     = EXCLUDED.node_pubkey, \
            activated_stake = EXCLUDED.activated_stake, \
            in_epoch_set    = EXCLUDED.in_epoch_set, \
            updated_at      = now()"
    );

    txn.execute(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        &upsert_sql,
        params,
    ))
    .await?;

    // Prune older epochs — keep current and current-1 only.
    txn.execute(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        "DELETE FROM epoch_stakes WHERE epoch < $1",
        [Value::from((data.epoch.saturating_sub(1)) as i64)],
    ))
    .await?;

    txn.commit().await?;

    tracing::debug!(
        target: "persist_epoch_stakes",
        "persisted {} voters for epoch {} in {:.3}s",
        data.voters.len(),
        data.epoch,
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}
