// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use solana_accounts_db::accounts_file::AccountsFile;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{sync::mpsc::Sender, task::JoinSet};
use tokio::{task::JoinHandle, time::Instant};
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
};
use cloudbreak_core::{
    Result, SnapshotConfig, modules::account_owner_map::AccountOwnerMap,
};

use crate::{
    db_queries::SnapshotAccountVersion,
    sidecar::{AccountFileData, SnapshotData, SnapshotType, download_snapshot_file},
};

mod accountsdb_helpers;
mod db_queries;
pub mod lt_hash;
pub mod metrics;
pub mod sidecar;

const DB_ACCOUNTS_BATCH_SIZE: usize = 200;

/// Download and save into postgres the snapshot data for the received slot (getting all snapshots files
///  needed until data to that slot is available)
/// If slot is not provided it will just download the latest available full and incremental snapshots
///
/// Safety Note: This function should be run in a separate thread to avoid blocking the main thread
pub async fn run(
    config: SnapshotConfig,
    received_slot: Option<u64>,
    metrics_registry: Option<prometheus::Registry>,
    buffer_size: Option<Arc<Mutex<usize>>>,
    accounts_owner_map: AccountOwnerMap,
) -> Result<()> {
    let start_time = Instant::now();

    let database = Database::connect(ConnectOptions::from(config.database.clone())).await?;

    metrics::register_metrics(metrics_registry)?;

    db_queries::create_temp_snapshot_account_versions_table(&database).await?;

    let snapshot_pair = sidecar::get_snapshot_data(
        &config.tracker_endpoint.endpoint,
        received_slot,
        true,
        false,
    )
    .await?;

    tracing::info!("Snapshot data: {:?}", snapshot_pair);

    // Download and process the snapshots
    let full_snapshot_handle = download_and_process_snapshot(
        snapshot_pair.downloading_endpoint.clone(),
        snapshot_pair.full_snapshot.clone(),
        SnapshotType::Full,
        &database,
        config.clone(),
        accounts_owner_map.clone(),
    );

    // Process incremental snapshot only if needed
    if let Some(incremental_snapshot_data) = snapshot_pair.incremental_snapshot {
        download_and_process_snapshot(
            snapshot_pair.downloading_endpoint.clone(),
            incremental_snapshot_data,
            SnapshotType::Incremental,
            &database,
            config.clone(),
            accounts_owner_map.clone(),
        )
        .await??;

        tracing::info!(target: "incremental_snapshot_completed", "Incremental snapshot completed successfully in {} secs", start_time.elapsed().as_secs_f64());
    }

    full_snapshot_handle.await??;

    tracing::info!(target: "full_snapshot_completed", "Full snapshot completed successfully in {} secs", start_time.elapsed().as_secs_f64());

    if let Some(buffer_size) = buffer_size {
        db_queries::cluster_snapshot_accounts_table(
            &database,
            buffer_size,
            config.database.partition_clustering_threshold,
        )
        .await?;
    }

    db_queries::clean_up_duplicated_accounts(&database).await?;
    db_queries::clean_up_closed_accounts(&database).await?;
    db_queries::create_database_indexes(&database, &config.pg_indexes).await?;

    tracing::info!(
        "Total snapshot processing time after cleanup: {} secs",
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}

fn download_and_process_snapshot(
    sidecar_endpoint: String,
    snapshot_data: SnapshotData,
    snapshot_type: SnapshotType,
    database: &DatabaseConnection,
    config: SnapshotConfig,
    accounts_owner_map: AccountOwnerMap,
) -> JoinHandle<Result<()>> {
    let db_clone = database.clone();

    tokio::spawn(async move {
        let base_dir = sidecar::snapshot_base_dir(snapshot_data.slot);
        download_snapshot_file(
            &sidecar_endpoint,
            snapshot_data.clone(),
            snapshot_type,
            &base_dir,
        )
        .await
        .inspect_err(|e| {
            tracing::error!("Failed to download snapshot: {:?} ({:?})", e, snapshot_type);
        })?;

        process_downloaded_snapshot(&db_clone, snapshot_data, config, accounts_owner_map).await?;

        Ok(())
    })
}

/// Note: this function uses `jobs` as a concurrency limit for spawning new tasks
async fn process_downloaded_snapshot(
    database: &DatabaseConnection,
    snapshot_data: SnapshotData,
    config: SnapshotConfig,
    accounts_owner_map: AccountOwnerMap,
) -> Result<()> {
    let start_time = Instant::now();
    let total_accounts_files_opening_time_micros = Arc::new(Mutex::new(0));

    let base_dir = sidecar::snapshot_base_dir(snapshot_data.slot);
    let path = base_dir.join(&snapshot_data.file_name);
    let solana_snapshot =
        sidecar::unpack_compressed_snapshot(path, &base_dir, snapshot_data.slot)?;
    let mut account_file_workers: JoinSet<Result<()>> = JoinSet::new();
    let accounts_file_concurency = config.accounts_file_concurency.unwrap_or(32);
    let programs_include = config
        .programs
        .include
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();
    let programs_exclude = config
        .programs
        .exclude
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();

    let total_accounts_files_count = solana_snapshot.len();
    let accounts_files_processed = Arc::new(Mutex::new(0));
    let mut last_log_time = Instant::now();

    let accounts_count = Arc::new(Mutex::new(0));

    let (
        insert_into_temp_snapshot_account_versions_tx,
        insert_into_temp_snapshot_account_versions_join_handle,
    ) = insert_into_temp_snapshot_account_versions_handler(database.clone());

    for AccountFileData {
        path,
        size: current_len,
        slot: account_file_slot,
        write_version,
    } in solana_snapshot
    {
        let accounts_count = accounts_count.clone();
        let programs_include = programs_include.clone();
        let programs_exclude = programs_exclude.clone();
        let database = database.clone();

        let percentage_processed =
            *accounts_files_processed.lock().unwrap() * 100 / total_accounts_files_count;

        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_total"])
            .inc();
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_percentage"])
            .set(percentage_processed as f64);
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_total"])
            .set(*accounts_count.lock().unwrap() as f64);

        *accounts_files_processed.lock().unwrap() += 1;

        if last_log_time.elapsed().as_secs() > 30 {
            tracing::info!(target: "processed_snapshot_items", "Accounts files processed: {}% - Accounts total: {}", percentage_processed, *accounts_count.lock().unwrap());
            last_log_time = Instant::now();
        }

        let total_accounts_files_opening_time_micros =
            total_accounts_files_opening_time_micros.clone();

        let insert_into_temp_snapshot_account_versions_tx =
            insert_into_temp_snapshot_account_versions_tx.clone();

        let accounts_owner_map = accounts_owner_map.clone();

        account_file_workers.spawn(async move {
            let start_time = Instant::now();
            let accounts = AccountsFile::new_for_startup(
                path,
                current_len,
                solana_accounts_db::accounts_file::StorageAccess::default(),
            )?;

            let elapsed = start_time.elapsed().as_micros();
            *total_accounts_files_opening_time_micros
                .lock()
                .expect("Failed to lock total_accounts_files_opening_time_micros") += elapsed;

            let mut all_accounts_chunks = Vec::new();
            let mut current_accounts_chunk = Vec::new();
            let mut snapshot_account_versions = Vec::new();

            // Collect all account offsets first
            let mut offsets = Vec::new();
            accounts.scan_accounts_without_data(|offset, _| {
                offsets.push(offset);
            })?;

            // Fetch full account data for each offset
            for offset in offsets {
                accounts.get_stored_account_callback(offset, |account| {
                    // Regardless of the owners being excluded or included, we add the snapshot account version to
                    // the list, we need all accounts there for later deduplication
                    snapshot_account_versions.push(SnapshotAccountVersion {
                        pubkey: account.pubkey().to_bytes().to_vec(),
                        slot: account_file_slot,
                        owner: account.owner.to_bytes().to_vec(),
                    });

                    if !programs_include.is_empty() {
                        if !programs_include.contains(account.owner) {
                            return;
                        }
                    } else if programs_exclude.contains(account.owner) {
                        return;
                    }

                    let pubkey = account.pubkey.to_bytes().to_vec();
                    let owner = account.owner.to_bytes().to_vec();

                    // Add non closed accounts to the accounts owner map (if enabled)
                    if account.lamports > 0 {
                        accounts_owner_map.upsert_account(&pubkey, &owner, account_file_slot);
                    }

                    let account_update = SubscribeUpdateAccount {
                        account: Some(SubscribeUpdateAccountInfo {
                            pubkey,
                            lamports: account.lamports,
                            owner,
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                            data: account.data.to_vec(),
                            write_version,
                            txn_signature: None,
                        }),
                        slot: account_file_slot,
                        is_startup: true,
                    };

                    current_accounts_chunk.push(account_update);

                    if current_accounts_chunk.len() >= DB_ACCOUNTS_BATCH_SIZE {
                        all_accounts_chunks.push(std::mem::take(&mut current_accounts_chunk));
                    }

                    *accounts_count.lock().unwrap() += 1;
                });
            }
            if !current_accounts_chunk.is_empty() {
                all_accounts_chunks.push(current_accounts_chunk);
            }

            for chunk in all_accounts_chunks {
                if chunk.is_empty() {
                    tracing::warn!(
                        "chunk is empty for slot: {} and write version: {}",
                        account_file_slot,
                        write_version
                    );
                    continue;
                }

                db_queries::upsert_accounts_batched(&database, chunk).await?;
            }

            // Send the closed accounts to the insert_into_temp_snapshot_account_versions_tx channel
            insert_into_temp_snapshot_account_versions_tx
                .send(snapshot_account_versions)
                .await?;

            Ok(())
        });

        if account_file_workers.len() >= accounts_file_concurency {
            account_file_workers
                .join_next()
                .await
                .expect("not expected empty account_file_workers")??;
        }
    }

    while let Some(res) = account_file_workers.join_next().await {
        res??;
    }

    drop(insert_into_temp_snapshot_account_versions_tx);

    insert_into_temp_snapshot_account_versions_join_handle.await??;

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "total_snapshot_accounts", "Snapshot processed! - Accounts count: {} in {} seconds", accounts_count.lock().unwrap(), elapsed);
    tracing::info!(target: "total_snapshot_accounts", "Total accounts files opening time: {} seconds", *total_accounts_files_opening_time_micros.lock().unwrap() / 1_000_000);

    Ok(())
}

fn insert_into_temp_snapshot_account_versions_handler(
    database: DatabaseConnection,
) -> (Sender<Vec<SnapshotAccountVersion>>, JoinHandle<Result<()>>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<SnapshotAccountVersion>>(10_000);
    let handle = tokio::spawn(async move {
        let mut snapshot_account_versions = Vec::new();
        let mut join_set: JoinSet<Result<()>> = JoinSet::new();

        while let Some(snapshot_account_versions_chunk) = rx.recv().await {
            metrics::SNAPSHOT_ACCOUNTS_BUFFER_SIZE
                .with_label_values(&["cleanup_duplicated"])
                .set(rx.len() as f64);

            snapshot_account_versions.extend(snapshot_account_versions_chunk);
            if snapshot_account_versions.len()
                >= db_queries::INSERT_SNAPSHOT_ACCOUNT_VERSIONS_TEMP_TABLE_BATCH_SIZE
            {
                let database = database.clone();

                // todo: make this configurable
                if join_set.len() >= 10 {
                    join_set.join_next().await;
                }

                join_set.spawn(async move {
                    db_queries::insert_into_temp_snapshot_account_versions(
                        &database,
                        snapshot_account_versions,
                    )
                    .await?;

                    Ok(())
                });
                snapshot_account_versions = Vec::new();
            }
        }

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Failed to insert snapshot account versions chunk: {:?}", e);
                    return Err(e);
                }
                Err(join_err) => {
                    tracing::error!(
                        "Join error while inserting snapshot account versions: {:?}",
                        join_err
                    );
                    return Err(join_err.into());
                }
            }
        }

        if !snapshot_account_versions.is_empty() {
            db_queries::insert_into_temp_snapshot_account_versions(
                &database,
                snapshot_account_versions,
            )
            .await?;
        }

        Ok(())
    });

    (tx, handle)
}

/// Version of the `process_downloaded_snapshot` function that only processes the slots that are in the gaps list
pub async fn process_downloaded_snapshot_with_gap_filling(
    snapshot_slot: u64,
    incremental_snapshot_file_name: String,
    base_dir: PathBuf,
    config: SnapshotConfig,
    gaps_list: Vec<u64>,
    block_sender: Sender<SubscribeUpdateBlock>,
) -> Result<()> {
    let start_time = Instant::now();

    let path = base_dir.join(&incremental_snapshot_file_name);
    let solana_snapshot = sidecar::unpack_compressed_snapshot(path, &base_dir, snapshot_slot)?;
    let mut account_file_workers: JoinSet<Result<()>> = JoinSet::new();
    let accounts_file_concurency = config.accounts_file_concurency.unwrap_or(32);
    let programs_include = config
        .programs
        .include
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();
    let programs_exclude = config
        .programs
        .exclude
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();

    let total_accounts_files_count = solana_snapshot.len();
    let accounts_files_processed = Arc::new(Mutex::new(0));
    let mut last_log_time = Instant::now();

    let accounts_count = Arc::new(Mutex::new(0));

    for AccountFileData {
        path,
        size: current_len,
        slot: account_file_slot,
        write_version,
    } in solana_snapshot
    {
        if !gaps_list.contains(&account_file_slot) {
            continue;
        }

        let accounts_count = accounts_count.clone();
        let programs_include = programs_include.clone();
        let programs_exclude = programs_exclude.clone();

        let percentage_processed =
            *accounts_files_processed.lock().unwrap() * 100 / total_accounts_files_count;

        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_total"])
            .inc();
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_percentage"])
            .set(percentage_processed as f64);
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_total"])
            .set(*accounts_count.lock().unwrap() as f64);

        *accounts_files_processed.lock().unwrap() += 1;

        if last_log_time.elapsed().as_secs() > 30 {
            tracing::info!(target: "processed_snapshot_items", "Accounts files processed: {}% - Accounts total: {}", percentage_processed, *accounts_count.lock().unwrap());
            last_log_time = Instant::now();
        }

        let block_sender = block_sender.clone();
        account_file_workers.spawn(async move {
            let accounts = AccountsFile::new_for_startup(
                path,
                current_len,
                solana_accounts_db::accounts_file::StorageAccess::default(),
            )?;

            let mut accounts_for_slot = Vec::new();

            // Collect all account offsets first
            let mut offsets = Vec::new();
            accounts.scan_accounts_without_data(|offset, _| {
                offsets.push(offset);
            })?;

            // Fetch full account data for each offset
            for offset in offsets {
                accounts.get_stored_account_callback(offset, |account| {
                    if !programs_include.is_empty() {
                        // We always include accounts being closed, they are needed for later cleanup of older versions of the accounts
                        if !programs_include.contains(account.owner) && account.lamports > 0 {
                            return;
                        }
                    } else if programs_exclude.contains(account.owner) {
                        return;
                    }

                    let account_update = SubscribeUpdateAccountInfo {
                        pubkey: account.pubkey.to_bytes().to_vec(),
                        lamports: account.lamports,
                        owner: account.owner.to_bytes().to_vec(),
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                        data: account.data.to_vec(),
                        write_version,
                        txn_signature: None,
                    };

                    accounts_for_slot.push(account_update);

                    *accounts_count.lock().unwrap() += 1;
                });
            }

            let accounts_for_slot_len = accounts_for_slot.len();

            block_sender
                .send(SubscribeUpdateBlock {
                    slot: account_file_slot,
                    accounts: accounts_for_slot,
                    block_height: None,
                    block_time: None,
                    blockhash: String::new(),
                    rewards: None,
                    parent_slot: 0,
                    parent_blockhash: String::new(),
                    executed_transaction_count: 0,
                    transactions: Vec::new(),
                    updated_account_count: accounts_for_slot_len as u64,
                    entries_count: 0,
                    entries: Vec::new(),
                })
                .await?;

            Ok(())
        });

        if account_file_workers.len() >= accounts_file_concurency {
            account_file_workers
                .join_next()
                .await
                .expect("not expected empty account_file_workers")??;
        }
    }

    while let Some(res) = account_file_workers.join_next().await {
        res??;
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "total_snapshot_accounts", "Snapshot processed! - Accounts count: {} in {} seconds", accounts_count.lock().unwrap(), elapsed);

    Ok(())
}
