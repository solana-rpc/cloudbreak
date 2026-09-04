// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::IndexConfig;
use cloudbreak_core::modules::supply::{self, Pending};
use cloudbreak_entity::accounts;
use sea_orm::{
    ActiveValue::{NotSet, Set},
    DatabaseConnection,
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::time::Duration;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use crate::indexer::{AccountsReceivedPerBlock, IndexerState};
use crate::modules::snapshot::SnapshotProcessingState;
use crate::{db_queries, metrics, modules};

/// One account's latest geyser update in the block, deduplicated by write version
/// for the supply delta computation.
struct PendingSupplyAccount {
    owner: Pubkey,
    lamports: u64,
    write_version: u64,
}

/// Splits the block into chunks and saves them into the "accounts" table
/// Also updates the HashMap with the accounts pubkeys that were updated in the slot
pub async fn save_block(
    block: SubscribeUpdateBlock,
    db: &DatabaseConnection,
    config: IndexConfig,
    indexer_state: IndexerState,
) {
    let IndexerState {
        snapshot_processing_state,
        self_healing_state: _,
        slot_finalizer,
        updated_accounts_during_startup,
        buffer_channel_rx_len: _,
        finalize_slot_buffer_size,
        accounts_owner_map,
        largest_accounts,
        non_circulating,
        supply_tracker,
    } = indexer_state;

    let start_time = Instant::now();
    let chunk_size = config.grpc.chunk_size;
    let max_chunk_bytes_data = config.grpc.max_chunk_bytes_data;

    let slot = block.slot;
    let is_repaired = block.blockhash.is_empty();

    modules::snapshot::process_snapshot_if_needed(
        config.clone(),
        slot,
        &updated_accounts_during_startup,
        finalize_slot_buffer_size.clone(),
        accounts_owner_map.clone(),
        largest_accounts.clone(),
        supply_tracker.clone(),
    )
    .await;

    let mut block_bytes_data: usize = 0;
    let mut chunks = Vec::new();
    let mut current_chunk = Vec::new();
    let mut current_chunk_bytes = 0;

    let mut updated_accounts_for_slot = Vec::new();
    let mut closed_accounts_for_slot = Vec::new();

    metrics::record_new_accounts_in_slot(block.accounts.len(), "block_accounts_total");

    let programs_include_filter = config
        .programs
        .include
        .iter()
        .map(|pubkey| pubkey.0.to_bytes().to_vec())
        .collect::<Vec<_>>();
    let programs_exclude_filter = config
        .programs
        .exclude
        .iter()
        .map(|pubkey| pubkey.0.to_bytes().to_vec())
        .collect::<Vec<_>>();

    // Fold the block's accounts into the largest-accounts tracker before the main loop
    // consumes `block.accounts`. Returns empty when the feature is disabled.
    let largest_pending = largest_accounts.build_block_pending(&block.accounts, &non_circulating);

    let supply_enabled = supply_tracker.is_enabled();
    let mut pending_supply_accounts: HashMap<Pubkey, PendingSupplyAccount> = HashMap::new();

    // Create the chunks for updating the "accounts" table
    let system_program_id = [0u8; 32].to_vec();
    for account in block.accounts {
        let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();

        if supply_enabled {
            let pending = pending_supply_accounts
                .entry(pubkey)
                .or_insert_with(|| PendingSupplyAccount {
                    owner: Pubkey::try_from(account.owner.as_slice()).unwrap(),
                    lamports: account.lamports,
                    write_version: account.write_version,
                });
            if account.write_version > pending.write_version {
                pending.owner = Pubkey::try_from(account.owner.as_slice()).unwrap();
                pending.lamports = account.lamports;
                pending.write_version = account.write_version;
            }
        }

        // If the account is being closed we still add it to the hashmap for cleanup
        //  but we don't add it to the "accounts" table in a normal fashion, instead we added using [`db_queries::insert_closed_accounts`]
        if account.lamports == 0 {
            closed_accounts_for_slot.push(account.pubkey);

            if !account.data.is_empty() || account.owner != system_program_id {
                tracing::warn!(
                    target: "save_block_closed_account",
                    "Account is being closed with data or owner not being the system program id. Pubkey: {}, owner: {}, data LEN: {}, lamports: {}",
                    pubkey,
                    Pubkey::try_from(account.owner.as_slice()).unwrap(),
                    account.data.len(),
                    account.lamports
                );
            }

            continue;
        }

        let mut is_new_owner_included = true;
        if !programs_include_filter.is_empty() {
            if !programs_include_filter.contains(&account.owner) {
                is_new_owner_included = false;
            }
        } else if programs_exclude_filter.contains(&account.owner) {
            is_new_owner_included = false;
        }

        if accounts_owner_map.account_to_be_deleted(
            &account.pubkey,
            &account.owner,
            slot,
            is_new_owner_included,
        ) {
            // If account needs to be deleted, add it to the closed accounts for slot (so that it creates the overriding
            //  "closed mock account" mask for the old owner)
            closed_accounts_for_slot.push(account.pubkey.clone());
        }

        if !is_new_owner_included {
            continue;
        }

        accounts_owner_map.upsert_account(&account.pubkey, &account.owner, slot);

        block_bytes_data += account.data.len();
        current_chunk_bytes += account.data.len();

        updated_accounts_for_slot.push(account.pubkey.clone());

        current_chunk.push(accounts::ActiveModel {
            pubkey: Set(account.pubkey),
            owner: Set(account.owner),
            lamports: Set(account.lamports as i64),
            slot: Set(slot as i64),
            executable: Set(account.executable),
            rent_epoch: Set(account.rent_epoch.into()),
            data: Set(account.data),
            write_version: Set(account.write_version as i64),
            updated_on: NotSet,
            txn_signature: Set(account.txn_signature),
            token_mint: NotSet,
            token_owner: NotSet,
        });

        if current_chunk.len() >= chunk_size || current_chunk_bytes >= max_chunk_bytes_data {
            chunks.push((current_chunk, current_chunk_bytes));
            current_chunk = Vec::new();

            metrics::record_chunk_size(current_chunk_bytes);

            current_chunk_bytes = 0;
        }
    }

    if !current_chunk.is_empty() {
        tracing::debug!(target: "last_chunk", "last_chunk len: {}", current_chunk.len());
        chunks.push((current_chunk, current_chunk_bytes));
    }

    // Build the deduped closed list from the block's final state, so a same-block
    // close then recreate produces one real row and no mask that would violate the
    // (pubkey, slot) primary key.
    if supply_enabled {
        closed_accounts_for_slot.retain(|pubkey| {
            Pubkey::try_from(pubkey.as_slice())
                .ok()
                .and_then(|pk| pending_supply_accounts.get(&pk))
                .is_none_or(|pending| pending.lamports == 0)
        });
    }

    // The supply delta runs under the block-writes lock, released before the
    // block's own inserts so the miss read never reads this block's rows. The
    // tracker owns the lock, the gap bookkeeping, and the metrics.
    let supply_query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);
    let supply_pending: Vec<Pending> = if supply_enabled {
        pending_supply_accounts
            .into_iter()
            .map(|(pubkey, pending)| Pending {
                pubkey,
                owner: pending.owner,
                lamports: pending.lamports,
                write_version: pending.write_version,
            })
            .collect()
    } else {
        Vec::new()
    };
    let supply_apply_start = std::time::Instant::now();
    let supply_outcome = supply_tracker
        .apply_block(
            slot,
            is_repaired,
            supply_pending,
            &closed_accounts_for_slot,
            db,
            supply_query_timeout,
        )
        .await;
    let supply_apply_elapsed = supply_apply_start.elapsed();

    let closed_account_for_slot_len = closed_accounts_for_slot.len();

    // We delay the closed accounts insertion until the snapshot is processed to avoid reads while
    // the `snapshot_accounts` table still doesn't have indexes
    let snapshot_processing_state: SnapshotProcessingState = {
        *snapshot_processing_state
            .lock()
            .expect("Failed to lock snapshot_processing_state")
    };

    let closed_accounts_insert_handle: Option<JoinHandle<bool>> = if snapshot_processing_state
        == SnapshotProcessingState::Finished
        || snapshot_processing_state == SnapshotProcessingState::FinishedAndCleanedUp
    {
        db_queries::insert_closed_accounts(
            db.clone(),
            closed_accounts_for_slot.clone(),
            slot,
            &config,
            accounts_owner_map,
        )
    } else {
        None
    };

    // Record the block data in the finalizer map (keyed by slot). It is held there until the slot
    // is finalized (via a finalized notification or the ancestor walk). For snapshot-repaired
    // blocks the chain fields are empty/zero.
    slot_finalizer.record_block(
        slot,
        AccountsReceivedPerBlock {
            block_time: block.block_time,
            accounts: updated_accounts_for_slot,
            closed_accounts: closed_accounts_for_slot,
        },
        block.blockhash.clone(),
        block.parent_slot,
        block.parent_blockhash.clone(),
    );

    let chunks_length = chunks.len();
    tracing::debug!(target: "chunks_length", "chunks_length: {}", chunks_length);

    metrics::record_closed_accounts_per_slot(closed_account_for_slot_len);
    metrics::record_block_size(block_bytes_data);

    // Update the "accounts" table
    let mut tasks = JoinSet::new();
    for (chunk, byte_size) in chunks {
        let db = db.clone();
        let config_clone = config.clone();
        // TODO: Set concurrency limit
        tasks.spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("insert_accounts_chunk");

            db_queries::insert_accounts_chunk(&db, chunk, byte_size, &config_clone).await
        });
    }

    let mut block_writes_ok = tasks.join_all().await.into_iter().all(|inserted| inserted);

    if let Some(handle) = closed_accounts_insert_handle {
        match handle.await {
            Ok(inserted) => block_writes_ok &= inserted,
            Err(e) => {
                tracing::error!(target: "save_block_closed_accounts_insert", "failed to insert closed accounts: {:?}", e);
                block_writes_ok = false;
            }
        }
    }

    largest_accounts
        .commit_block(slot, largest_pending, db, &config)
        .await;

    // Wait until the chunk processing is finished to insert the slot (this ensures that gPA calls can only read from completed slots)
    db_queries::insert_slot(
        slot,
        block.block_time,
        CommitmentLevel::Confirmed,
        updated_accounts_during_startup.health.is_healthy(),
        db,
        &config,
    )
    .await;

    db_queries::insert_recent_blockhash(
        slot,
        block.blockhash.clone(),
        block.block_height.map(|h| h.block_height),
        db,
        &config,
    )
    .await;

    // Commit or fail closed. The tracker set the status, total, and slot gauges
    // and pinned or marked stale on a write failure; persist the row it returns.
    let supply_finish_start = std::time::Instant::now();
    if let Some(commit) = supply_tracker.finish_block(slot, supply_outcome, block_writes_ok) {
        supply::persist::persist_supply_row(db, &commit, supply_query_timeout).await;
    }
    // Aggregate the supply work's cost in this loop (apply plus finish and the
    // row upsert, excluding the account inserts between them) and log a summary
    // every 50 blocks rather than per block.
    if supply_enabled {
        supply_tracker.observe_loop_time(slot, supply_apply_elapsed + supply_finish_start.elapsed());
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    metrics::record_block_processing(elapsed, "block");
}
