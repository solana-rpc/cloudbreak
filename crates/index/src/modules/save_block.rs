// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::{
    ActiveValue::{NotSet, Set},
    DatabaseConnection,
};
use solana_pubkey::Pubkey;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use cloudbreak_core::IndexConfig;
use cloudbreak_entity::accounts;

use crate::indexer::{AccountsReceivedPerBlock, IndexerState};
use crate::modules::snapshot::SnapshotProcessingState;
use crate::{db_queries, metrics, modules};

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
        updated_accounts_during_startup: _,
        buffer_channel_rx_len: _,
        finalize_slot_buffer_size,
        accounts_owner_map,
    } = indexer_state;

    let start_time = Instant::now();
    let chunk_size = config.grpc.chunk_size;
    let max_chunk_bytes_data = config.grpc.max_chunk_bytes_data;

    let slot = block.slot;

    modules::snapshot::process_snapshot_if_needed(
        config.clone(),
        slot,
        snapshot_processing_state.clone(),
        finalize_slot_buffer_size.clone(),
        accounts_owner_map.clone(),
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

    // Create the chunks for updating the "accounts" table
    let system_program_id = [0u8; 32].to_vec();
    for account in block.accounts {
        // If the account is being closed we still add it to the hashmap for cleanup
        //  but we don't add it to the "accounts" table in a normal fashion, instead we added using [`db_queries::insert_closed_accounts`]
        if account.lamports == 0 {
            closed_accounts_for_slot.push(account.pubkey.clone());

            if !account.data.is_empty() || account.owner != system_program_id {
                tracing::warn!(
                    target: "save_block_closed_account",
                    "Account is being closed with data or owner not being the system program id. Pubkey: {}, owner: {}, data LEN: {}, lamports: {}",
                    Pubkey::try_from(account.pubkey.as_slice()).unwrap(),
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
            pubkey: Set(account.pubkey.clone()),
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

    let closed_account_for_slot_len = closed_accounts_for_slot.len();

    // We delay the closed accounts insertion until the snapshot is processed to avoid reads while
    // the `snapshot_accounts` table still doesn't have indexes
    let snapshot_processing_state: SnapshotProcessingState = {
        *snapshot_processing_state
            .lock()
            .expect("Failed to lock snapshot_processing_state")
    };

    let closed_accounts_insert_handle: Option<JoinHandle<()>> = if snapshot_processing_state
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

            db_queries::insert_accounts_chunk(&db, chunk, byte_size, &config_clone).await;
        });
    }

    tasks.join_all().await;

    if let Some(handle) = closed_accounts_insert_handle
        && let Err(e) = handle.await
    {
        tracing::error!(target: "save_block_closed_accounts_insert", "failed to insert closed accounts: {:?}", e);
    }

    // Wait until the chunk processing is finished to insert the slot (this ensures that gPA calls can only read from completed slots)
    db_queries::insert_slot(
        slot,
        block.block_time,
        CommitmentLevel::Confirmed,
        db,
        &config,
    )
    .await;

    let elapsed = start_time.elapsed().as_secs_f64();
    metrics::record_block_processing(elapsed, "block");
}
