// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::time::Duration;

use cloudbreak_core::IndexConfig;
use sea_orm::DatabaseConnection;
use solana_pubkey::Pubkey;
use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use crate::indexer::{AccountsReceivedPerBlock, IndexerState};
use crate::modules::single_version::{self, BlockDeletes, BlockRows};
use crate::{db_queries, metrics, modules};

/// Writes the block into the one-row-per-account "accounts" table: staged in parallel, then moved
/// in one transaction.
/// Also updates the HashMap with the accounts pubkeys that were updated in the slot
pub async fn save_block(
    block: SubscribeUpdateBlock,
    db: &DatabaseConnection,
    config: IndexConfig,
    indexer_state: IndexerState,
) {
    let IndexerState {
        snapshot_processing_state: _,
        self_healing_state,
        slot_finalizer,
        updated_accounts_during_startup,
        buffer_channel_rx_len: _,
        finalize_slot_buffer_size,
        accounts_owner_map,
        largest_accounts,
        non_circulating,
        supply_tracker,
        mint_cache,
    } = indexer_state;

    let start_time = Instant::now();

    let slot = block.slot;
    let is_repaired = block.blockhash.is_empty();

    modules::snapshot::process_snapshot_if_needed(
        config.clone(),
        slot,
        &updated_accounts_during_startup,
        finalize_slot_buffer_size.clone(),
        accounts_owner_map.clone(),
        largest_accounts.clone(),
        non_circulating.clone(),
        supply_tracker.clone(),
        mint_cache.clone(),
    )
    .await;

    let mut block_bytes_data: usize = 0;
    let mut block_rows = BlockRows::default();

    let mut updated_accounts_for_slot = Vec::new();
    let mut updated_accounts_owners_for_slot = Vec::new();
    let mut closed_accounts_for_slot = Vec::new();
    // Owners are only captured when the owner map is enabled; empty owners make the
    // finalize cleanup fall back to the no-owner SQL.
    let capture_owners = accounts_owner_map.is_enabled();

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

    // The stake map goes first so the trackers below see this block's membership.
    // Each returns empty when its feature is disabled.
    let stake_block = non_circulating.apply_block(slot, is_repaired, &block.accounts);
    let largest_pending = largest_accounts.build_block_pending(
        &block.accounts,
        &non_circulating,
        &stake_block.expired,
    );
    let supply_pending = supply_tracker.build_block_pending(&block.accounts);
    mint_cache.indexer().on_block(
        db,
        slot,
        &block.accounts,
        self_healing_state.lowest_gap(),
        config.database.save_block_queries_timeout,
    );

    // Create the chunks for updating the "accounts" table
    let system_program_id = [0u8; 32].to_vec();
    for account in block.accounts {
        let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();

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

        updated_accounts_for_slot.push(account.pubkey.clone());
        if capture_owners {
            updated_accounts_owners_for_slot.push(account.owner.clone());
        }

        block_rows.push(
            account.pubkey,
            account.owner,
            account.lamports,
            account.executable,
            account.rent_epoch,
            account.data,
            account.write_version,
            account.txn_signature,
        );
    }

    let closed_account_for_slot_len = closed_accounts_for_slot.len();

    // Captured before `save_closed_accounts` drains the map entries and changed-owner
    // record for this slot; at finalize time the old owners are unrecoverable.
    let (closed_cleanup_pubkeys, closed_cleanup_owners) = if capture_owners {
        accounts_owner_map.closed_cleanup_pairs(&closed_accounts_for_slot, slot)
    } else {
        (Vec::new(), Vec::new())
    };

    // `snapshot_accounts` rows are deleted in the block transaction only after the startup
    // one-shot finished. Until then the block's keys join its cache, so the two never race.
    let snapshot_ready = updated_accounts_during_startup.snapshot_ready_or_defer(
        block_rows
            .pubkeys
            .iter()
            .chain(closed_accounts_for_slot.iter())
            .cloned(),
    );

    let (routed_pubkeys, routed_owners, unrouted_pubkeys) =
        accounts_owner_map.take_closed_pairs(&closed_accounts_for_slot, slot);
    let block_deletes = BlockDeletes {
        routed_pubkeys,
        routed_owners,
        unrouted_pubkeys,
    };

    // Record the block data in the finalizer map (keyed by slot). It is held there until the slot
    // is finalized (via a finalized notification or the ancestor walk). For snapshot-repaired
    // blocks the chain fields are empty/zero.
    slot_finalizer.record_block(
        slot,
        AccountsReceivedPerBlock {
            block_time: block.block_time,
            accounts: updated_accounts_for_slot,
            accounts_owners: updated_accounts_owners_for_slot,
            closed_accounts: closed_accounts_for_slot,
            closed_cleanup_pubkeys,
            closed_cleanup_owners,
        },
        block.blockhash.clone(),
        block.parent_slot,
        block.parent_blockhash.clone(),
    );

    metrics::record_closed_accounts_per_slot(closed_account_for_slot_len);
    metrics::record_block_size(block_bytes_data);

    // Stage the rows in parallel, then move them into "accounts" in one transaction, left open
    // until the slot row. The supply tracker starts this after its cache probe; its miss read
    // sees the state before the block.
    let pool = db.get_postgres_connection_pool();
    let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);
    let mut open_block_tx = None;
    let block_writes = async {
        if let Err(e) = single_version::stage_block(
            pool,
            slot,
            &block_rows,
            config.grpc.chunk_size,
            config.grpc.max_chunk_bytes_data,
            query_timeout,
        )
        .await
        {
            tracing::error!(target: "single_version", "block {} stage failed: {}", slot, e);
            metrics::increment_db_errors();
            return false;
        }
        // A deadlock aborts the whole transaction, so the block is simply written again.
        let mut attempt = 1;
        loop {
            match single_version::write_block(
                pool,
                slot,
                &block_rows,
                &block_deletes,
                snapshot_ready,
                query_timeout,
            )
            .await
            {
                Ok(open) => {
                    open_block_tx = Some(open);
                    break true;
                }
                Err(e) if attempt < 3 && e.contains("deadlock detected") => {
                    tracing::warn!(target: "single_version", "block {} write attempt {} hit a deadlock, retrying: {}", slot, attempt, e);
                    attempt += 1;
                }
                Err(e) => {
                    tracing::error!(target: "single_version", "block {} write failed, rolled back: {}", slot, e);
                    metrics::increment_db_errors();
                    break false;
                }
            }
        }
    };
    let (supply_outcome, block_writes_ok) = supply_tracker
        .apply_block(
            slot,
            is_repaired,
            supply_pending,
            &stake_block,
            block_writes,
        )
        .await;

    largest_accounts
        .commit_block(slot, largest_pending, db, &config)
        .await;
    non_circulating
        .persist_block(&stake_block, db, &config)
        .await;

    // The slot row commits with the block's rows, so readers see the whole block or none of it.
    // Without an open transaction (the write failed) the slot row is written on its own, as before.
    let published = match open_block_tx {
        Some(open) => match open
            .publish(
                slot,
                block.block_time.unwrap_or_default().timestamp,
                Some(block.blockhash.clone()),
                updated_accounts_during_startup.health.is_healthy(),
                CommitmentLevel::Confirmed as i32,
            )
            .await
        {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(target: "single_version", "block {} publish failed, rolled back: {}", slot, e);
                metrics::increment_db_errors();
                false
            }
        },
        None => false,
    };
    if let Err(e) = single_version::clear_stage(pool, slot, query_timeout).await {
        tracing::warn!(target: "single_version", "block {} stage cleanup failed: {}", slot, e);
    }
    if !published {
        db_queries::insert_slot(
            slot,
            block.block_time,
            Some(&block.blockhash),
            CommitmentLevel::Confirmed,
            updated_accounts_during_startup.health.is_healthy(),
            db,
            &config,
        )
        .await;
    }

    db_queries::insert_recent_blockhash(
        slot,
        block.blockhash.clone(),
        block.block_height.map(|h| h.block_height),
        db,
        &config,
    )
    .await;

    supply_tracker.finish_block(slot, supply_outcome, block_writes_ok);

    let elapsed = start_time.elapsed().as_secs_f64();
    metrics::record_block_processing(elapsed, "block");
}
