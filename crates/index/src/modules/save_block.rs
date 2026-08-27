// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::IndexConfig;
use cloudbreak_core::modules::largest_accounts::{
    self, LargestAccountsTracker, PendingLargestAccount, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID,
};
use cloudbreak_entity::accounts;
use sea_orm::{
    ActiveValue::{NotSet, Set},
    DatabaseConnection,
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::Duration;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout},
};
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use cloudbreak_core::modules::account_owner_map::AccountOwnerMap;
use cloudbreak_core::modules::supply_tracker::SupplyTracker;

use crate::indexer::{AccountsReceivedPerBlock, IndexerState};
use crate::modules::snapshot::SnapshotProcessingState;
use crate::{db_queries, metrics, modules};

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

    let largest_enabled = largest_accounts.is_enabled();
    let mut largest_pending: HashMap<Pubkey, PendingLargestAccount> = HashMap::new();
    let supply_enabled = supply_tracker.is_enabled();
    let mut pending_supply_accounts: HashMap<Pubkey, PendingSupplyAccount> = HashMap::new();

    // Create the chunks for updating the "accounts" table
    let system_program_id = [0u8; 32].to_vec();
    for account in block.accounts {
        let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();
        if largest_enabled {
            let is_token_owner = account.owner.as_slice() == TOKEN_PROGRAM_ID.as_ref()
                || account.owner.as_slice() == TOKEN_2022_PROGRAM_ID.as_ref();
            let token = if is_token_owner
                && account.data.len() >= 72
                && largest_accounts.is_tracked(&account.data[0..32])
            {
                Some((
                    Pubkey::try_from(&account.data[0..32]).unwrap(),
                    u64::from_le_bytes(account.data[64..72].try_into().unwrap()),
                ))
            } else {
                None
            };
            let update = PendingLargestAccount {
                write_version: account.write_version,
                lamports: account.lamports,
                non_circulating: supply_tracker.is_non_circulating(&pubkey),
                token,
            };
            match largest_pending.entry(pubkey) {
                Entry::Vacant(entry) => {
                    entry.insert(update);
                }
                Entry::Occupied(mut entry) => {
                    if update.write_version > entry.get().write_version {
                        entry.insert(update);
                    }
                }
            }
        }

        supply_tracker.observe_account(pubkey, slot, account.lamports);

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

        let resurrects_gap_closed_account = is_repaired
            && supply_tracker
                .gap_close_floor(&pubkey)
                .is_some_and(|closed_slot| closed_slot >= slot);
        if !resurrects_gap_closed_account {
            accounts_owner_map.upsert_account(&account.pubkey, &account.owner, slot);
        }

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

    let supply_write_guard = supply_tracker.lock_block_writes().await;
    if !is_repaired {
        supply_tracker.record_gap_closes(slot, &closed_accounts_for_slot);
    }
    let defer_map_removals = supply_tracker.is_gap_filling();
    let block_supply_delta = if supply_tracker.is_tracking_deltas() {
        compute_block_supply_delta(
            &accounts_owner_map,
            &supply_tracker,
            pending_supply_accounts,
            slot,
            is_repaired,
        )
    } else {
        accounts_owner_map.merge_lamports(
            pending_supply_accounts
                .iter()
                .map(|(pubkey, pending)| (*pubkey, pending.owner, pending.lamports)),
            slot,
        );
        supply_tracker.record_startup_touches(
            slot,
            pending_supply_accounts
                .into_iter()
                .map(|(pubkey, pending)| (pubkey, pending.lamports, pending.write_version)),
        );
        None
    };

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
            defer_map_removals,
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

    if block_supply_delta.is_none() && !block_writes_ok && supply_tracker.mark_bootstrap_failed() {
        tracing::error!(
            target: "supply_tracker",
            "account writes failed for slot {} during supply bootstrap, marking bootstrap failed",
            slot
        );
    }

    drop(supply_write_guard);

    if largest_enabled {
        commit_largest_accounts(&largest_accounts, largest_pending, slot, db, &config).await;
    }

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

    if let Some(block_delta) = block_supply_delta {
        if !block_writes_ok {
            tracing::error!(
                target: "supply_tracker",
                "account writes failed for slot {}, marking supply stale",
                slot
            );
            if supply_tracker.mark_stale() {
                metrics::SUPPLY_STALE.set(1);
            }
        } else if let Some(commit) = supply_tracker.commit_block(slot, block_delta) {
            db_queries::upsert_supply_row(db, &commit, &config).await;
            metrics::SUPPLY_TOTAL_LAMPORTS.set(commit.total as i64);
            metrics::SUPPLY_SLOT.set(commit.slot as i64);
            metrics::SUPPLY_STALE.set(0);
        }
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    metrics::record_block_processing(elapsed, "block");
}

async fn commit_largest_accounts(
    largest_accounts: &LargestAccountsTracker,
    largest_pending: HashMap<Pubkey, PendingLargestAccount>,
    slot: u64,
    db: &DatabaseConnection,
    config: &IndexConfig,
) {
    let outcome = largest_accounts.apply_block(slot, largest_pending);
    persist_largest_outcome(largest_accounts, outcome, slot, db, config).await;
}

pub(crate) async fn persist_largest_outcome(
    largest_accounts: &LargestAccountsTracker,
    outcome: largest_accounts::BlockOutcome,
    slot: u64,
    db: &DatabaseConnection,
    config: &IndexConfig,
) {
    let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);

    for mint in &outcome.newly_stale {
        tracing::error!(
            target: "largest_accounts",
            "largest accounts reservoir unsound for mint {} at slot {}, marking stale",
            mint,
            slot
        );
    }

    for mint in outcome.newly_stale.iter().chain(outcome.cleared.iter()) {
        let delete = timeout(query_timeout, largest_accounts::delete_mint_rows(db, mint)).await;
        if !matches!(delete, Ok(Ok(()))) {
            metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
            tracing::error!(
                target: "largest_accounts",
                "failed to delete largest_accounts rows for mint {}: {:?}",
                mint,
                delete
            );
        }
    }

    if !outcome.records.is_empty() {
        let persist = timeout(
            query_timeout,
            largest_accounts::persist_records(db, &outcome.records),
        )
        .await;
        if !matches!(persist, Ok(Ok(()))) {
            metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
            tracing::error!(
                target: "largest_accounts",
                "failed to persist largest_accounts records for slot {}, marking {} mint(s) stale: {:?}",
                slot,
                outcome.records.len(),
                persist
            );
            for record in &outcome.records {
                largest_accounts.mark_mint_stale(&record.mint);
                let delete = timeout(
                    query_timeout,
                    largest_accounts::delete_mint_rows(db, &record.mint),
                )
                .await;
                if !matches!(delete, Ok(Ok(()))) {
                    metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
                }
            }
        }
    }

    metrics::LARGEST_ACCOUNTS_STALE_MINTS.set(largest_accounts.stale_count() as i64);
}

fn compute_block_supply_delta(
    accounts_owner_map: &AccountOwnerMap,
    supply_tracker: &SupplyTracker,
    pending_accounts: HashMap<Pubkey, PendingSupplyAccount>,
    slot: u64,
    is_repaired: bool,
) -> Option<i128> {
    let mut routed = Vec::with_capacity(pending_accounts.len());
    for (pubkey, pending) in pending_accounts {
        let zero_prev = supply_tracker.take_zero_prev(&pubkey);
        if is_repaired
            && !zero_prev
            && supply_tracker
                .gap_close_floor(&pubkey)
                .is_some_and(|closed_slot| closed_slot >= slot)
        {
            continue;
        }
        routed.push((pubkey, pending.owner, pending.lamports, zero_prev));
    }

    Some(accounts_owner_map.count_supply_deltas(routed, slot))
}
