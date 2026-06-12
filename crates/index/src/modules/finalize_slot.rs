// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::IndexConfig;
use sea_orm::DatabaseConnection;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::{task::JoinSet, time::Instant};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use crate::indexer::AccountsReceivedPerBlock;
use crate::modules::health::{HealthReason, ServiceHealth};
use crate::modules::snapshot::SnapshotProcessingState;
use crate::{db_queries, metrics};

const SLOT_FINALIZE_BATCH_SIZE: usize = 500;

/// Emits a warning when the in-memory blocks map grows beyond this size, as an alert for
/// further debugging (e.g. finalization stalled or a fork is leaving orphaned entries behind).
const BLOCKS_MAP_WARN_THRESHOLD: usize = 500;

/// A confirmed-but-not-yet-finalized block kept in memory until it is finalized.
///
/// For blocks received live from gRPC the chain fields (`blockhash`, `parent_slot`,
/// `parent_blockhash`) are populated and used to walk ancestors when a finalized notification
/// is missed. For slots repaired from a snapshot these are empty/zero, so the ancestor walk
/// stops at them.
#[derive(Default)]
pub struct BlockEntry {
    pub accounts: AccountsReceivedPerBlock,
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
}

#[derive(Default)]
pub struct FinalizerInner {
    /// Confirmed-but-not-yet-finalized block data, keyed by slot.
    pub blocks: HashMap<u64, BlockEntry>,
    /// Slots awaiting finalization, processed in ascending (slot) order.
    pub pending: BTreeSet<u64>,
    /// While non-empty the worker does not drain `pending` (finalization is paused).
    pub pause_reasons: HashSet<PauseReason>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PauseReason {
    GapFill,
}

impl PauseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            PauseReason::GapFill => "gap_fill",
        }
    }
}

/// Owns the confirmed-block map and the ordered finalization queue, and runs a single
/// sequential worker that finalizes slots in order.
///
/// Constraints honored:
/// - A slot is only finalized once its confirmed block data is available (in `blocks`), or it is
///   a snapshot-repaired slot explicitly enqueued.
/// - Finalization is sequential (single worker) to avoid DB deadlocks.
/// - During a gap fill, `pause()` stops the worker from draining. Live finalized notifications
///   fill `pending` up to `finalize_slot_buffer_size` and then block the producer (back-pressure).
///   Repaired slots are enqueued bypassing that bound and, being older, drain first on `resume()`.
///
/// The pair of `Notify`s allow to consume [`FinalizerInner::pending`] in an ascending order(compared to the
/// traditional FIFO a channel would give). And also allows to apply [`Self::bound`] back-pressure only for live
/// slots(not repaired gaps).
#[derive(Clone)]
pub struct SlotFinalizer {
    pub inner: Arc<Mutex<FinalizerInner>>,
    /// Notifies the worker that there might be new drainable work (or that it was resumed).
    work_available: Arc<Notify>,
    /// Notifies blocked live producers that a slot finalized and there is space in the queue.
    /// Only used for live slots (not repaired gaps).
    space_available: Arc<Notify>,
    pub db: DatabaseConnection,
    config: IndexConfig,
    updated_accounts_during_startup: UpdatedAccountsDuringStartup,
    health: ServiceHealth,
    /// Max number of pending live slots before `note_finalized` blocks (back-pressure bound).
    /// Bypassed by `enqueue_unbounded` and `enqueue_gap_boundary`(gap fill).
    pub bound: usize,
}

impl SlotFinalizer {
    /// Creates the finalizer and spawns its single sequential worker task.
    pub fn spawn(
        db: DatabaseConnection,
        config: IndexConfig,
        updated_accounts_during_startup: UpdatedAccountsDuringStartup,
        health: ServiceHealth,
    ) -> Self {
        let bound = config.finalize_slot_buffer_size;
        let finalizer = Self {
            inner: Arc::new(Mutex::new(FinalizerInner::default())),
            work_available: Arc::new(Notify::new()),
            space_available: Arc::new(Notify::new()),
            db,
            config,
            updated_accounts_during_startup,
            health,
            bound,
        };

        let worker = finalizer.clone();
        tokio::spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("slot_finalizer");
            worker.run_worker().await;
        });

        finalizer
    }

    /// Records the confirmed block data for a slot. Called by `save_block` for every block,
    /// whether received live or repaired from a snapshot. Non-blocking.
    ///
    /// For snapshot-repaired blocks `blockhash`/`parent_blockhash` are empty and `parent_slot` is 0.
    pub fn record_block(
        &self,
        slot: u64,
        accounts: AccountsReceivedPerBlock,
        blockhash: String,
        parent_slot: u64,
        parent_blockhash: String,
    ) {
        let mut inner = self.inner.lock().expect("Failed to lock finalizer");
        let already_present = inner
            .blocks
            .insert(
                slot,
                BlockEntry {
                    accounts,
                    blockhash,
                    parent_slot,
                    parent_blockhash,
                },
            )
            .is_some();
        let len = inner.blocks.len();

        if already_present {
            tracing::error!("Block data for slot {} already existed in the map", slot);
        }
        if len > BLOCKS_MAP_WARN_THRESHOLD {
            tracing::warn!(
                target: "finalizer_blocks_map",
                "Finalizer blocks map is unexpectedly large: {} entries (slot {})",
                len,
                slot
            );
        }
    }

    /// Enqueues a live(gRPC) finalized slot. Blocks while the queue is full (back-pressure).
    pub async fn note_finalized(&self, slot: u64) {
        loop {
            {
                let mut inner = self.inner.lock().expect("Failed to lock finalizer");
                // If queue lenght < bound, insert and return immediately. Else wait for space to be available.
                if inner.pending.len() < self.bound {
                    inner.pending.insert(slot);
                    let len = inner.pending.len();
                    drop(inner);
                    self.set_pending_metric(len);
                    self.work_available.notify_one();
                    return;
                }
            }
            self.space_available.notified().await;
        }
    }

    /// Inserts a slot directly into the pending queue without respecting the back-pressure bound.
    /// (vs [`Self::note_finalized`] which respects the bound).
    pub fn enqueue_unbounded(&self, slot: u64) {
        let len = {
            let mut inner = self.inner.lock().expect("Failed to lock finalizer");
            inner.pending.insert(slot);
            inner.pending.len()
        };

        self.set_pending_metric(len);
        self.work_available.notify_one();
    }

    /// Pauses finalization (used while a gap fill is in progress) and marks the service unhealthy.
    pub async fn pause(&self) {
        let newly_paused = self
            .inner
            .lock()
            .expect("Failed to lock finalizer")
            .pause_reasons
            .insert(PauseReason::GapFill);

        if newly_paused {
            tracing::warn!(target: "finalizer", "Finalization paused (gap fill in progress)");
            self.health.add_reason(HealthReason::GapFill).await;
        }
    }

    /// Resumes finalization after a gap fill and clears the gap unhealthy reason (marking the
    /// service healthy if no other reasons remain). Always clears the health reason, even if the
    /// worker was not paused (e.g. the gap was resolved before a fill started); `remove_reason`
    /// is a no-op when the reason is absent.
    pub async fn resume(&self) {
        let was_paused = self
            .inner
            .lock()
            .expect("Failed to lock finalizer")
            .pause_reasons
            .remove(&PauseReason::GapFill);

        if was_paused {
            tracing::info!(target: "finalizer", "Finalization resumed");
            self.work_available.notify_one();
        }

        self.health.remove_reason(HealthReason::GapFill).await;
    }

    /// Read-only lookup of a recorded block's blockhash, used by self-healing to confirm gaps
    /// from the parent chain without an RPC call.
    pub fn block_hash(&self, slot: u64) -> Option<String> {
        self.inner
            .lock()
            .expect("Failed to lock finalizer")
            .blocks
            .get(&slot)
            .map(|e| e.blockhash.clone())
    }

    fn set_pending_metric(&self, len: usize) {
        metrics::FINALIZE_SLOT_HANDLER_QUEUE_SIZE.set(len as i64);
    }

    /// Main worker loop that finalizes slots in order.
    ///
    /// Each iteration it checks if should actually be processing something or if it should go to sleep,
    /// if there is it will process all pending slots in order until empty the pending queue.
    async fn run_worker(self) {
        loop {
            // Check if we actually should process something or either go to sleep.
            let should_wait = {
                let inner = self.inner.lock().expect("Failed to lock finalizer");
                !inner.pause_reasons.is_empty() || inner.pending.is_empty()
            };
            if should_wait {
                self.work_available.notified().await;
            }

            // Loop to consume all pending slots in order.
            loop {
                let slot = {
                    let mut inner = self.inner.lock().expect("Failed to lock finalizer");
                    if !inner.pause_reasons.is_empty() {
                        break;
                    }
                    match inner.pending.iter().next().copied() {
                        Some(slot) => {
                            inner.pending.remove(&slot);
                            slot
                        }
                        None => break,
                    }
                };

                self.finalize_slot_with_ancestors(slot).await;

                let len = self
                    .inner
                    .lock()
                    .expect("Failed to lock finalizer")
                    .pending
                    .len();

                self.set_pending_metric(len);
                // Notify (for `Self::note_finalized`) that a slot was finalized and there is space in the queue.
                self.space_available.notify_one();
            }
        }
    }

    /// Finalizes `slot` and, walking the parent chain, any not-yet-finalized ancestors we have
    /// confirmed data for but whose finalized notification was missed (e.g. during a gap). The
    /// walk hash-checks each hop and stops when the parent is no longer in the map (so repaired
    /// slots, which carry no chain data, are not walked).
    async fn finalize_slot_with_ancestors(&self, slot: u64) {
        let ancestors = self.get_slot_ancestors(slot);

        for (slot, entry) in ancestors {
            finalize_slot(
                &self.config,
                slot,
                self.db.clone(),
                entry.accounts,
                self.updated_accounts_during_startup.clone(),
            )
            .await;
        }

        // After finalizing the slot and its ancestors, any older slot still in the map is unexpected
        let lingering = {
            let inner = self.inner.lock().expect("Failed to lock finalizer");
            inner.blocks.keys().copied().filter(|s| *s < slot).min()
        };
        if let Some(min_slot) = lingering {
            tracing::warn!(
                target: "finalize_anomaly",
                "Slot {} older than just-finalized slot {} still present in the map after the ancestor walk (possible fork)",
                min_slot,
                slot
            );
        }
    }

    /// Returns the chain of ancestors (slot, entry) pairs for a given slot, oldest first.
    /// The list will include the slot itself as the last element. It will also remove any found
    /// ancestor from the pending queue.
    fn get_slot_ancestors(&self, slot: u64) -> Vec<(u64, BlockEntry)> {
        let mut inner = self.inner.lock().expect("Failed to lock finalizer");
        let mut chain: Vec<(u64, BlockEntry)> = Vec::new();

        let entry = inner.blocks.remove(&slot).unwrap_or_default();
        let mut parent_slot = entry.parent_slot;
        let mut parent_hash = entry.parent_blockhash.clone();
        chain.push((slot, entry));

        loop {
            let parent_matches = match inner.blocks.get(&parent_slot) {
                Some(parent) => {
                    let incorrect_parent =
                        !parent_hash.is_empty() && parent.blockhash == parent_hash;
                    if incorrect_parent {
                        tracing::warn!(
                            target: "finalize_anomaly",
                            "Slot {} has incorrect parent: {} (expected: {})",
                            slot,
                            parent.blockhash,
                            parent_hash
                        );
                    }
                    incorrect_parent
                }
                None => false,
            };
            if !parent_matches {
                break;
            }

            let parent_entry = inner
                .blocks
                .remove(&parent_slot)
                .expect("parent must exist (checked above)");

            // Remove the parent slot from the pending queue to avoid finalizing it twice.
            inner.pending.remove(&parent_slot);

            let next_parent_slot = parent_entry.parent_slot;
            let next_parent_hash = parent_entry.parent_blockhash.clone();
            chain.push((parent_slot, parent_entry));
            parent_slot = next_parent_slot;
            parent_hash = next_parent_hash;
        }

        if chain.len() > 1 {
            let ancestors = chain
                .iter()
                .skip(1)
                .map(|(s, _)| s.to_string())
                .collect::<Vec<_>>()
                .join(" -> ");
            tracing::info!(
                target: "finalize_ancestors",
                "Finalizing slot {} together with missed-notification ancestors: {:?}",
                slot,
                ancestors
            );
        }

        chain.reverse();

        chain
    }
}

async fn finalize_slot(
    config: &IndexConfig,
    slot: u64,
    db: DatabaseConnection,
    updated_accounts: AccountsReceivedPerBlock,
    updated_accounts_during_startup: UpdatedAccountsDuringStartup,
) {
    let start_time = Instant::now();

    let db_clone = db.clone();
    let config_clone = config.clone();

    // Mark the slot as finalized before starting the cleanup tasks for API queries consistency
    db_queries::insert_slot(
        slot,
        updated_accounts.block_time,
        CommitmentLevel::Finalized,
        &db_clone,
        &config_clone,
    )
    .await;

    // These are accounts that were in the slot but did not have an older version (which means
    //  they are completely new to our db)
    let new_accounts_in_slot = Arc::new(Mutex::new(0));

    let batches = updated_accounts
        .accounts
        .chunks(SLOT_FINALIZE_BATCH_SIZE)
        .map(|batch| batch.to_vec())
        .collect::<Vec<_>>();

    let mut join_set = JoinSet::new();

    updated_accounts_during_startup.cleanup_stored_accounts_once(&db, slot, config);

    for batch in batches {
        let db_clone = db.clone();
        let batch_clone = batch.clone();
        let new_accounts_in_slot_clone = new_accounts_in_slot.clone();
        let updated_accounts_during_startup = updated_accounts_during_startup.clone();
        let config_clone = config.clone();
        join_set.spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("finalize_slot_internal");

            db_queries::cleanup_accounts(
                &db_clone,
                batch_clone,
                slot,
                "accounts",
                new_accounts_in_slot_clone,
                "cleanup_accounts_batch",
                &config_clone,
            )
            .await;
        });

        // If we are in startup, we just save the updated accounts to delete them after the snapshot is processed
        if updated_accounts_during_startup.is_startup() {
            updated_accounts_during_startup.add_batch_to_cache_during_startup(batch);
            continue;
        }

        let db_clone = db.clone();
        let config_clone = config.clone();
        join_set.spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("finalize_slot_internal");

            // with the latest changes it doesn't make sense any more to try to measure this on the snapshot accounts table
            // but this will asintotically become more accurate as the snapshot accounts table is deleted/cleaned up
            let dummy_new_accounts_in_slot = Arc::new(Mutex::new(0));

            db_queries::cleanup_accounts(
                &db_clone,
                batch,
                slot,
                "snapshot_accounts",
                dummy_new_accounts_in_slot,
                "cleanup_snapshot_accounts_batch",
                &config_clone,
            )
            .await;
        });
    }

    let closed_accounts = updated_accounts.closed_accounts.clone();
    let db_clone = db.clone();
    let config_clone = config.clone();
    join_set.spawn(async move {
        // Updated accounts doesn't include the closed accounts, instead this query will delete the closed accounts inserted
        //  and any previous version of the accounts, so it's safe to execute concurrently with the cleanup_accounts tasks
        // because there is not overlap between the accounts sets
        db_queries::cleanup_closed_accounts(&db_clone, closed_accounts, slot, &config_clone).await;
    });

    // If we are in startup, we just save the closed accounts to delete them after the snapshot is processed
    if updated_accounts_during_startup.is_startup() {
        updated_accounts_during_startup
            .add_batch_to_cache_during_startup(updated_accounts.closed_accounts);
    } else {
        let config_clone = config.clone();
        join_set.spawn(async move {
            // Closed accounts are not included in the updated accounts, so we need to cleanup them separately
            db_queries::cleanup_accounts(
                &db,
                updated_accounts.closed_accounts,
                slot,
                "snapshot_accounts",
                Arc::new(Mutex::new(0)),
                "cleanup_snapshot_closed_accounts",
                &config_clone,
            )
            .await;
        });
    }

    join_set.join_all().await;

    metrics::record_finalize_slot(start_time.elapsed().as_secs_f64(), "total");
    metrics::record_new_accounts_in_slot(
        *new_accounts_in_slot
            .lock()
            .expect("Failed to lock new_accounts_in_slot"),
        "new_accounts_in_slot",
    );
}

///Used to store all accounts that are updated/closed while loading the snapshot, and delete them after the snapshot is processed
#[derive(Clone)]
pub struct UpdatedAccountsDuringStartup {
    pub accounts: Arc<Mutex<HashSet<Vec<u8>>>>,
    pub snapshot_processing_state: Arc<Mutex<SnapshotProcessingState>>,
    health: ServiceHealth,
}

impl UpdatedAccountsDuringStartup {
    pub fn new(
        snapshot_processing_state: Arc<Mutex<SnapshotProcessingState>>,
        health: ServiceHealth,
    ) -> Self {
        Self {
            accounts: Arc::new(Mutex::new(HashSet::new())),
            snapshot_processing_state,
            health,
        }
    }

    pub fn is_startup(&self) -> bool {
        let snapshot_processing_state = self
            .snapshot_processing_state
            .lock()
            .expect("Failed to lock snapshot_processing_state");
        *snapshot_processing_state == SnapshotProcessingState::NotStarted
            || *snapshot_processing_state == SnapshotProcessingState::Started
    }

    pub fn add_batch_to_cache_during_startup(&self, batch: Vec<Vec<u8>>) {
        let mut accounts = self.accounts.lock().expect("Failed to lock accounts");
        accounts.extend(batch);
    }

    /// Only cleans up the accounts if we are NOT in startup and if the accounts cache is not empty already
    fn cleanup_stored_accounts_once(
        &self,
        db: &DatabaseConnection,
        slot: u64,
        config: &IndexConfig,
    ) {
        if self.is_startup()
            || self
                .accounts
                .lock()
                .expect("Failed to lock accounts")
                .is_empty()
        {
            return;
        }

        let accounts = self
            .accounts
            .lock()
            .expect("Failed to lock accounts")
            .drain()
            .collect::<Vec<_>>();

        let db = db.clone();
        let config = config.clone();
        let snapshot_processing_state = self.snapshot_processing_state.clone();
        let health = self.health.clone();

        tokio::spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("startup_snapshot_accounts_cleanup");

            let start_time = Instant::now();

            tracing::info!(target: "cleanup_stored_accounts", "Cleaning up stored accounts from snapshot_accounts - accounts: {}", accounts.len());

            let batches = accounts
                .chunks(SLOT_FINALIZE_BATCH_SIZE)
                .map(|batch| batch.to_vec())
                .collect::<Vec<_>>();

            let mut join_set = JoinSet::new();
            const MAX_CONCURRENT_CLEANUP_TASKS: usize = 10;

            for batch in batches {
                while join_set.len() >= MAX_CONCURRENT_CLEANUP_TASKS {
                    join_set.join_next().await;
                }

                let db = db.clone();
                let config_clone = config.clone();
                join_set.spawn(async move {
                    let _guard =
                        metrics::TokioTaskCounterGuard::new("startup_snapshot_accounts_cleanup");

                    db_queries::cleanup_accounts(
                        &db,
                        batch,
                        slot,
                        "snapshot_accounts",
                        Arc::new(Mutex::new(0)),
                        "cleanup_startup_snapshot_accounts_batch",
                        &config_clone,
                    )
                    .await;
                });
            }

            join_set.join_all().await;

            let elapsed = start_time.elapsed().as_secs_f64();
            tracing::info!(target: "cleanup_stored_accounts", "Cleaned up stored accounts from snapshot_accounts in {} seconds", elapsed);

            // Startup snapshot processing is complete: clear the startup unhealthy reason.
            *snapshot_processing_state
                .lock()
                .expect("Failed to lock snapshot_processing_state") =
                SnapshotProcessingState::FinishedAndCleanedUp;
            health.remove_reason(HealthReason::Startup).await;
        });
    }
}
