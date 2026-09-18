// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The spawned drainer.
//!
//! One task, one batch at a time. A failed drain is queued again and reattempted on the next
//! round, never inside this one: every failed statement counts against the process-wide
//! `max-db-errors-threshold`, so a batch that fails forever must cost one error per slot, which
//! is what the inline cleanup it replaces cost.
//!
//! Not health gated. It publishes no state and deletes only rows the read paths already hide,
//! and the unhealthy window is a gap fill, which is when the queue holds the most work. This
//! deviates from `docs/feature-guideline.md` rule 8 deliberately.

use std::sync::Arc;
use std::time::Duration;

use super::CleanupHandle;
use super::persist::{self, CleanupExecutor};
use crate::metrics;
use crate::modules::finalize_slot::UpdatedAccountsDuringStartup;

pub fn spawn_cleanup_drainer<E>(
    handle: CleanupHandle,
    executor: Arc<E>,
    startup: UpdatedAccountsDuringStartup,
    query_timeout: Duration,
) -> tokio::task::JoinHandle<()>
where
    E: CleanupExecutor,
{
    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("finalize_cleanup_drainer");
        run(handle, executor, startup, query_timeout).await;
    })
}

async fn run<E: CleanupExecutor>(
    handle: CleanupHandle,
    executor: Arc<E>,
    startup: UpdatedAccountsDuringStartup,
    query_timeout: Duration,
) {
    loop {
        handle.wait_for_work().await;
        handle.note_drain();

        let taken = handle.take_all();
        if taken.is_empty() {
            handle.finish();
            continue;
        }

        let keys = taken.len();
        match persist::drain_all(
            executor.as_ref(),
            &taken,
            query_timeout,
            startup.is_startup(),
        )
        .await
        {
            Ok(new_accounts) => {
                handle.finish();
                metrics::record_new_accounts_in_slot(new_accounts, "new_accounts_in_slot");
            }
            Err(error) => {
                handle.reinsert(taken);
                tracing::warn!(
                    target: "finalize_cleanup",
                    "cleanup of {} keys failed, queued for the next round: {}",
                    keys,
                    error
                );
            }
        }

        handle.publish_lag();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::cleanup::pending::tests::routed;
    use crate::modules::cleanup::persist::tests::{RecordingExecutor, allow_db_errors};
    use crate::modules::snapshot::SnapshotProcessingState;
    use crate::modules::{finalize_slot::UpdatedAccountsDuringStartup, health::ServiceHealth};
    use std::sync::Mutex;

    fn ready_startup() -> UpdatedAccountsDuringStartup {
        UpdatedAccountsDuringStartup::new(
            Arc::new(Mutex::new(SnapshotProcessingState::FinishedAndCleanedUp)),
            ServiceHealth::new(sea_orm::DatabaseConnection::Disconnected),
        )
    }

    async fn drain_once<E: CleanupExecutor>(handle: &CleanupHandle, executor: Arc<E>) {
        let task = spawn_cleanup_drainer(
            handle.clone(),
            executor,
            ready_startup(),
            Duration::from_secs(5),
        );
        for _ in 0..200 {
            if handle.is_quiescent() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        task.abort();
    }

    #[tokio::test]
    async fn the_run_loop_drains_the_queue_to_empty() {
        let handle = CleanupHandle::new(1);
        let executor = Arc::new(RecordingExecutor::default());
        handle.enqueue(100, &[(routed(1, 1), 100), (routed(1, 2), 100)]);

        drain_once(&handle, executor.clone()).await;

        assert!(handle.is_quiescent(), "the drainer left work behind");
        assert_eq!(
            executor.issued(),
            vec!["snapshot_accounts:routed", "accounts:routed"]
        );
    }

    #[tokio::test]
    async fn a_failed_drain_leaves_its_keys_queued() {
        allow_db_errors();
        let handle = CleanupHandle::new(1);
        let executor = Arc::new(RecordingExecutor::failing("snapshot_accounts"));
        handle.enqueue(100, &[(routed(1, 1), 100)]);

        let task = spawn_cleanup_drainer(
            handle.clone(),
            executor,
            ready_startup(),
            Duration::from_secs(5),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        task.abort();

        assert!(!handle.is_quiescent(), "the key must not be lost");
    }
}
