// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The cleanup statements.
//!
//! # Ordering across the two tables
//!
//! A closed account's mask lives in `accounts` and the rows it shadows can live in either table.
//! Every read unions both tables in one statement and takes the newest version per pubkey, then
//! drops the ones with no lamports. So a reader sees one Postgres snapshot, and the only state
//! that reads wrong is "mask gone, older positive row still there".
//!
//! [`drain_all`] therefore issues every `snapshot_accounts` statement first and the `accounts`
//! statements only after they succeed. If the first half fails the second never runs, which
//! leaves the mask in place, and a mask in place reads as closed. That is why this needs
//! ordering and not a transaction.
//!
//! This leans on every serving read being a single statement over both tables. That invariant
//! lives in `crates/api/src/db/*.sql` and the indexer's own readers, and it must be rechecked if
//! a reader ever splits into two statements.

use std::time::Duration;

use futures::future::join_all;
use futures::stream::{self, StreamExt};
use sea_orm::sea_query::ArrayType;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr, Statement, Value};
use tokio::time::{Instant, timeout};

use super::pending::{CleanupKey, KeyForm, Taken};
use crate::metrics;

const ACCOUNTS_TABLE: &str = "accounts";
const SNAPSHOT_ACCOUNTS_TABLE: &str = "snapshot_accounts";

/// Keys per statement in the startup one-shot, which drains a whole snapshot load at once.
const STARTUP_BATCH_SIZE: usize = 500;

/// Startup one-shot statements in flight at once.
const STARTUP_CONCURRENCY: usize = 10;

/// Runs one cleanup statement. Implemented for [`DatabaseConnection`], and for a recording fake
/// in the tests so the ordering rule can be pinned without a database.
pub trait CleanupExecutor: Send + Sync + 'static {
    fn execute_cleanup(
        &self,
        statement: Statement,
    ) -> impl std::future::Future<Output = Result<u64, DbErr>> + Send;
}

impl CleanupExecutor for DatabaseConnection {
    async fn execute_cleanup(&self, statement: Statement) -> Result<u64, DbErr> {
        self.execute(statement)
            .await
            .map(|result| result.rows_affected())
    }
}

/// Runs one drain against both tables, `snapshot_accounts` first.
///
/// `skip_snapshot` is the startup rule: while the snapshot load is running that table has no
/// indexes and is still taking rows, so its keys go to the one-shot instead.
///
/// `batch_size` caps the keys per statement. The chunks of one table run concurrently, and the
/// `accounts` chunks start only after every `snapshot_accounts` chunk has succeeded, so a failure
/// part way through leaves masks in place.
///
/// Returns the keys that had no older version in `accounts`.
pub async fn drain_all<E: CleanupExecutor>(
    executor: &E,
    taken: &Taken,
    query_timeout: Duration,
    skip_snapshot: bool,
    batch_size: usize,
) -> Result<usize, DbErr> {
    let batch_size = batch_size.max(1);

    if !skip_snapshot {
        run_table(
            executor,
            taken,
            SNAPSHOT_ACCOUNTS_TABLE,
            "cleanup_snapshot_accounts",
            "cleanup_snapshot_accounts_batch",
            query_timeout,
            batch_size,
        )
        .await?;
    }

    let chunks = run_table(
        executor,
        taken,
        ACCOUNTS_TABLE,
        "cleanup_accounts",
        "cleanup_accounts_batch",
        query_timeout,
        batch_size,
    )
    .await?;
    Ok(chunks
        .into_iter()
        .map(|(keys, deleted)| keys.saturating_sub(deleted as usize))
        .sum())
}

/// Runs every chunk of both forms against one table at once and waits for all of them.
/// Returns `(keys, deleted)` per chunk, or the first error once every chunk has finished.
async fn run_table<E: CleanupExecutor>(
    executor: &E,
    taken: &Taken,
    table: &str,
    origin: &str,
    batch_origin: &str,
    query_timeout: Duration,
    batch_size: usize,
) -> Result<Vec<(usize, u64)>, DbErr> {
    let start_time = Instant::now();
    let statements = forms(taken).into_iter().flat_map(|(form, items)| {
        items.chunks(batch_size).map(move |chunk| async move {
            let deleted =
                run_statement(executor, table, batch_origin, form, chunk, query_timeout).await?;
            Ok((chunk.len(), deleted))
        })
    });
    let result = join_all(statements).await.into_iter().collect();
    metrics::record_finalize_slot(start_time.elapsed().as_secs_f64(), origin);
    result
}

fn forms(taken: &Taken) -> [(KeyForm, &[(CleanupKey, u64)]); 2] {
    [
        (KeyForm::Routed, taken.routed.as_slice()),
        (KeyForm::Unrouted, taken.unrouted.as_slice()),
    ]
}

/// Deletes every `snapshot_accounts` row of the given pubkeys below one shared cutoff.
///
/// This is the startup one-shot for the accounts touched while the snapshot was loading. A
/// uniform cutoff is safe on `snapshot_accounts` alone: every row it deletes is either below the
/// first live slot, and so superseded, or has a twin in `accounts` at the same slot. No such twin
/// rule holds for `accounts`, so this cutoff must never be applied there.
///
/// Every chunk runs, `STARTUP_CONCURRENCY` at a time. If any chunk failed the error is returned
/// once all have finished, so the caller can leave the node unhealthy rather than declare startup
/// complete over a batch that never landed.
pub async fn delete_below_uniform_cutoff<E: CleanupExecutor>(
    executor: &E,
    pubkeys: Vec<Vec<u8>>,
    cutoff: u64,
    query_timeout: Duration,
) -> Result<u64, DbErr> {
    let chunks: Vec<Vec<(CleanupKey, u64)>> = pubkeys
        .chunks(STARTUP_BATCH_SIZE)
        .map(|chunk| {
            chunk
                .iter()
                .filter_map(|pubkey| {
                    Some((
                        CleanupKey {
                            owner: None,
                            pubkey: pubkey.as_slice().try_into().ok()?,
                        },
                        cutoff,
                    ))
                })
                .collect()
        })
        .collect();

    let statements: Vec<_> = chunks
        .iter()
        .map(|items| run_startup_statement(executor, items, query_timeout))
        .collect();
    let results: Vec<Result<u64, DbErr>> = stream::iter(statements)
        .buffer_unordered(STARTUP_CONCURRENCY)
        .collect()
        .await;

    results.into_iter().sum()
}

async fn run_startup_statement<E: CleanupExecutor>(
    executor: &E,
    items: &[(CleanupKey, u64)],
    query_timeout: Duration,
) -> Result<u64, DbErr> {
    run_statement(
        executor,
        SNAPSHOT_ACCOUNTS_TABLE,
        "cleanup_startup_snapshot_accounts_batch",
        KeyForm::Unrouted,
        items,
        query_timeout,
    )
    .await
}

async fn run_statement<E: CleanupExecutor>(
    executor: &E,
    table: &str,
    origin: &str,
    form: KeyForm,
    items: &[(CleanupKey, u64)],
    query_timeout: Duration,
) -> Result<u64, DbErr> {
    if items.is_empty() {
        return Ok(0);
    }

    let start_time = Instant::now();
    let statement = build_statement(table, form, items);

    let result = timeout(query_timeout, executor.execute_cleanup(statement))
        .await
        .unwrap_or_else(|elapsed| {
            tracing::error!(target: "finalize_cleanup", "cleanup timeout on {}: {}", table, elapsed);
            Err(DbErr::RecordNotInserted)
        });

    metrics::record_finalize_slot(start_time.elapsed().as_secs_f64(), origin);

    match result {
        Ok(deleted) => {
            metrics::FINALIZE_SLOT_DELETED_ACCOUNTS.observe(deleted as f64);
            Ok(deleted)
        }
        Err(error) => {
            tracing::error!(
                target: "finalize_cleanup",
                "cleanup failed on {} for {} keys: {}",
                table,
                items.len(),
                error
            );
            metrics::increment_db_errors();
            Err(error)
        }
    }
}

fn build_statement(table: &str, form: KeyForm, items: &[(CleanupKey, u64)]) -> Statement {
    let pubkeys = Value::Array(
        ArrayType::Bytes,
        Some(Box::new(
            items
                .iter()
                .map(|(key, _)| Value::Bytes(Some(Box::new(key.pubkey.to_vec()))))
                .collect(),
        )),
    );
    let cutoffs = Value::Array(
        ArrayType::BigInt,
        Some(Box::new(
            items
                .iter()
                .map(|(_, cutoff)| Value::BigInt(Some(*cutoff as i64)))
                .collect(),
        )),
    );

    match form {
        KeyForm::Routed => {
            let sql =
                include_str!("../../db/cleanupWithOwner.sql").replace("accounts_table_name", table);
            let owners = Value::Array(
                ArrayType::Bytes,
                Some(Box::new(
                    items
                        .iter()
                        .map(|(key, _)| {
                            Value::Bytes(Some(Box::new(key.owner.unwrap_or_default().to_vec())))
                        })
                        .collect(),
                )),
            );
            Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Postgres,
                sql,
                vec![pubkeys, owners, cutoffs],
            )
        }
        KeyForm::Unrouted => {
            let sql = include_str!("../../db/cleanup.sql").replace("accounts_table_name", table);
            Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Postgres,
                sql,
                vec![pubkeys, cutoffs],
            )
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::modules::cleanup::pending::tests::{routed, unrouted};
    use std::sync::Mutex;

    /// Records every statement it is handed, in order, and fails the tables it is told to fail.
    #[derive(Default)]
    pub(crate) struct RecordingExecutor {
        pub(crate) issued: Mutex<Vec<String>>,
        pub(crate) values: Mutex<Vec<String>>,
        fail_on: Mutex<Vec<String>>,
    }

    impl RecordingExecutor {
        pub(crate) fn failing(table: &str) -> Self {
            Self {
                fail_on: Mutex::new(vec![table.to_string()]),
                ..Default::default()
            }
        }

        pub(crate) fn issued(&self) -> Vec<String> {
            self.issued.lock().expect("lock").clone()
        }

        pub(crate) fn last_values(&self) -> Vec<String> {
            self.values.lock().expect("lock").clone()
        }
    }

    impl CleanupExecutor for RecordingExecutor {
        async fn execute_cleanup(&self, statement: Statement) -> Result<u64, DbErr> {
            let sql = statement.sql.clone();
            let table = if sql.contains("DELETE FROM snapshot_accounts") {
                "snapshot_accounts"
            } else {
                "accounts"
            };
            let form = if sql.contains("k.owner") {
                "routed"
            } else {
                "unrouted"
            };
            self.issued
                .lock()
                .expect("lock")
                .push(format!("{table}:{form}"));
            self.values
                .lock()
                .expect("lock")
                .push(format!("{:?}", statement.values));

            if self
                .fail_on
                .lock()
                .expect("lock")
                .iter()
                .any(|t| t == table)
            {
                return Err(DbErr::RecordNotInserted);
            }
            Ok(1)
        }
    }

    /// Raises the DB error threshold so a test that drives a failing statement does not trip the
    /// process exit in `increment_db_errors`.
    pub(crate) fn allow_db_errors() {
        let _ = crate::metrics::DB_ERRORS_THRESHOLD.set(f64::MAX);
    }

    fn taken(routed_keys: Vec<(CleanupKey, u64)>, unrouted_keys: Vec<(CleanupKey, u64)>) -> Taken {
        Taken {
            routed: routed_keys,
            unrouted: unrouted_keys,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn every_snapshot_statement_runs_before_any_accounts_statement() {
        let executor = RecordingExecutor::default();
        let batch = taken(vec![(routed(1, 1), 100)], vec![(unrouted(2), 100)]);

        drain_all(&executor, &batch, Duration::from_secs(5), false, 500)
            .await
            .expect("succeeds");

        let issued = executor.issued();
        assert_eq!(issued.len(), 4);
        assert!(
            issued[..2]
                .iter()
                .all(|s| s.starts_with("snapshot_accounts"))
        );
        assert!(issued[2..].iter().all(|s| s.starts_with("accounts")));
    }

    #[tokio::test]
    async fn a_failed_snapshot_statement_stops_the_accounts_statement() {
        allow_db_errors();
        let executor = RecordingExecutor::failing("snapshot_accounts");
        let batch = taken(vec![(routed(1, 1), 100)], vec![]);

        let result = drain_all(&executor, &batch, Duration::from_secs(5), false, 500).await;

        assert!(result.is_err());
        assert_eq!(
            executor.issued(),
            vec!["snapshot_accounts:routed".to_string()],
            "the mask must not be deleted while the rows it shadows survive"
        );
    }

    fn unrouted_keys(count: u8) -> Vec<(CleanupKey, u64)> {
        (1..=count).map(|byte| (unrouted(byte), 100)).collect()
    }

    #[tokio::test]
    async fn a_drain_chunks_each_form_at_the_batch_size() {
        let executor = RecordingExecutor::default();
        let batch = taken(vec![], unrouted_keys(5));

        drain_all(&executor, &batch, Duration::from_secs(5), false, 2)
            .await
            .expect("succeeds");

        let issued = executor.issued();
        assert_eq!(issued.len(), 6, "5 keys at 2 per statement is 3 per table");
        assert!(
            issued[..3]
                .iter()
                .all(|s| s.starts_with("snapshot_accounts"))
        );
        assert!(issued[3..].iter().all(|s| s.starts_with("accounts")));
    }

    #[tokio::test]
    async fn a_failed_snapshot_chunk_stops_every_accounts_chunk() {
        allow_db_errors();
        let executor = RecordingExecutor::failing("snapshot_accounts");
        let batch = taken(vec![], unrouted_keys(5));

        let result = drain_all(&executor, &batch, Duration::from_secs(5), false, 2).await;

        assert!(result.is_err());
        assert_eq!(
            executor.issued(),
            vec!["snapshot_accounts:unrouted".to_string(); 3],
            "every snapshot chunk runs, no accounts chunk may follow"
        );
    }

    /// Holds each statement open briefly and records how many run at once, per table.
    #[derive(Default)]
    struct OverlapExecutor {
        in_flight: Mutex<usize>,
        peak: Mutex<usize>,
        snapshot_done: Mutex<usize>,
        accounts_started_early: Mutex<bool>,
        fail_one: Mutex<bool>,
    }

    impl CleanupExecutor for OverlapExecutor {
        async fn execute_cleanup(&self, statement: Statement) -> Result<u64, DbErr> {
            let snapshot = statement.sql.contains("DELETE FROM snapshot_accounts");
            if !snapshot && *self.snapshot_done.lock().expect("lock") < 3 {
                *self.accounts_started_early.lock().expect("lock") = true;
            }
            {
                let mut in_flight = self.in_flight.lock().expect("lock");
                *in_flight += 1;
                let mut peak = self.peak.lock().expect("lock");
                *peak = (*peak).max(*in_flight);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            *self.in_flight.lock().expect("lock") -= 1;
            if snapshot {
                *self.snapshot_done.lock().expect("lock") += 1;
            }
            if std::mem::take(&mut *self.fail_one.lock().expect("lock")) {
                return Err(DbErr::RecordNotInserted);
            }
            Ok(1)
        }
    }

    #[tokio::test]
    async fn a_table_runs_its_chunks_at_once_and_accounts_waits_for_snapshot() {
        let executor = OverlapExecutor::default();
        let batch = taken(vec![], unrouted_keys(5));

        drain_all(&executor, &batch, Duration::from_secs(5), false, 2)
            .await
            .expect("succeeds");

        assert_eq!(
            *executor.peak.lock().expect("lock"),
            3,
            "3 chunks per table"
        );
        assert!(!*executor.accounts_started_early.lock().expect("lock"));
    }

    #[tokio::test]
    async fn the_one_shot_runs_ten_at_a_time_and_fails_on_any_chunk() {
        allow_db_errors();
        let executor = OverlapExecutor::default();
        *executor.fail_one.lock().expect("lock") = true;
        let pubkeys = (0..STARTUP_BATCH_SIZE * 25)
            .map(|i| (i as u32).to_le_bytes().repeat(8))
            .collect();

        let result =
            delete_below_uniform_cutoff(&executor, pubkeys, 900, Duration::from_secs(5)).await;

        assert!(result.is_err(), "one failed chunk fails the one-shot");
        assert_eq!(
            *executor.snapshot_done.lock().expect("lock"),
            25,
            "every chunk still runs"
        );
        assert_eq!(*executor.peak.lock().expect("lock"), STARTUP_CONCURRENCY);
    }

    #[tokio::test]
    async fn the_snapshot_half_is_skipped_during_startup() {
        let executor = RecordingExecutor::default();
        let batch = taken(vec![(routed(1, 1), 100)], vec![]);

        drain_all(&executor, &batch, Duration::from_secs(5), true, 500)
            .await
            .expect("succeeds");

        assert_eq!(executor.issued(), vec!["accounts:routed".to_string()]);
    }

    #[tokio::test]
    async fn the_routed_form_binds_owners_and_the_unrouted_form_does_not() {
        let executor = RecordingExecutor::default();
        let batch = taken(vec![(routed(1, 1), 100)], vec![(unrouted(2), 200)]);

        drain_all(&executor, &batch, Duration::from_secs(5), true, 500)
            .await
            .expect("succeeds");

        let values = executor.last_values();
        assert_eq!(
            values[0].matches("Array(").count(),
            3,
            "routed binds pubkeys, owners and cutoffs"
        );
        assert!(values[0].contains("100"));
        assert_eq!(
            values[1].matches("Array(").count(),
            2,
            "unrouted binds pubkeys and cutoffs"
        );
        assert!(values[1].contains("200"));
    }

    #[tokio::test]
    async fn the_uniform_cutoff_binds_the_same_slot_for_every_key_and_never_touches_accounts() {
        let executor = RecordingExecutor::default();
        let pubkeys = vec![vec![1u8; 32], vec![2u8; 32]];

        delete_below_uniform_cutoff(&executor, pubkeys, 900, Duration::from_secs(5))
            .await
            .expect("succeeds");

        assert_eq!(executor.issued(), vec!["snapshot_accounts:unrouted"]);
        assert!(executor.last_values()[0].contains("900"));
    }

    #[tokio::test]
    async fn the_uniform_cutoff_returns_the_error_so_startup_stays_unhealthy() {
        allow_db_errors();
        let executor = RecordingExecutor::failing("snapshot_accounts");

        let result = delete_below_uniform_cutoff(
            &executor,
            vec![vec![1u8; 32]],
            900,
            Duration::from_secs(5),
        )
        .await;

        assert!(result.is_err());
    }
}
