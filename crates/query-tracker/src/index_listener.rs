// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::tracker;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr, Statement};
use solana_pubkey::Pubkey;
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;
use tracing::{error, info, warn};
use cloudbreak_core::QueryTrackerConfig;

const COUNT_AUTO_INDEXES_SQL: &str = "SELECT COUNT(*) FROM pg_indexes \
    WHERE schemaname = 'public' \
    AND tablename = 'snapshot_accounts'";

async fn count_auto_indexes(db: &DatabaseConnection) -> Option<usize> {
    let stmt = Statement::from_string(db.get_database_backend(), COUNT_AUTO_INDEXES_SQL);
    match db.query_one(stmt).await {
        Ok(Some(row)) => {
            let count = row
                .try_get_by_index::<i64>(0)
                .ok()
                .map(|v| v.max(0) as usize);
            if let Some(count) = count {
                crate::metrics::SNAPSHOT_ACCOUNTS_INDEXES.set(count as i64);
            }
            count
        }
        Ok(None) => {
            crate::metrics::SNAPSHOT_ACCOUNTS_INDEXES.set(0);
            Some(0)
        }
        Err(e) => {
            error!(
                target: "query_tracker_index_listener",
                "Failed to count auto-created indexes: {:?}", e
            );
            None
        }
    }
}

pub fn check_program_in_index_list(program: Pubkey, config: &QueryTrackerConfig) -> bool {
    if config.included_programs.is_empty() {
        !config.excluded_programs.iter().any(|p| p.0 == program)
    } else {
        config.included_programs.iter().any(|p| p.0 == program)
    }
}

#[tracing::instrument(name = "index_listener", skip_all)]
pub async fn index_listener(db: DatabaseConnection, config: QueryTrackerConfig) {
    let enabled = config.create_database_indexes;
    let delay = config.index_creation_delay;

    info!(
        "Index listener started (execution {}, delay: {:?}, max-auto-indexes: {:?}, pull-based)",
        if enabled { "enabled" } else { "disabled" },
        delay,
        config.max_auto_indexes
    );

    loop {
        let indexer_metrics = tracker::read_indexer_metrics(&config.indexer_metrics).await;
        if let Some(indexer_metrics) = indexer_metrics {
            if indexer_metrics > config.indexer_metrics_threshold {
                tracing::debug!(target: "query_tracker_metrics", "Indexer metrics is above threshold: {}", indexer_metrics);
                tokio::time::sleep(delay).await;
                continue;
            }
        } else {
            error!(
                "Failed to read indexer metrics at: {}",
                config.indexer_metrics
            );
            tokio::time::sleep(delay).await;
            continue;
        }

        let has_items = tokio::task::spawn_blocking(|| tracker::wait_for_queue_items(5000))
            .await
            .unwrap_or(false);

        if !has_items {
            continue;
        }

        let prioritized = tokio::task::spawn_blocking(tracker::pop_highest_priority_query)
            .await
            .unwrap_or(None);

        if let Some(query_item) = prioritized {
            if !enabled {
                info!(
                    target: "index_listener_disabled",
                    "Index creation disabled; skipping query for program '{}' (count: {})",
                    query_item.program, query_item.count
                );
                continue;
            }

            tracing::debug!(
                target: "query_tracker_index_listener",
                "Processing highest-priority query for program '{}' (count: {})",
                query_item.program, query_item.count
            );

            if !check_program_in_index_list(query_item.program, &config) {
                continue;
            }

            if let Some(max) = config.max_auto_indexes
                && let Some(current) = count_auto_indexes(&db).await
                && current >= max
            {
                warn!(
                    target: "query_tracker_index_listener",
                    "Auto-index cap reached ({}/{}); dropping query for program '{}' (count: {})",
                    current, max, query_item.program, query_item.count
                );
                tokio::time::sleep(delay).await;
                continue;
            }

            let program = query_item.program;
            let query_config = query_item.config.clone();

            create_index_for_table(&db, program, &query_config, "accounts").await;
            create_index_for_table(&db, program, &query_config, "snapshot_accounts").await;

            tracing::debug!(
                target: "query_tracker_index_listener",
                "Waiting {:?} before checking queue for next index creation...",
                delay
            );

            tokio::time::sleep(delay).await;
        }
    }
}

async fn create_index_for_table(
    db: &DatabaseConnection,
    program: Pubkey,
    config: &Option<RpcProgramAccountsConfig>,
    table_name: &str,
) {
    let (sql_query, index_name, _rpc_example) =
        match tracker::generate_sql_for_query(program, config.as_ref(), table_name) {
            Some((sql_query, index_name, rpc_example)) => (sql_query, index_name, rpc_example),
            None => {
                warn!(
                    target: "query_tracker_index_listener",
                    "Failed to generate SQL for program '{}'; skipping", program
                );
                return;
            }
        };

    tracing::debug!(
        target: "query_tracker_sql_debug",
        "Executing index creation query (no timeout - may take minutes)... ({})",
        sql_query
    );

    let stmt = Statement::from_string(db.get_database_backend(), sql_query.clone());
    let start_time = tokio::time::Instant::now();

    match db.execute(stmt).await {
        Ok(_) => {
            info!(
                target: "query_tracker_index_listener",
                "Successfully executed index creation (program: {}): {} (elapsed: {:?} secs)",
                program,
                index_name,
                start_time.elapsed().as_secs_f64()
            );
        }
        Err(e) => {
            if is_duplicate_index_error(&e) {
                tracing::debug!(
                    target: "query_tracker_index_listener",
                    "Index already exists, skipping: {}", sql_query
                );
            } else if is_connection_error(&e) {
                error!(
                    target: "query_tracker_index_listener",
                    "Database connection error during index creation: program: {}, index: {} - Error: {:?}",
                    program, index_name, e
                );
            } else {
                error!(
                    target: "query_tracker_index_listener",
                    "Failed to execute index creation: program: {}, index: {} - Error: {:?}",
                    program, index_name, e
                );
            }
        }
    }
}

fn is_duplicate_index_error(err: &DbErr) -> bool {
    match err {
        DbErr::Exec(runtime_err) => {
            let err_str = runtime_err.to_string().to_lowercase();
            err_str.contains("already exists")
                || err_str.contains("duplicate")
                || err_str.contains("42p07")
        }
        DbErr::Query(runtime_err) => {
            let err_str = runtime_err.to_string().to_lowercase();
            err_str.contains("already exists")
                || err_str.contains("duplicate")
                || err_str.contains("42p07")
        }
        _ => false,
    }
}

fn is_connection_error(err: &DbErr) -> bool {
    matches!(err, DbErr::ConnectionAcquire(_) | DbErr::Conn(_))
}
