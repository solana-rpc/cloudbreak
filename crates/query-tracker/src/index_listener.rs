// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::tracker;
use cloudbreak_core::QueryTrackerConfig;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, DbErr, Statement, TransactionTrait};
use solana_pubkey::Pubkey;
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;
use std::collections::{HashMap, HashSet};
use tracing::{error, info, warn};

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

const READ_AUTO_INDEX_USAGE_SQL: &str = "SELECT u.index_name, \
    COALESCE(SUM(s.idx_scan), 0)::bigint AS idx_scan, \
    COALESCE(SUM(pg_relation_size(COALESCE(t.relid, c.oid))), 0)::bigint AS bytes \
    FROM auto_index_usage u \
    JOIN pg_class c ON c.relname = u.index_name AND c.relkind IN ('i', 'I') \
    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public' \
    LEFT JOIN LATERAL pg_partition_tree(c.oid) t ON true \
    LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = COALESCE(t.relid, c.oid) \
    GROUP BY u.index_name";

#[tracing::instrument(name = "index_eviction_task", skip_all)]
pub async fn index_eviction_task(db: DatabaseConnection, config: QueryTrackerConfig) {
    if !config.index_eviction_enabled {
        info!(
            target: "query_tracker_index_eviction",
            "Index eviction disabled; task not running"
        );
        return;
    }

    let interval = config.index_eviction_interval;
    info!(
        target: "query_tracker_index_eviction",
        "Index eviction task started (interval: {:?}, min-idle: {:?}, min-age-grace: {:?})",
        interval, config.index_min_idle, config.index_min_age_grace
    );

    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = run_eviction_pass(&db, &config).await {
            error!(
                target: "query_tracker_index_eviction",
                "Index eviction pass failed: {:?}", e
            );
        }
    }
}

async fn run_eviction_pass(
    db: &DatabaseConnection,
    config: &QueryTrackerConfig,
) -> Result<(), DbErr> {
    let backend = db.get_database_backend();

    let track_counts_on = db
        .query_one(Statement::from_string(
            backend,
            "SELECT current_setting('track_counts') = 'on' AS enabled",
        ))
        .await?
        .map(|row| row.try_get::<bool>("", "enabled"))
        .transpose()?
        .unwrap_or(false);
    if !track_counts_on {
        warn!(
            target: "query_tracker_index_eviction",
            "track_counts is off; index scan stats are frozen — skipping eviction pass to avoid \
             dropping in-use indexes"
        );
        return Ok(());
    }

    db.execute(Statement::from_string(
        backend,
        "DELETE FROM auto_index_usage WHERE index_name NOT IN (\
            SELECT c.relname FROM pg_class c \
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public' \
            WHERE c.relkind IN ('i', 'I'))",
    ))
    .await?;

    let rows = db
        .query_all(Statement::from_string(backend, READ_AUTO_INDEX_USAGE_SQL))
        .await?;

    let mut index_bytes: HashMap<String, i64> = HashMap::new();

    for row in &rows {
        let name: String = row.try_get("", "index_name")?;
        let idx_scan: i64 = row.try_get("", "idx_scan")?;
        let bytes: i64 = row.try_get("", "bytes")?;
        index_bytes.insert(name.clone(), bytes);

        db.execute(Statement::from_sql_and_values(
            backend,
            "UPDATE auto_index_usage SET \
               last_seen_used = CASE WHEN $2 <> last_idx_scan THEN now() ELSE last_seen_used END, \
               last_idx_scan = $2 \
             WHERE index_name = $1",
            [name.into(), idx_scan.into()],
        ))
        .await?;
    }

    let min_idle_secs = config.index_min_idle.as_secs() as i64;
    let min_age_grace_secs = config.index_min_age_grace.as_secs() as i64;
    let candidate_rows = db
        .query_all(Statement::from_sql_and_values(
            backend,
            "SELECT index_name FROM auto_index_usage \
             WHERE EXTRACT(EPOCH FROM (now() - last_seen_used)) > $1 \
               AND EXTRACT(EPOCH FROM (now() - created_at)) > $2",
            [min_idle_secs.into(), min_age_grace_secs.into()],
        ))
        .await?;

    let candidates: HashSet<String> = candidate_rows
        .iter()
        .map(|r| r.try_get::<String>("", "index_name"))
        .collect::<Result<_, _>>()?;

    if candidates.is_empty() {
        return Ok(());
    }

    let existing: HashSet<String> = index_bytes.keys().cloned().collect();

    let to_evict = indexes_to_evict(&candidates, &existing);
    let mut reclaimed_bytes: i64 = 0;
    let mut evicted = 0usize;
    for idx in &to_evict {
        match drop_auto_index(db, backend, idx).await {
            Ok(()) => {
                reclaimed_bytes += index_bytes.get(idx).copied().unwrap_or(0);
                evicted += 1;
            }
            Err(e) => {
                warn!(
                    target: "query_tracker_index_eviction",
                    "Failed to evict idle auto-index '{}': {:?}; will retry next pass", idx, e
                );
            }
        }
    }

    if evicted > 0 {
        info!(
            target: "query_tracker_index_eviction",
            "Evicted {} idle auto-index(es), reclaiming ~{} bytes",
            evicted, reclaimed_bytes
        );
    }

    Ok(())
}

fn twin_index_name(name: &str) -> Option<String> {
    if let Some(suffix) = name.strip_prefix("idx_snapshot_accounts_") {
        (!suffix.is_empty()).then(|| format!("idx_accounts_{suffix}"))
    } else if let Some(suffix) = name.strip_prefix("idx_accounts_") {
        (!suffix.is_empty()).then(|| format!("idx_snapshot_accounts_{suffix}"))
    } else {
        None
    }
}

fn indexes_to_evict(candidates: &HashSet<String>, existing: &HashSet<String>) -> Vec<String> {
    candidates
        .iter()
        .filter(|name| match twin_index_name(name) {
            Some(twin) => candidates.contains(&twin) || !existing.contains(&twin),
            None => false,
        })
        .cloned()
        .collect()
}

fn is_safe_index_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

async fn drop_auto_index(
    db: &DatabaseConnection,
    backend: DbBackend,
    index_name: &str,
) -> Result<(), DbErr> {
    if !is_safe_index_identifier(index_name) {
        return Err(DbErr::Custom(format!(
            "refusing to drop index with unexpected name '{index_name}'"
        )));
    }

    let txn = db.begin().await?;
    txn.execute(Statement::from_string(
        backend,
        "SET LOCAL lock_timeout = '5s'".to_string(),
    ))
    .await?;
    txn.execute(Statement::from_string(
        backend,
        format!("DROP INDEX IF EXISTS {index_name}"),
    ))
    .await?;
    txn.execute(Statement::from_sql_and_values(
        backend,
        "DELETE FROM auto_index_usage WHERE index_name = $1",
        [index_name.into()],
    ))
    .await?;
    txn.commit().await?;

    Ok(())
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

            let full_name = format!("idx_{table_name}_{index_name}").to_lowercase();
            let register = Statement::from_sql_and_values(
                db.get_database_backend(),
                "INSERT INTO auto_index_usage (index_name, last_idx_scan, last_seen_used, created_at) \
                 VALUES ($1, 0, now(), now()) \
                 ON CONFLICT (index_name) DO UPDATE SET \
                   last_idx_scan = 0, last_seen_used = now(), created_at = now()",
                [full_name.as_str().into()],
            );
            if let Err(e) = db.execute(register).await {
                warn!(
                    target: "query_tracker_index_listener",
                    "Failed to register auto-index '{}' for eviction tracking: {:?}", full_name, e
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn twin_maps_between_the_two_tables() {
        assert_eq!(
            twin_index_name("idx_accounts_abc_o0l8").as_deref(),
            Some("idx_snapshot_accounts_abc_o0l8")
        );
        assert_eq!(
            twin_index_name("idx_snapshot_accounts_abc_o0l8").as_deref(),
            Some("idx_accounts_abc_o0l8")
        );
        assert_eq!(twin_index_name("pg_unrelated_index"), None);
        assert_eq!(twin_index_name("idx_accounts_"), None);
        assert_eq!(twin_index_name("idx_snapshot_accounts_"), None);
    }

    #[test]
    fn evicts_both_when_pair_is_idle() {
        let pair = set(&["idx_accounts_abc", "idx_snapshot_accounts_abc"]);
        let mut got = indexes_to_evict(&pair, &pair);
        got.sort();
        assert_eq!(
            got,
            vec![
                "idx_accounts_abc".to_string(),
                "idx_snapshot_accounts_abc".to_string()
            ]
        );
    }

    #[test]
    fn keeps_idle_index_when_twin_still_active() {
        let candidates = set(&["idx_snapshot_accounts_abc"]);
        let existing = set(&["idx_accounts_abc", "idx_snapshot_accounts_abc"]);
        assert!(indexes_to_evict(&candidates, &existing).is_empty());
    }

    #[test]
    fn evicts_orphan_when_twin_no_longer_exists() {
        let candidates = set(&["idx_snapshot_accounts_abc"]);
        let existing = set(&["idx_snapshot_accounts_abc"]);
        assert_eq!(
            indexes_to_evict(&candidates, &existing),
            vec!["idx_snapshot_accounts_abc".to_string()]
        );
    }

    #[test]
    fn ignores_names_outside_the_auto_index_convention() {
        let candidates = set(&["some_random_index"]);
        let existing = set(&["some_random_index"]);
        assert!(indexes_to_evict(&candidates, &existing).is_empty());
    }
}
