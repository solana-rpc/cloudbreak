// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Eviction — reclaiming index budget, carefully.
//!
//! One pass per `index-eviction-interval` that, in order:
//!
//! 0. **Latency regression guard** (when `index-regression-guard` is not `off`).
//!    Demand-side only, so it runs before the supply refresh and ignores the
//!    fill threshold: any created index measured *slower* than the pattern was
//!    without it (by `index-regression-ratio`, once both sides have
//!    `index-latency-min-samples`) is either warned about or dropped and marked
//!    `rejected`. A rejected pattern is not rebuilt until fresh without-index
//!    samples show the index would help again (see `creation`).
//! 1. **Refreshes supply.** Reads `idx_scan`/size per auto-index from Postgres
//!    (summed across partition leaves), folds it into each pattern's row and the
//!    per-index metrics. Skipped entirely if `track_counts` is off (stats
//!    frozen), since stale `idx_scan` must never drive a drop.
//! 2. **Flags discrepancies** (see `stats::discrepancy`) — records the
//!    demand-vs-supply verdict for observability only, and emits **one summary
//!    line per pass** (counts plus the worst offenders) rather than a warning per
//!    index. It does not gate drops: a starved-but-demanded index is safe simply
//!    because step 3 requires demand-idle (and optionally supply-idle when
//!    `use-supply-for-eviction` is on), so a still-demanded index is never a drop
//!    candidate.
//! 3. **Drops (unconditional trim to target)** — when the capped table is above
//!    the fill target (`floor(eviction-fill-threshold × max-auto-indexes)`), the
//!    idlest eligible pairs (no demand for `index-min-idle`, and — when
//!    `use-supply-for-eviction` — no scans either; older than
//!    `index-min-age-grace`) are dropped in ascending-score order until the
//!    table is back at the target. At or below the target nothing is dropped.
//!
//! Only eligible pairs are ever dropped — demand-idle (optionally also
//! supply-idle) and past the age grace — so a still-demanded or freshly built
//! index is never trimmed; if too few are eligible, the table simply stays above
//! target until more go idle.
//! The trim inherits the priority mode's notion of "value": lifetime-total modes
//! weigh an idle index by its *all-time* worth, while
//! [`Weighted`](cloudbreak_core::PriorityMode::Weighted) weighs **recent
//! windowed** activity, so decisions track current throughput.
//!
//! Drops are gated on indexer backpressure and run with a bounded `lock_timeout`
//! plus a configurable retry; a drop that cannot take its lock is logged and
//! left for the next pass rather than blocking ingest.

use crate::modules::store::patterns::status;
use crate::modules::store::{Store, prioritization};
use crate::modules::{CAP_TABLE, INDEX_TABLES, indexer_backpressure};
use crate::stats::discrepancy::{self, DiscrepancyState};
use crate::stats::metrics;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use cloudbreak_core::modules::service_health;
use cloudbreak_core::{IndexRegressionGuard, QueryTrackerConfig};
use sea_orm::{ConnectionTrait, DbErr, Statement, TransactionTrait};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Max indexes named in a per-pass summary log before it collapses the rest into
/// a `[+N more]` suffix — keeps the discrepancy/EXPLAIN summaries to one line.
const LOG_LIST_LIMIT: usize = 10;

/// How often the trim re-checks indexer load while paused on backpressure.
const BACKPRESSURE_POLL: Duration = Duration::from_secs(10);
/// Upper bound on how long a single eviction pass will wait out backpressure
/// before giving up and finishing the remainder on the next pass. Bounds the
/// pause so a permanently-overloaded indexer cannot make a pass hang forever.
const BACKPRESSURE_MAX_WAIT: Duration = Duration::from_secs(600);

/// Block until the indexer is no longer under pressure, or until
/// [`BACKPRESSURE_MAX_WAIT`] elapses. DDL (`DROP INDEX`) takes heavy locks, so
/// the trim pauses rather than hammering a busy indexer — but it *resumes* the
/// same pass once load recovers instead of abandoning the remaining drops until
/// the next hourly pass. Returns `true` when it is safe to proceed, `false` if
/// the wait timed out (caller should stop this pass and retry next time).
async fn wait_out_backpressure(config: &QueryTrackerConfig) -> bool {
    if !indexer_backpressure::is_under_pressure(
        &config.indexer_metrics,
        config.indexer_metrics_threshold,
    )
    .await
    {
        return true;
    }
    let start = Instant::now();
    info!(
        target: "query_tracker_eviction",
        "indexer under pressure; pausing eviction and waiting up to {}s for it to recover",
        BACKPRESSURE_MAX_WAIT.as_secs()
    );
    while start.elapsed() < BACKPRESSURE_MAX_WAIT {
        tokio::time::sleep(BACKPRESSURE_POLL).await;
        if !indexer_backpressure::is_under_pressure(
            &config.indexer_metrics,
            config.indexer_metrics_threshold,
        )
        .await
        {
            info!(
                target: "query_tracker_eviction",
                "indexer recovered after ~{:.0}s; resuming eviction",
                start.elapsed().as_secs_f64()
            );
            return true;
        }
    }
    false
}

#[tracing::instrument(name = "query_tracker_eviction", skip_all)]
pub async fn run(store: Store, config: QueryTrackerConfig) {
    info!(
        target: "query_tracker_eviction",
        "eviction task started (interval: {:?}, min-idle: {:?}, min-age-grace: {:?}, fill-threshold: {})",
        config.index_eviction_interval, config.index_min_idle, config.index_min_age_grace,
        config.eviction_fill_threshold
    );

    loop {
        tokio::time::sleep(config.index_eviction_interval).await;
        if let Err(e) = run_pass(&store, &config).await {
            error!(target: "query_tracker_eviction", "eviction pass failed: {e:?}");
        }
    }
}

async fn run_pass(store: &Store, config: &QueryTrackerConfig) -> Result<(), DbErr> {
    // Latency regression guard first. It compares demand-side with/without-index
    // cost, so it needs neither Postgres stats nor the fill threshold — a
    // harmful index is dropped even when the table has room.
    if config.index_regression_guard != IndexRegressionGuard::Off {
        run_regression_guard(store, config).await?;
    }

    if !store.track_counts_enabled().await? {
        warn!(
            target: "query_tracker_eviction",
            "track_counts is off; idx_scan is frozen — skipping pass to avoid dropping in-use indexes"
        );
        return Ok(());
    }

    let supply = supply_by_pattern(store).await?;
    refresh_supply_and_discrepancy(store, config, &supply).await?;
    refresh_aggregate_metrics(store).await;

    // Dropping requires a cap to define the target; without one we never evict.
    let Some(max) = config.max_auto_indexes else {
        return Ok(());
    };
    let current = store.count_table_indexes(CAP_TABLE).await?;
    metrics::SNAPSHOT_ACCOUNTS_INDEXES.set(current);
    // The operating size we trim back to. The band (target, max] is a buffer the
    // creation-time value guard only lets valuable indexes enter; here we simply
    // reclaim back down to `target`.
    let target = (config.eviction_fill_threshold * max as f64).floor() as i64;
    if current <= target {
        return Ok(());
    }

    let min_idle = config.index_min_idle.as_secs() as i64;
    let min_age = config.index_min_age_grace.as_secs() as i64;
    // Eligible drops, least-useful first (idle + past age grace; supply-idle
    // only when `use-supply-for-eviction` is on).
    let candidates = store
        .eviction_candidates(
            config.priority_mode,
            config.without_index_compensation_factor,
            min_idle,
            min_age,
            config.use_supply_for_eviction,
        )
        .await?;
    if candidates.is_empty() {
        return Ok(());
    }

    let mut count = current;
    let mut evicted = 0usize;
    let mut reclaimed: i64 = 0;

    // Optionally drain traffic off the node for the duration of the trim: mark it
    // unhealthy before the loop and healthy again after, so a load balancer reading
    // the shared health flag routes around the DROP INDEX lock spikes.
    if config.mark_unhealthy_for_eviction
        && let Err(e) = service_health::update_service_health(store.db(), false).await
    {
        error!(
            target: "query_tracker_eviction",
            "failed to mark node unhealthy before eviction trim: {e:?}; continuing anyway"
        );
    }

    // Unconditional trim: drop the least-valuable eligible pairs until back at
    // the target (entry into the buffer was already value-gated at creation).
    // Timed end-to-end (wall clock, incl. backpressure waits and drop retries).
    let loop_start = Instant::now();
    for scored in &candidates {
        let row = &scored.row;
        if count <= target {
            break;
        }
        // Pause (not abort) while the indexer is busy: DROP INDEX takes heavy
        // locks, so we wait for load to recover and resume this same pass. Only
        // give up if it never clears within the bounded wait.
        if !wait_out_backpressure(config).await {
            warn!(
                target: "query_tracker_eviction",
                "indexer still under pressure after {}s; stopping trim early ({evicted} evicted), \
                 will finish next pass",
                BACKPRESSURE_MAX_WAIT.as_secs()
            );
            break;
        }
        let identity = match row.identity() {
            Ok(i) => i,
            Err(e) => {
                error!(target: "query_tracker_eviction", "skipping eviction candidate: {e}");
                continue;
            }
        };
        match drop_pair(store, &identity, config).await {
            Ok(()) => {
                if let Err(e) = store.mark_evicted(&row.pattern_id).await {
                    error!(target: "query_tracker_eviction", "failed to mark evicted: {e:?}");
                }
                metrics::INDEX_EVICTED_TOTAL.inc();
                clear_index_metrics(&row.human_name);
                reclaimed += row.index_bytes;
                evicted += 1;
                count -= 1;
            }
            Err(e) => {
                warn!(
                    target: "query_tracker_eviction",
                    "could not evict '{}' — {}",
                    identity.human_name(),
                    drop_failure_detail(&e, config.drop_lock_timeout.as_millis())
                );
            }
        }
    }

    let elapsed_ms = loop_start.elapsed().as_millis();

    if config.mark_unhealthy_for_eviction
        && let Err(e) = service_health::update_service_health(store.db(), true).await
    {
        error!(
            target: "query_tracker_eviction",
            "failed to restore node healthy after eviction trim: {e:?}"
        );
    }

    info!(
        target: "query_tracker_eviction",
        "eviction trim loop finished in {elapsed_ms}ms: evicted {evicted} idle index pair(s) \
         to return to fill target ({target}/{max}), reclaiming ~{reclaimed} bytes"
    );
    Ok(())
}

/// Latency regression guard: find created indexes that measure **slower** than
/// the pattern was without them and, per `index-regression-guard`, either warn
/// or drop-and-`reject` them. Drops honour backpressure and the same
/// `lock_timeout`/retry path as idle eviction; a rejected pattern is not rebuilt
/// until [`Store::promote_recovered_rejections`] sees fresh contrary evidence.
async fn run_regression_guard(store: &Store, config: &QueryTrackerConfig) -> Result<(), DbErr> {
    let rows = store
        .regression_candidates(
            config.index_min_age_grace.as_secs() as i64,
            config.index_regression_ratio,
            config.without_index_compensation_factor,
        )
        .await?;

    // Current count of created indexes in the regressed state (before any drops
    // this pass move them to `rejected`).
    metrics::REGRESSED_INDEXES.set(rows.len() as i64);

    for row in &rows {
        let avg_with = avg_us(row.cost_with_index_us, row.cost_with_index_count);
        let avg_without_raw = avg_us(row.cost_without_index_us, row.cost_without_index_count);
        let avg_without = avg_without_raw * config.without_index_compensation_factor;

        match config.index_regression_guard {
            IndexRegressionGuard::Warn => {
                warn!(
                    target: "query_tracker_regression",
                    "index '{}' is slower WITH the index than without: with~{avg_with:.0}us vs \
                     without~{avg_without:.0}us (raw~{avg_without_raw:.0}us ×{:.2} compensation; \
                     >{:.2}x; {}/{} with/without requests); warn mode, keeping it",
                    row.human_name, config.without_index_compensation_factor,
                    config.index_regression_ratio,
                    row.cost_with_index_count, row.cost_without_index_count
                );
            }
            IndexRegressionGuard::Evict => {
                // Same pause-and-resume as the idle trim: wait out indexer load
                // rather than abandoning the remaining regression drops.
                if !wait_out_backpressure(config).await {
                    warn!(
                        target: "query_tracker_regression",
                        "indexer still under pressure after {}s; deferring remaining regression drops \
                         to next pass",
                        BACKPRESSURE_MAX_WAIT.as_secs()
                    );
                    break;
                }
                let identity = match row.identity() {
                    Ok(i) => i,
                    Err(e) => {
                        error!(target: "query_tracker_regression", "skipping regressed pattern: {e}");
                        continue;
                    }
                };
                match drop_pair(store, &identity, config).await {
                    Ok(()) => {
                        if let Err(e) = store.mark_rejected(&row.pattern_id).await {
                            error!(target: "query_tracker_regression", "failed to mark rejected: {e:?}");
                        }
                        metrics::INDEX_EVICTED_TOTAL.inc();
                        clear_index_metrics(&row.human_name);
                        warn!(
                            target: "query_tracker_regression",
                            "dropped regressed index '{}' (with~{avg_with:.0}us > without~{avg_without:.0}us); \
                             marked rejected — will not rebuild until it is slower without it",
                            row.human_name
                        );
                    }
                    Err(e) => warn!(
                        target: "query_tracker_regression",
                        "could not drop regressed index '{}' — {}",
                        identity.human_name(),
                        drop_failure_detail(&e, config.drop_lock_timeout.as_millis())
                    ),
                }
            }
            IndexRegressionGuard::Off => {}
        }
    }
    Ok(())
}

/// Average microseconds per request for a `(sum_us, count)` bucket.
fn avg_us(sum_us: i64, count: i64) -> f64 {
    sum_us as f64 / count.max(1) as f64
}

/// Aggregate the raw `(index_name, idx_scan, bytes)` rows into per-`pattern_id`
/// supply by stripping the table prefix and summing the pair.
async fn supply_by_pattern(store: &Store) -> Result<HashMap<String, (i64, i64)>, DbErr> {
    let mut by_pattern: HashMap<String, (i64, i64)> = HashMap::new();
    for (name, idx_scan, bytes) in store.read_auto_index_supply().await? {
        let Some(pattern_id) = strip_index_prefix(&name) else {
            continue;
        };
        let entry = by_pattern.entry(pattern_id).or_default();
        entry.0 += idx_scan;
        entry.1 += bytes;
    }
    Ok(by_pattern)
}

/// Fold supply into each created pattern, update per-index metrics, and evaluate
/// discrepancies.
async fn refresh_supply_and_discrepancy(
    store: &Store,
    config: &QueryTrackerConfig,
    supply: &HashMap<String, (i64, i64)>,
) -> Result<(), DbErr> {
    // Per-pass discrepancy tally, emitted as one summary line at the end instead
    // of a warning per index. `flagged` collects `(name, ratio, demand, supply)`
    // for the non-`ok` verdicts so the summary can name the worst offenders.
    let mut checked = 0i64;
    let mut ok = 0i64;
    let mut starved: Vec<(String, f64, i64, i64)> = Vec::new();
    let mut over_scanned: Vec<(String, f64, i64, i64)> = Vec::new();

    for row in store.list_created().await? {
        let (idx_scan, bytes) = supply.get(&row.pattern_id).copied().unwrap_or((0, 0));
        store
            .update_supply(&row.pattern_id, idx_scan, bytes)
            .await?;

        // A served request scans both tables of the pair, so raw idx_scan is ~2x
        // the request count; every surface (metric, log, endpoint) reports the
        // compensated value so supply lines up ~1:1 with demand.
        let compensated_idx_scan = idx_scan / prioritization::SCANS_PER_REQUEST;
        metrics::INDEX_COMPENSATED_IDX_SCAN
            .with_label_values(&[&row.human_name])
            .set(compensated_idx_scan);
        metrics::INDEX_DEMAND
            .with_label_values(&[&row.human_name])
            .set(row.demand_count);
        metrics::INDEX_VARIETY
            .with_label_values(&[&row.human_name])
            .set(row.variety_estimate);

        if config.discrepancy_enabled {
            let demand_since_create = (row.demand_count - row.demand_at_create).max(0);
            let (state, ratio) = discrepancy::evaluate(
                demand_since_create,
                compensated_idx_scan,
                config.discrepancy_delta,
                config.index_generation_threshold as i64,
            );
            store
                .set_discrepancy(&row.pattern_id, state.as_stored(), ratio)
                .await?;
            checked += 1;
            let entry = (
                row.human_name.clone(),
                ratio.unwrap_or(0.0),
                demand_since_create,
                compensated_idx_scan,
            );
            match state {
                DiscrepancyState::Ok => ok += 1,
                DiscrepancyState::Starved => starved.push(entry),
                DiscrepancyState::OverScanned => over_scanned.push(entry),
            }
        }
    }

    if config.discrepancy_enabled {
        log_discrepancy_summary(checked, ok, starved, over_scanned);
    }
    Ok(())
}

/// One line per discrepancy pass instead of a warning per index. `warn` when any
/// index is **starved** (Postgres ignoring a wanted index — actionable); `info`
/// otherwise (all healthy, or only the informational over-scanned case).
fn log_discrepancy_summary(
    checked: i64,
    ok: i64,
    mut starved: Vec<(String, f64, i64, i64)>,
    mut over_scanned: Vec<(String, f64, i64, i64)>,
) {
    if starved.is_empty() && over_scanned.is_empty() {
        info!(
            target: "query_tracker_discrepancy",
            "discrepancy pass: {checked} checked — all ok"
        );
        return;
    }

    let mut detail = format!(
        "discrepancy pass: {checked} checked — {ok} ok, {} starved, {} over_scanned",
        starved.len(),
        over_scanned.len()
    );
    if !starved.is_empty() {
        // Worst = lowest ratio (least supply for the demand).
        starved.sort_by(|a, b| a.1.total_cmp(&b.1));
        detail.push_str(&format!("; starved: {}", fmt_flagged(&starved)));
    }
    if !over_scanned.is_empty() {
        // Worst = highest ratio (most excess scanning).
        over_scanned.sort_by(|a, b| b.1.total_cmp(&a.1));
        detail.push_str(&format!("; over_scanned: {}", fmt_flagged(&over_scanned)));
    }

    if starved.is_empty() {
        info!(target: "query_tracker_discrepancy", "{detail}");
    } else {
        warn!(target: "query_tracker_discrepancy", "{detail}");
    }
}

/// Render up to [`LOG_LIST_LIMIT`] already-sorted flagged indexes as
/// `name (ratio R, demand~D/supply~S)`, with a `[+N more]` suffix when truncated.
fn fmt_flagged(list: &[(String, f64, i64, i64)]) -> String {
    let shown = list
        .iter()
        .take(LOG_LIST_LIMIT)
        .map(|(name, ratio, demand, supply)| {
            format!("{name} (ratio {ratio:.2}, demand~{demand}/supply~{supply})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    if list.len() > LOG_LIST_LIMIT {
        format!("{shown} [+{} more]", list.len() - LOG_LIST_LIMIT)
    } else {
        shown
    }
}

async fn refresh_aggregate_metrics(store: &Store) {
    match store.counts().await {
        Ok(c) => {
            metrics::PATTERNS
                .with_label_values(&[status::CREATED])
                .set(c.created);
            metrics::PATTERNS
                .with_label_values(&[status::CANDIDATE])
                .set(c.candidate);
            metrics::PATTERNS
                .with_label_values(&[status::EVICTED])
                .set(c.evicted);
            metrics::PATTERNS
                .with_label_values(&[status::REJECTED])
                .set(c.rejected);
            metrics::DISCREPANT_INDEXES.set(c.discrepant);
        }
        Err(e) => error!(target: "query_tracker_eviction", "failed to refresh metrics: {e:?}"),
    }
}

/// Failure of a single `DROP INDEX`, carrying enough context for the caller to
/// emit **one** consolidated log line.
enum DropError {
    /// Every attempt hit `lock_timeout`. `attempts` is the total tries made
    /// (`1 + drop-retries`); the caller reports the configured timeout itself.
    LockTimeout { index: String, attempts: u32 },
    /// Any other DB error (or an unsafe identifier) — not retried.
    Other { index: String, msg: String },
}

/// Drop both sides of an index pair, honouring `lock_timeout` and retries. On
/// failure returns the [`DropError`] for the side that failed; logging is left
/// to the caller so a failed drop is a single line.
async fn drop_pair(
    store: &Store,
    identity: &IndexIdentity,
    config: &QueryTrackerConfig,
) -> Result<(), DropError> {
    for table in INDEX_TABLES {
        drop_one(store, &identity.pg_index_name(table), config).await?;
    }
    Ok(())
}

/// Attempt `DROP INDEX` with a bounded `lock_timeout`, retrying only on lock
/// timeout up to `drop-retries`. Silent by design — it classifies the outcome
/// into [`DropError`] and lets the caller log once.
async fn drop_one(
    store: &Store,
    index_name: &str,
    config: &QueryTrackerConfig,
) -> Result<(), DropError> {
    if !is_safe_index_identifier(index_name) {
        return Err(DropError::Other {
            index: index_name.to_string(),
            msg: "refusing to drop unexpected index name".to_string(),
        });
    }
    let backend = store.db().get_database_backend();
    let lock_ms = config.drop_lock_timeout.as_millis();

    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let result: Result<(), DbErr> = async {
            let txn = store.db().begin().await?;
            txn.execute(Statement::from_string(
                backend,
                format!("SET LOCAL lock_timeout = '{lock_ms}ms'"),
            ))
            .await?;
            txn.execute(Statement::from_string(
                backend,
                format!("DROP INDEX IF EXISTS {index_name}"),
            ))
            .await?;
            txn.commit().await
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                let is_lock_timeout = msg.to_lowercase().contains("lock timeout")
                    || msg.to_lowercase().contains("55p03");
                // Retry silently while under the retry budget; the final count is
                // folded into the caller's single line.
                if is_lock_timeout && attempts <= config.drop_retries {
                    continue;
                }
                return Err(if is_lock_timeout {
                    DropError::LockTimeout {
                        index: index_name.to_string(),
                        attempts,
                    }
                } else {
                    DropError::Other {
                        index: index_name.to_string(),
                        msg,
                    }
                });
            }
        }
    }
}

/// Tail of the one-line drop-failure log for `e`: which physical index blocked
/// and why. Shared by the idle-eviction and regression-guard call sites so both
/// read identically; the verb ("could not evict" / "could not drop regressed
/// index") and identity are supplied by the caller.
fn drop_failure_detail(e: &DropError, lock_ms: u128) -> String {
    match e {
        DropError::LockTimeout { index, attempts } => format!(
            "lock timeout dropping index '{index}' after {attempts} attempt(s) \
             (lock_timeout={lock_ms}ms); left in place, will retry next pass"
        ),
        DropError::Other { index, msg } => {
            format!("dropping index '{index}' failed: {msg}; will retry next pass")
        }
    }
}

fn clear_index_metrics(human_name: &str) {
    let _ = metrics::INDEX_COMPENSATED_IDX_SCAN.remove_label_values(&[human_name]);
    let _ = metrics::INDEX_DEMAND.remove_label_values(&[human_name]);
    let _ = metrics::INDEX_VARIETY.remove_label_values(&[human_name]);
}

/// `idx_accounts_<id>` / `idx_snapshot_accounts_<id>` -> `<id>`.
fn strip_index_prefix(name: &str) -> Option<String> {
    name.strip_prefix("idx_snapshot_accounts_")
        .or_else(|| name.strip_prefix("idx_accounts_"))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn is_safe_index_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_both_prefixes() {
        assert_eq!(
            strip_index_prefix("idx_accounts_abc123").as_deref(),
            Some("abc123")
        );
        assert_eq!(
            strip_index_prefix("idx_snapshot_accounts_abc123").as_deref(),
            Some("abc123")
        );
        assert_eq!(strip_index_prefix("pg_toast_index"), None);
        assert_eq!(strip_index_prefix("idx_accounts_"), None);
    }

    #[test]
    fn rejects_unsafe_names() {
        assert!(is_safe_index_identifier("idx_accounts_abc123"));
        assert!(!is_safe_index_identifier("idx_accounts_abc; DROP TABLE"));
        assert!(!is_safe_index_identifier(""));
    }
}
