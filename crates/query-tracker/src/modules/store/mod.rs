// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Store — the only place that speaks SQL.
//!
//! Everything the tracker knows lives in the `index_patterns` table; this
//! module owns the connection and exposes typed operations over it (record
//! demand, pick candidates, update supply, evict, flag discrepancies, read
//! counts). No SQL or column name leaks past this boundary, which keeps the
//! pipeline modules (`ingest`, `creation`, `eviction`, ...) readable and makes
//! the schema easy to evolve.
//!
//! The tracker is a single service, so the per-identity variety sketch
//! ([`VarietySketch`]) is simply loaded, folded, and written back inside a row
//! transaction — no cross-process HLL merge is needed.

pub mod patterns;
pub mod prioritization;

use crate::stats::variety::VarietySketch;
use cloudbreak_core::PriorityMode;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use patterns::{PatternRow, offsets_from_json, offsets_to_json, status};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbErr, QueryResult, Statement, TransactionTrait, Value,
};
use std::collections::HashSet;

/// Columns selected into a [`PatternRow`]; kept in one place so every read
/// stays in sync with [`row_to_pattern`].
const PATTERN_COLUMNS: &str = "pattern_id, program, human_name, offsets_lengths, datasize, \
     example_request, \
     demand_count, demand_at_create, total_cost_us, failed_count, \
     cost_with_index_us, cost_with_index_count, cost_without_index_us, cost_without_index_count, \
     variety_estimate, status, last_idx_scan, index_bytes, discrepancy_state, discrepancy_ratio, \
     explain_state, EXTRACT(EPOCH FROM created_at)::float8 AS created_at_epoch";

/// Shared `WHERE` fragment defining an **eviction-eligible** created index:
/// past `index-min-age-grace` (`$1`) and demand-idle for `index-min-idle` (`$2`).
/// When `use_supply` is set, also requires supply-idle (`last_seen_used`) for the
/// same window — see `use-supply-for-eviction`. Used by both
/// [`Store::eviction_candidates`] and [`Store::eviction_boundary_score`] so the
/// eviction trim and the creation-time value guard share one definition of
/// "droppable" and can never drift apart.
fn eviction_gate(use_supply: bool) -> String {
    let supply_idle = if use_supply {
        "AND EXTRACT(EPOCH FROM (now() - COALESCE(last_seen_used, created_at, first_seen_at))) > $2 "
    } else {
        ""
    };
    format!(
        "status = '{created}' \
         AND EXTRACT(EPOCH FROM (now() - COALESCE(created_at, first_seen_at))) > $1 \
         {supply_idle}\
         AND EXTRACT(EPOCH FROM (now() - COALESCE(last_demand_at, first_seen_at))) > $2",
        created = status::CREATED,
    )
}

/// Aggregate counts for metrics. `created`/`candidate`/`evicted`/`rejected`
/// partition the table by lifecycle status (their sum is the total);
/// `discrepant` is a sub-count of `created` and orthogonal to status.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreCounts {
    pub created: i64,
    pub candidate: i64,
    pub evicted: i64,
    pub rejected: i64,
    pub discrepant: i64,
}

/// A pattern paired with its [`PriorityMode`] score and, for
/// [`PriorityMode::Weighted`], the three factors whose product **is** that score
/// (`avg_cost_us`, the latency `gain` multiplier, and the `counts` multiplier;
/// see [`prioritization::score_component_exprs`]). The factors are `None` for the
/// single-quantity modes, which do not decompose. The debug endpoints show them
/// beside the score; creation and eviction read only `score`.
pub struct ScoredPattern {
    pub row: PatternRow,
    pub score: f64,
    pub components: Option<(f64, f64, f64)>,
}

/// SQL select-list tail: the score plus, for `Weighted`, its three component
/// columns. Non-`Weighted` modes select `NULL::float8` for the components so
/// every row parses through [`row_to_scored`] identically.
fn score_select(mode: PriorityMode, compensation: f64) -> String {
    let score = prioritization::score_expr(mode, compensation);
    match prioritization::score_component_exprs(mode, compensation) {
        Some((avg, gain, counts)) => format!(
            "({score})::float8 AS score, ({avg})::float8 AS score_avg, \
             ({gain})::float8 AS score_gain, ({counts})::float8 AS score_counts"
        ),
        None => format!(
            "({score})::float8 AS score, NULL::float8 AS score_avg, \
             NULL::float8 AS score_gain, NULL::float8 AS score_counts"
        ),
    }
}

/// Parse a row selected with [`score_select`] into a [`ScoredPattern`]. Components
/// are `Some` only when all three columns are non-NULL (i.e. `Weighted`).
fn row_to_scored(r: &QueryResult) -> Result<ScoredPattern, DbErr> {
    let row = row_to_pattern(r)?;
    let score = r.try_get::<f64>("", "score")?;
    let components = match (
        r.try_get::<Option<f64>>("", "score_avg")?,
        r.try_get::<Option<f64>>("", "score_gain")?,
        r.try_get::<Option<f64>>("", "score_counts")?,
    ) {
        (Some(avg), Some(gain), Some(counts)) => Some((avg, gain, counts)),
        _ => None,
    };
    Ok(ScoredPattern {
        row,
        score,
        components,
    })
}

/// Typed access to the `index_patterns` table.
#[derive(Clone)]
pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Escape hatch for the few catalog reads that are not about
    /// `index_patterns` (pg_stat / pg_class), used by `creation`/`eviction`.
    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }

    // ---- ingest -----------------------------------------------------------

    /// Fold one aggregated observation into the pattern's row: bump demand,
    /// cost and failure counters, refresh `last_demand_at`, resurrect an evicted
    /// pattern back to `candidate`, and merge value fingerprints into the
    /// variety sketch. Runs in a transaction so the sketch read-modify-write is
    /// consistent under concurrent `/track` requests.
    pub async fn record_demand(
        &self,
        identity: &IndexIdentity,
        count: u32,
        cost_us: u64,
        failed: u32,
        fingerprints: &HashSet<u64>,
        example_request: Option<serde_json::Value>,
    ) -> Result<(), DbErr> {
        let backend = self.db.get_database_backend();
        let pattern_id = identity.pattern_id();

        let txn = self.db.begin().await?;

        // New rows are always `candidate` (no index yet), so their first cost
        // goes to the without-index bucket. On update we route by the row's
        // current status: `created` → with-index bucket, anything else →
        // without-index bucket. ($6 = count, $7 = cost are reused for the
        // without-index columns on insert.) `example_request` ($9) is set once on
        // first insert and then kept (COALESCE), giving a stable EXPLAIN probe.
        let insert = Statement::from_sql_and_values(
            backend,
            "INSERT INTO index_patterns \
               (pattern_id, program, human_name, offsets_lengths, datasize, example_request, \
                demand_count, total_cost_us, failed_count, \
                cost_without_index_us, cost_without_index_count, last_demand_at) \
             VALUES ($1, $2, $3, $4, $5, $9, $6, $7, $8, $7, $6, now()) \
             ON CONFLICT (pattern_id) DO UPDATE SET \
               demand_count = index_patterns.demand_count + EXCLUDED.demand_count, \
               total_cost_us = index_patterns.total_cost_us + EXCLUDED.total_cost_us, \
               failed_count = index_patterns.failed_count + EXCLUDED.failed_count, \
               cost_with_index_us = index_patterns.cost_with_index_us \
                 + CASE WHEN index_patterns.status = 'created' THEN EXCLUDED.total_cost_us ELSE 0 END, \
               cost_with_index_count = index_patterns.cost_with_index_count \
                 + CASE WHEN index_patterns.status = 'created' THEN EXCLUDED.demand_count ELSE 0 END, \
               cost_without_index_us = index_patterns.cost_without_index_us \
                 + CASE WHEN index_patterns.status = 'created' THEN 0 ELSE EXCLUDED.total_cost_us END, \
               cost_without_index_count = index_patterns.cost_without_index_count \
                 + CASE WHEN index_patterns.status = 'created' THEN 0 ELSE EXCLUDED.demand_count END, \
               example_request = COALESCE(index_patterns.example_request, EXCLUDED.example_request), \
               last_demand_at = now(), \
               status = CASE WHEN index_patterns.status = 'evicted' \
                             THEN 'candidate' ELSE index_patterns.status END \
             RETURNING variety_hll",
            [
                pattern_id.clone().into(),
                identity.program.to_bytes().to_vec().into(),
                identity.human_name().into(),
                Value::Json(Some(Box::new(offsets_to_json(
                    &identity.filter.offsets_lengths,
                )))),
                identity.filter.datasize.map(|d| d as i64).into(),
                (count as i64).into(),
                (cost_us as i64).into(),
                (failed as i64).into(),
                Value::Json(example_request.map(Box::new)),
            ],
        );

        let existing_hll: Option<Vec<u8>> = txn
            .query_one(insert)
            .await?
            .and_then(|row| row.try_get::<Option<Vec<u8>>>("", "variety_hll").ok())
            .flatten();

        if !fingerprints.is_empty() {
            let mut sketch = VarietySketch::from_bytes(existing_hll.as_deref());
            sketch.insert_many(fingerprints.iter().copied());
            let hll_bytes = sketch.to_bytes();
            let estimate = sketch.estimate() as i64;
            txn.execute(Statement::from_sql_and_values(
                backend,
                "UPDATE index_patterns SET variety_hll = $2, variety_estimate = $3 \
                 WHERE pattern_id = $1",
                [pattern_id.into(), hll_bytes.into(), estimate.into()],
            ))
            .await?;
        }

        txn.commit().await
    }

    // ---- creation ---------------------------------------------------------

    /// Highest-priority creation candidates, ranked per [`PriorityMode`], each
    /// with its numeric score — the exact SQL ranking value — so callers can
    /// show operators (or feed the value guard) the number driving the order
    /// without re-deriving it in Rust. Numeric parameters are embedded as
    /// literals (they are never user-controlled), which keeps the dynamic
    /// ORDER BY / cost gate simple.
    pub async fn top_candidates_scored(
        &self,
        mode: PriorityMode,
        compensation: f64,
        threshold: u32,
        cost_eligibility_us: Option<u64>,
        limit: u64,
    ) -> Result<Vec<ScoredPattern>, DbErr> {
        let select = score_select(mode, compensation);
        let cost_gate = match cost_eligibility_us {
            Some(us) => format!(" AND (total_cost_us::float8 / GREATEST(demand_count, 1)) >= {us}"),
            None => String::new(),
        };
        let sql = format!(
            "SELECT {PATTERN_COLUMNS}, {select} FROM index_patterns \
             WHERE status = '{candidate}' AND demand_count >= {threshold}{cost_gate} \
             ORDER BY score DESC LIMIT {limit}",
            candidate = status::CANDIDATE,
        );
        let rows = self
            .db
            .query_all(Statement::from_string(self.db.get_database_backend(), sql))
            .await?;
        rows.iter().map(row_to_scored).collect()
    }

    /// Mark a pattern as created and (re)anchor its supply clock to now.
    pub async fn mark_created(&self, pattern_id: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET status = 'created', created_at = now(), \
                   last_seen_used = now(), last_idx_scan = 0, last_create_error = NULL, \
                   evicted_at = NULL, demand_at_create = demand_count, \
                   idx_scan_prev = 0, supply_rate = NULL \
                 WHERE pattern_id = $1",
                [pattern_id.into()],
            ))
            .await
            .map(|_| ())
    }

    /// Record a failed creation attempt (keeps the pattern a candidate so it is
    /// retried on a later pass).
    pub async fn mark_create_failed(&self, pattern_id: &str, error: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET create_attempts = create_attempts + 1, \
                   last_create_error = $2 WHERE pattern_id = $1",
                [pattern_id.into(), error.into()],
            ))
            .await
            .map(|_| ())
    }

    // ---- supply / eviction ------------------------------------------------

    /// All patterns currently in the `created` state.
    pub async fn list_created(&self) -> Result<Vec<PatternRow>, DbErr> {
        let sql = format!(
            "SELECT {PATTERN_COLUMNS} FROM index_patterns WHERE status = '{}'",
            status::CREATED
        );
        let rows = self
            .db
            .query_all(Statement::from_string(self.db.get_database_backend(), sql))
            .await?;
        rows.iter().map(row_to_pattern).collect()
    }

    /// All `created` patterns, each with its [`PriorityMode`] score — the same
    /// ranking value the creation queue (`top_candidates_scored`) and eviction
    /// order use, so the `/debug/created` view can show operators exactly where a
    /// live index sits in that one shared ordering.
    pub async fn list_created_scored(
        &self,
        mode: PriorityMode,
        compensation: f64,
    ) -> Result<Vec<ScoredPattern>, DbErr> {
        let select = score_select(mode, compensation);
        let sql = format!(
            "SELECT {PATTERN_COLUMNS}, {select} FROM index_patterns \
             WHERE status = '{}'",
            status::CREATED
        );
        let rows = self
            .db
            .query_all(Statement::from_string(self.db.get_database_backend(), sql))
            .await?;
        rows.iter().map(row_to_scored).collect()
    }

    /// Update a pattern's supply columns; bump `last_seen_used` only when the
    /// scan counter actually moved (a change is the only reliable "was used"
    /// signal, and it survives stat resets — a reset just restarts the clock).
    pub async fn update_supply(
        &self,
        pattern_id: &str,
        idx_scan: i64,
        bytes: i64,
    ) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET \
                   index_bytes = $2, \
                   last_seen_used = CASE WHEN $3 <> last_idx_scan OR last_seen_used IS NULL \
                                        THEN now() ELSE last_seen_used END, \
                   last_idx_scan = $3 \
                 WHERE pattern_id = $1",
                [pattern_id.into(), bytes.into(), idx_scan.into()],
            ))
            .await
            .map(|_| ())
    }

    /// Roll the windowed score counters: store the count observed since the last
    /// roll (`current - prev`, clamped ≥ 0 to absorb Postgres stat resets) in the
    /// `*_rate` columns, then snapshot the current totals into `*_prev`. Run once
    /// per `score-rate-window`; only [`PriorityMode::Weighted`] reads the result.
    /// Returns the number of rows rolled.
    pub async fn roll_scores(&self) -> Result<u64, DbErr> {
        let res = self
            .db
            .execute(Statement::from_string(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET \
                   demand_rate = GREATEST(demand_count - demand_prev, 0), \
                   failed_rate = GREATEST(failed_count - failed_prev, 0), \
                   supply_rate = GREATEST(last_idx_scan - idx_scan_prev, 0), \
                   demand_prev = demand_count, \
                   failed_prev = failed_count, \
                   idx_scan_prev = last_idx_scan"
                    .to_string(),
            ))
            .await?;
        Ok(res.rows_affected())
    }

    /// Created patterns idle for longer than `min_idle_secs` and older than
    /// `min_age_secs`. Demand-idle is always required; supply-idle
    /// (`last_seen_used`) is required only when `use_supply` is set (see
    /// `use-supply-for-eviction`). Demand-idle alone already avoids the
    /// drop→slow→rebuild churn loop: a pattern the API is still demanding is
    /// never idle.
    ///
    /// Rows come back in **drop-priority order — least useful first** — each
    /// paired with its numeric score: the same [`prioritization::score_expr`]
    /// the creation loop ranks by, but ascending, so eviction drops exactly what
    /// creation would build last. The eviction pass drops from the top until the
    /// table is back at its fill target. Ordering lives here, next to the
    /// eligibility filter, so the whole "what may be dropped, and in what order"
    /// decision is one SQL statement. `pattern_id` breaks ties deterministically
    /// (idle rows share the same low score). The score is also what the
    /// creation-time value guard compares a new candidate against (via
    /// [`Store::eviction_boundary_score`]).
    pub async fn eviction_candidates(
        &self,
        mode: PriorityMode,
        compensation: f64,
        min_idle_secs: i64,
        min_age_secs: i64,
        use_supply: bool,
    ) -> Result<Vec<ScoredPattern>, DbErr> {
        let select = score_select(mode, compensation);
        let sql = format!(
            "SELECT {PATTERN_COLUMNS}, {select} FROM index_patterns \
             WHERE {gate} \
             ORDER BY score ASC, pattern_id ASC",
            gate = eviction_gate(use_supply),
        );
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                sql,
                [min_age_secs.into(), min_idle_secs.into()],
            ))
            .await?;
        rows.iter().map(row_to_scored).collect()
    }

    /// Score of the eviction candidate at position `offset` in drop-priority
    /// order (least useful first) — the index that a newly created index would
    /// displace when the table is already `offset` indexes above the fill
    /// target. This is the boundary the creation-time value guard compares a new
    /// candidate against: a candidate is only built when it out-scores this.
    ///
    /// Same eligibility gate and scoring as [`Store::eviction_candidates`], so
    /// the two sides of the guard are strictly comparable. Returns `None` when
    /// there is no eligible index at `offset` (fewer reclaimable indexes than the
    /// overflow), which the guard treats as "nothing to displace → build anyway
    /// up to the hard cap and let eviction reclaim later".
    pub async fn eviction_boundary_score(
        &self,
        mode: PriorityMode,
        compensation: f64,
        min_idle_secs: i64,
        min_age_secs: i64,
        offset: i64,
        use_supply: bool,
    ) -> Result<Option<f64>, DbErr> {
        let score = prioritization::score_expr(mode, compensation);
        let sql = format!(
            "SELECT ({score})::float8 AS score FROM index_patterns \
             WHERE {gate} \
             ORDER BY score ASC, pattern_id ASC \
             OFFSET $3 LIMIT 1",
            gate = eviction_gate(use_supply),
        );
        self.db
            .query_one(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                sql,
                [min_age_secs.into(), min_idle_secs.into(), offset.into()],
            ))
            .await?
            .map(|row| row.try_get::<f64>("", "score"))
            .transpose()
    }

    /// Mark a pattern as evicted after its index pair has been dropped.
    pub async fn mark_evicted(&self, pattern_id: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET status = 'evicted', evicted_at = now() \
                 WHERE pattern_id = $1",
                [pattern_id.into()],
            ))
            .await
            .map(|_| ())
    }

    // ---- latency regression guard -----------------------------------------

    /// Created patterns that are **slower with the index than without** by more
    /// than `ratio`. An index is only judged once it is older than
    /// `min_age_secs` (the same lifetime grace idle eviction uses), by which
    /// point it has had time to gather with-index latency to compare against its
    /// frozen without-index baseline. Patterns with no with-index traffic have a
    /// zero with-index average and so never trip the ratio. The without-index
    /// average is first scaled by `compensation`
    /// (`without-index-compensation-factor`), so a scan whose wall-clock is
    /// deflated by parallel workers is not mistaken for a cheaper alternative.
    /// These are the drop/warn candidates for the regression guard.
    pub async fn regression_candidates(
        &self,
        min_age_secs: i64,
        ratio: f64,
        compensation: f64,
    ) -> Result<Vec<PatternRow>, DbErr> {
        let sql = format!(
            "SELECT {PATTERN_COLUMNS} FROM index_patterns \
             WHERE status = '{created}' \
               AND cost_with_index_count > 0 \
               AND EXTRACT(EPOCH FROM (now() - COALESCE(created_at, first_seen_at))) > {min_age_secs} \
               AND (cost_with_index_us::float8 / GREATEST(cost_with_index_count, 1)) \
                 > {without} * {ratio} \
             ORDER BY pattern_id ASC",
            created = status::CREATED,
            without = prioritization::compensated_without_avg_expr(compensation),
        );
        let rows = self
            .db
            .query_all(Statement::from_string(self.db.get_database_backend(), sql))
            .await?;
        rows.iter().map(row_to_pattern).collect()
    }

    /// Mark a pattern `rejected` after its index pair was dropped for being a
    /// latency regression. Captures the with-index average that condemned it and
    /// snapshots the without-index bucket, so recovery
    /// ([`Store::promote_recovered_rejections`]) is judged only on without-index
    /// samples gathered after this point.
    pub async fn mark_rejected(&self, pattern_id: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET status = 'rejected', evicted_at = now(), \
                   rejected_with_index_avg_us = \
                     cost_with_index_us::float8 / GREATEST(cost_with_index_count, 1), \
                   without_index_at_reject_us = cost_without_index_us, \
                   without_index_at_reject_count = cost_without_index_count \
                 WHERE pattern_id = $1",
                [pattern_id.into()],
            ))
            .await
            .map(|_| ())
    }

    /// Flip `rejected` patterns back to `candidate` once fresh evidence justifies
    /// retrying the index. A pattern must have been rejected for at least
    /// `retry_delay_secs` — time spent accumulating without-index samples again —
    /// and its without-index average measured **since rejection** (scaled by
    /// `compensation`, as in detection) must exceed the with-index average
    /// recorded then (times `ratio` for hysteresis). Returns the number promoted.
    pub async fn promote_recovered_rejections(
        &self,
        retry_delay_secs: i64,
        ratio: f64,
        compensation: f64,
    ) -> Result<u64, DbErr> {
        let res = self
            .db
            .execute(Statement::from_string(
                self.db.get_database_backend(),
                format!(
                    "UPDATE index_patterns SET status = 'candidate', \
                       rejected_with_index_avg_us = NULL, \
                       without_index_at_reject_us = NULL, \
                       without_index_at_reject_count = NULL \
                     WHERE status = 'rejected' \
                       AND rejected_with_index_avg_us IS NOT NULL \
                       AND EXTRACT(EPOCH FROM (now() - evicted_at)) > {retry_delay_secs} \
                       AND (cost_without_index_count - COALESCE(without_index_at_reject_count, 0)) > 0 \
                       AND ((cost_without_index_us - COALESCE(without_index_at_reject_us, 0))::float8 \
                            / GREATEST(cost_without_index_count \
                                       - COALESCE(without_index_at_reject_count, 0), 1)) * {compensation} \
                           > rejected_with_index_avg_us * {ratio}"
                ),
            ))
            .await?;
        Ok(res.rows_affected())
    }

    /// Record (or clear) the discrepancy verdict for a pattern.
    pub async fn set_discrepancy(
        &self,
        pattern_id: &str,
        state: Option<&str>,
        ratio: Option<f64>,
    ) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET discrepancy_state = $2, discrepancy_ratio = $3, \
                   discrepancy_at = CASE WHEN $2 IS NOT NULL THEN now() ELSE discrepancy_at END \
                 WHERE pattern_id = $1",
                [
                    pattern_id.into(),
                    state.map(|s| s.to_string()).into(),
                    ratio.into(),
                ],
            ))
            .await
            .map(|_| ())
    }

    /// Record the latest `EXPLAIN` sampling verdict for a pattern (which physical
    /// tables the planner would use its index on). `state` is one of the
    /// [`patterns::explain_state`] constants. Written only by the optional
    /// explain pass, so the column stays `NULL` when that pass is disabled.
    pub async fn set_explain_state(&self, pattern_id: &str, state: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET explain_state = $2 WHERE pattern_id = $1",
                [pattern_id.into(), state.into()],
            ))
            .await
            .map(|_| ())
    }

    // ---- catalog reads ----------------------------------------------------

    /// `(index_name, summed idx_scan, summed bytes)` for every auto-index on the
    /// `accounts` / `snapshot_accounts` tables. Sums across partition leaves via
    /// `pg_partition_tree`; a plain index yields no tree rows so the LEFT JOIN +
    /// COALESCE falls back to the index's own OID.
    pub async fn read_auto_index_supply(&self) -> Result<Vec<(String, i64, i64)>, DbErr> {
        let rows = self
            .db
            .query_all(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT c.relname AS index_name, \
                    COALESCE(SUM(s.idx_scan), 0)::bigint AS idx_scan, \
                    COALESCE(SUM(pg_relation_size(COALESCE(t.relid, c.oid))), 0)::bigint AS bytes \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public' \
                 LEFT JOIN LATERAL pg_partition_tree(c.oid) t ON true \
                 LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = COALESCE(t.relid, c.oid) \
                 WHERE c.relkind IN ('i', 'I') \
                   AND (c.relname LIKE 'idx_accounts\\_%' OR c.relname LIKE 'idx_snapshot_accounts\\_%') \
                 GROUP BY c.relname"
                    .to_string(),
            ))
            .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get::<String>("", "index_name")?,
                    row.try_get::<i64>("", "idx_scan")?,
                    row.try_get::<i64>("", "bytes")?,
                ))
            })
            .collect()
    }

    /// Physical index names that a query plan may actually reference for the
    /// partitioned index `parent`: every leaf of its partition tree plus the
    /// parent itself. On a partitioned table `CREATE INDEX <parent> ON accounts`
    /// builds a partitioned parent index (never scanned directly, so it never
    /// appears in a plan) and an auto-named child index on each partition — and
    /// those child names are what `EXPLAIN` prints. So a probe that wants to know
    /// "does the plan use this index" must match against these members, not the
    /// parent name alone. Mirrors the partition-tree walk in
    /// [`Store::read_auto_index_supply`]; a plain (non-partitioned) index yields
    /// no tree rows and falls back to its own name via `COALESCE`.
    pub async fn index_member_names(&self, parent: &str) -> Result<Vec<String>, DbErr> {
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "SELECT m.relname AS name \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public' \
                 LEFT JOIN LATERAL pg_partition_tree(c.oid) t ON true \
                 JOIN pg_class m ON m.oid = COALESCE(t.relid, c.oid) \
                 WHERE c.relkind IN ('i', 'I') AND c.relname = $1",
                [parent.into()],
            ))
            .await?;
        let mut names: Vec<String> = rows
            .iter()
            .filter_map(|r| r.try_get::<String>("", "name").ok())
            .collect();
        if !names.iter().any(|n| n == parent) {
            names.push(parent.to_string());
        }
        Ok(names)
    }

    /// Number of indexes on `table` (used for the cap / fill ratio).
    pub async fn count_table_indexes(&self, table: &str) -> Result<i64, DbErr> {
        self.db
            .query_one(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "SELECT COUNT(*)::bigint AS n FROM pg_indexes \
                 WHERE schemaname = 'public' AND tablename = $1",
                [table.into()],
            ))
            .await?
            .map(|row| row.try_get::<i64>("", "n"))
            .transpose()
            .map(|n| n.unwrap_or(0))
    }

    /// Whether Postgres is currently accumulating statistics. When off,
    /// `idx_scan` is frozen and the eviction pass must be skipped.
    pub async fn track_counts_enabled(&self) -> Result<bool, DbErr> {
        Ok(self
            .db
            .query_one(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT current_setting('track_counts') = 'on' AS enabled".to_string(),
            ))
            .await?
            .map(|row| row.try_get::<bool>("", "enabled"))
            .transpose()?
            .unwrap_or(false))
    }

    /// Aggregate counts for metrics.
    pub async fn counts(&self) -> Result<StoreCounts, DbErr> {
        let row = self
            .db
            .query_one(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT COUNT(*) FILTER (WHERE status = 'created')::bigint AS created, \
                   COUNT(*) FILTER (WHERE status = 'candidate')::bigint AS candidate, \
                   COUNT(*) FILTER (WHERE status = 'evicted')::bigint AS evicted, \
                   COUNT(*) FILTER (WHERE status = 'rejected')::bigint AS rejected, \
                   COUNT(*) FILTER (WHERE status = 'created' AND discrepancy_state IS NOT NULL)::bigint AS discrepant \
                 FROM index_patterns"
                    .to_string(),
            ))
            .await?;
        match row {
            Some(row) => Ok(StoreCounts {
                created: row.try_get("", "created")?,
                candidate: row.try_get("", "candidate")?,
                evicted: row.try_get("", "evicted")?,
                rejected: row.try_get("", "rejected")?,
                discrepant: row.try_get("", "discrepant")?,
            }),
            None => Ok(StoreCounts::default()),
        }
    }
}

/// Decode one query row into a [`PatternRow`]. The single point that must match
/// [`PATTERN_COLUMNS`].
fn row_to_pattern(row: &QueryResult) -> Result<PatternRow, DbErr> {
    Ok(PatternRow {
        pattern_id: row.try_get("", "pattern_id")?,
        program: row.try_get("", "program")?,
        human_name: row.try_get("", "human_name")?,
        offsets_lengths: offsets_from_json(
            &row.try_get::<serde_json::Value>("", "offsets_lengths")?,
        ),
        datasize: row.try_get("", "datasize")?,
        example_request: row.try_get::<Option<serde_json::Value>>("", "example_request")?,
        demand_count: row.try_get("", "demand_count")?,
        demand_at_create: row.try_get("", "demand_at_create")?,
        total_cost_us: row.try_get("", "total_cost_us")?,
        failed_count: row.try_get("", "failed_count")?,
        cost_with_index_us: row.try_get("", "cost_with_index_us")?,
        cost_with_index_count: row.try_get("", "cost_with_index_count")?,
        cost_without_index_us: row.try_get("", "cost_without_index_us")?,
        cost_without_index_count: row.try_get("", "cost_without_index_count")?,
        variety_estimate: row.try_get("", "variety_estimate")?,
        status: row.try_get("", "status")?,
        last_idx_scan: row.try_get("", "last_idx_scan")?,
        index_bytes: row.try_get("", "index_bytes")?,
        discrepancy_state: row.try_get("", "discrepancy_state")?,
        discrepancy_ratio: row.try_get("", "discrepancy_ratio")?,
        explain_state: row.try_get("", "explain_state")?,
        created_at_epoch: row.try_get("", "created_at_epoch")?,
    })
}
