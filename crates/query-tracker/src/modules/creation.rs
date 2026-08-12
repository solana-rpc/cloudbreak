// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Creation — turning demand into indexes.
//!
//! A single polling loop that, each tick: checks indexer backpressure, checks
//! the index cap, asks the [`Store`] for the highest-priority candidate the
//! configured [`PriorityMode`](cloudbreak_core::PriorityMode) allows, and — if it passes the program
//! allow/deny list — builds the index **pair** (`accounts` +
//! `snapshot_accounts`).
//!
//! There is no in-memory queue: candidates live in the DB and are ranked on
//! read, so nothing is lost across restarts. Physical `CREATE INDEX IF NOT
//! EXISTS` plus a persisted `created` status means we never re-issue an index we
//! already have.
//!
//! ## Two thresholds: fill target and hard cap
//!
//! Creation is bounded by two sizes derived from config:
//! - **fill target** = `floor(eviction-fill-threshold × max-auto-indexes)` — the
//!   operating size the eviction pass trims back to.
//! - **hard cap** = `max-auto-indexes` — never exceeded.
//!
//! Below the target the top candidate is built freely. In the *buffer band*
//! `(target, max]` the **value guard** ([`value_guard_allows`]) only builds an
//! index that out-scores the index it would displace, so the table climbs toward
//! the cap only for genuinely valuable indexes; the eviction pass later reclaims
//! the buffer back to the target. The one exception: if nothing is currently
//! reclaimable to displace (no eligible eviction candidate at that position), the
//! candidate is built anyway up to the cap — the hard cap is the real ceiling and
//! eviction reclaims once indexes go idle. At the cap, creation is *paused* —
//! candidates stay queued in `index_patterns`, never dropped.
//!
//! ### Stickiness
//!
//! Both sides of the guard use the same `priority-mode` score, but a `created`
//! index carries realized signal a fresh candidate cannot: its with-index
//! latency `gain` and its `idx_scan` supply. That structurally favors incumbents
//! ("stickiness") — usually desirable, since proven indexes should not yield to
//! unproven ones — but tunable via `value-guard-creation-bias`, which scales the
//! candidate's score in the comparison (`> 1` less sticky, `< 1` stickier).

use crate::modules::indexer_backpressure;
use crate::modules::store::{Store, patterns::PatternRow};
use crate::modules::{CAP_TABLE, INDEX_TABLES};
use crate::stats::metrics;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use cloudbreak_core::{IndexRegressionGuard, QueryTrackerConfig};
use sea_orm::{ConnectionTrait, Statement};
use solana_pubkey::Pubkey;
use tracing::{error, info, warn};

/// How many top candidates to fetch per tick before applying the program
/// allow/deny filter in Rust (keeps program scoping out of SQL).
const CANDIDATE_FETCH_LIMIT: u64 = 16;

/// Format a raw priority score in thousands with one decimal (e.g. `3194.9K`),
/// matching the debug endpoints. The raw score is derived from per-request
/// microseconds, so ÷1000 makes it proportional to the millisecond figures
/// shown elsewhere.
fn score_k(s: f64) -> String {
    format!("{:.1}K", s / 1000.0)
}

/// Edge-trigger state for value-guard deferral logging: which candidate is
/// currently blocked. Lets the loop log a deferral only when the blocked
/// candidate changes, instead of every `index-creation-delay` tick — the top
/// candidate is usually stable, so an unchanged block stays silent.
#[derive(Default)]
struct DeferLog {
    last: Option<String>,
}

impl DeferLog {
    /// Log the deferral of `name` iff it differs from the last blocked
    /// candidate. Returns whether a line should be emitted, updating the state.
    fn should_log(&mut self, name: &str) -> bool {
        let changed = self.last.as_deref() != Some(name);
        self.last = Some(name.to_string());
        changed
    }

    /// Clear the blocked candidate after a successful build so the next deferral
    /// (even of the same identity) logs immediately.
    fn reset(&mut self) {
        self.last = None;
    }
}

/// Whether `program` is allowed by the include/exclude lists.
pub fn program_allowed(program: Pubkey, config: &QueryTrackerConfig) -> bool {
    if config.included_programs.is_empty() {
        !config.excluded_programs.iter().any(|p| p.0 == program)
    } else {
        config.included_programs.iter().any(|p| p.0 == program)
    }
}

#[tracing::instrument(name = "query_tracker_creation", skip_all)]
pub async fn run(store: Store, config: QueryTrackerConfig) {
    info!(
        target: "query_tracker_creation",
        "creation loop started (delay: {:?}, priority: {:?}, threshold: {}, max-auto-indexes: {:?})",
        config.index_creation_delay, config.priority_mode, config.index_generation_threshold,
        config.max_auto_indexes
    );

    // Edge-triggered so the cap-reached warning logs once per "reach", not every
    // tick while pinned at the cap. Reset whenever the count drops back below it
    // (eviction made room), so the next reach logs once more.
    let mut cap_reached_logged = false;
    // Edge-triggered value-guard deferral logging (see `DeferLog`).
    let mut defer_log = DeferLog::default();

    loop {
        tokio::time::sleep(config.index_creation_delay).await;

        // Recover `rejected` patterns (dropped by the latency guard) that have
        // since proven slower without the index — they become candidates again.
        if config.index_regression_guard != IndexRegressionGuard::Off {
            match store
                .promote_recovered_rejections(
                    config.index_regression_retry_delay.as_secs() as i64,
                    config.index_regression_ratio,
                    config.without_index_compensation_factor,
                )
                .await
            {
                Ok(n) if n > 0 => info!(
                    target: "query_tracker_creation",
                    "recovered {n} rejected pattern(s) to candidate (now slower without the index)"
                ),
                Ok(_) => {}
                Err(e) => error!(
                    target: "query_tracker_creation",
                    "failed to recover rejected patterns: {e:?}"
                ),
            }
        }

        if indexer_backpressure::is_under_pressure(
            &config.indexer_metrics,
            config.indexer_metrics_threshold,
        )
        .await
        {
            continue;
        }

        // Snapshot the physical index count once — drives both the hard cap and
        // the creation-time value guard below.
        let current = match config.max_auto_indexes {
            Some(_) => match store.count_table_indexes(CAP_TABLE).await {
                Ok(c) => {
                    metrics::SNAPSHOT_ACCOUNTS_INDEXES.set(c);
                    c
                }
                Err(e) => {
                    error!(target: "query_tracker_creation", "failed to count indexes: {e:?}");
                    continue;
                }
            },
            None => 0,
        };

        // Hard ceiling: never exceed the cap, whatever the value guard decides.
        if let Some(max) = config.max_auto_indexes
            && current as usize >= max
        {
            // Only warn on the transition into the at-cap state, not every tick.
            if !cap_reached_logged {
                warn!(
                    target: "query_tracker_creation",
                    "auto-index cap reached ({current}/{max}); deferring creation until eviction makes room"
                );
                cap_reached_logged = true;
            }
            continue;
        }
        // Below the cap again — re-arm the warning for the next time it fills up.
        cap_reached_logged = false;

        let candidates = match store
            .top_candidates_scored(
                config.priority_mode,
                config.without_index_compensation_factor,
                config.index_generation_threshold,
                config.cost_eligibility_threshold_us,
                CANDIDATE_FETCH_LIMIT,
            )
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!(target: "query_tracker_creation", "failed to fetch candidates: {e:?}");
                continue;
            }
        };

        // Highest-priority candidate that passes program scoping and parses back
        // to a valid identity, carrying its score for the value guard.
        let chosen = candidates.into_iter().find_map(|scored| {
            let identity = match scored.row.identity() {
                Ok(i) => i,
                Err(e) => {
                    error!(target: "query_tracker_creation", "skipping candidate: {e}");
                    return None;
                }
            };
            program_allowed(identity.program, &config).then_some((
                scored.row,
                identity,
                scored.score,
            ))
        });

        let Some((row, identity, score)) = chosen else {
            continue;
        };

        // Creation-time value guard (see module docs): in the buffer band, only
        // build an index that out-values the one it would displace.
        if !value_guard_allows(&store, &config, current, &identity, score, &mut defer_log).await {
            continue;
        }

        defer_log.reset();
        create_pair(&store, &row, &identity).await;
    }
}

/// Creation-time value guard. While the capped table sits in the *buffer band*
/// — at or above the fill target `floor(eviction-fill-threshold × max-auto-indexes)`
/// — a new index is only built when it out-scores the index it would displace.
///
/// ## The `over` position — why it is not simply "beat the worst index"
///
/// `over = current − target` is how many indexes the table already holds beyond
/// the operating size. The eviction pass will reclaim those `over` least-valuable
/// eligible indexes back down to `target`, so they are already "spoken for" and
/// leaving regardless. A *new* index therefore does not compete with the very
/// worst index — it competes with the first index that would still **survive**
/// that trim: the one at position `over` in eviction (ascending-score) order,
/// `E[over]`. Building the candidate pushes the overflow to `over + 1`; the
/// subsequent trim drops `E[0..=over]`, so the candidate keeps a lasting slot iff
/// it out-scores `E[over]`.
///
/// Worked example (`max = 200`, `fill = 0.7` ⇒ `target = 140`):
/// - `current = 140` (`over = 0`) → compare against `E[0]`, the single worst
///   eligible index: a straight swap-the-worst.
/// - `current = 176` (`over = 36`) → the 36 worst eligible indexes are headed for
///   the next trim anyway, so the candidate must beat `E[36]` (the 37th-worst) to
///   earn a lasting slot; if it does, the trim removes `E[0..=36]` and it stays.
///
/// `value-guard-creation-bias` scales the candidate's score before the
/// comparison to tune stickiness (see its config docs). Returns `true` when the
/// index may be built: below the target, when the guard does not apply (eviction
/// disabled, or no `max-auto-indexes`), when the candidate out-scores its
/// boundary, or when there is **no reclaimable index to displace** at `over`
/// (`over` beyond the eligible set — fewer eligible indexes than the overflow).
/// That last case grows toward the hard cap and lets the eviction pass reclaim
/// later, rather than stall creation behind indexes that are not yet droppable —
/// the cap is the real ceiling. Only a DB error, or losing the comparison
/// against a real boundary index, defers creation.
async fn value_guard_allows(
    store: &Store,
    config: &QueryTrackerConfig,
    current: i64,
    identity: &IndexIdentity,
    score: f64,
    defer_log: &mut DeferLog,
) -> bool {
    // The guard only applies when eviction can reclaim the buffer and a cap
    // defines the target; otherwise fall back to cap-only creation.
    let Some(max) = config.max_auto_indexes else {
        return true;
    };
    if !config.index_eviction_enabled {
        return true;
    }
    let target = (config.eviction_fill_threshold * max as f64).floor() as i64;
    let over = current - target;
    if over < 0 {
        return true; // below the operating size — build freely
    }

    let boundary = match store
        .eviction_boundary_score(
            config.priority_mode,
            config.without_index_compensation_factor,
            config.index_min_idle.as_secs() as i64,
            config.index_min_age_grace.as_secs() as i64,
            over,
            config.use_supply_for_eviction,
        )
        .await
    {
        Ok(b) => b,
        Err(e) => {
            error!(
                target: "query_tracker_creation",
                "value guard: boundary lookup failed: {e:?}; deferring creation"
            );
            return false;
        }
    };

    let bias = config.value_guard_creation_bias;
    let biased = score * bias;
    match boundary {
        Some(b) if biased > b => {
            info!(
                target: "query_tracker_creation",
                "value guard: building '{}' (score {}×{bias:.2}={}) beats displaced \
                 index (score {}) at over={over}",
                identity.human_name(), score_k(score), score_k(biased), score_k(b)
            );
            true
        }
        Some(b) => {
            // Edge-triggered: the top candidate is usually stable, so log the
            // deferral only when the blocked candidate changes — otherwise this
            // fires every creation tick.
            let name = identity.human_name();
            if defer_log.should_log(&name) {
                info!(
                    target: "query_tracker_creation",
                    "value guard: deferring '{name}' (score {}×{bias:.2}={}) — does not beat \
                     displaced index (score {}) at over={over}; logged once — silent until the \
                     blocked candidate changes or it gets built",
                    score_k(score), score_k(biased), score_k(b)
                );
            }
            false
        }
        None => {
            // Above the target but nothing reclaimable to compare against
            // (fewer eligible indexes than the overflow). Build anyway — the
            // hard cap still bounds growth, and the eviction pass reclaims
            // whatever becomes eligible later. Deferring here would stall
            // creation behind indexes that are not (yet) droppable.
            tracing::debug!(
                target: "query_tracker_creation",
                "value guard: building '{}' at over={over} — no reclaimable index to displace; \
                 growing toward the cap, eviction reclaims once indexes go idle",
                identity.human_name()
            );
            true
        }
    }
}

/// Build the index pair for one identity, marking the pattern created only if
/// both sides succeed.
async fn create_pair(store: &Store, row: &PatternRow, identity: &IndexIdentity) {
    for table in INDEX_TABLES {
        if let Err(e) = create_one(store, identity, table).await {
            error!(
                target: "query_tracker_creation",
                "failed to create index '{}' on {table}: {e:?}",
                identity.pg_index_name(table)
            );
            metrics::INDEX_CREATE_FAILURES_TOTAL.inc();
            if let Err(e) = store.mark_create_failed(&row.pattern_id, &e).await {
                error!(target: "query_tracker_creation", "failed to record create failure: {e:?}");
            }
            return;
        }
    }

    if let Err(e) = store.mark_created(&row.pattern_id).await {
        error!(target: "query_tracker_creation", "failed to mark pattern created: {e:?}");
        return;
    }

    metrics::INDEX_CREATED_TOTAL.inc();
    info!(
        target: "query_tracker_creation",
        "created index pair for '{}' (demand: {}, variety~{})",
        identity.human_name(), row.demand_count, row.variety_estimate
    );
}

/// Execute a single `CREATE INDEX IF NOT EXISTS`. Returns the error string on
/// failure so it can be persisted.
async fn create_one(store: &Store, identity: &IndexIdentity, table: &str) -> Result<(), String> {
    let sql = identity.create_index_sql(table);
    let db = store.db();
    let stmt = Statement::from_string(db.get_database_backend(), sql);
    db.execute(stmt)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}
