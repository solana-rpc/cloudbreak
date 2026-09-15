// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `processed-suite`: correctness and speed checks for processed commitment on one API instance.
//!
//! A Yellowstone watcher keeps a block tree of recent processed blocks. The fronts run
//! concurrently for `--duration` and share one key pool, one HTTP client, and per source one
//! semaphore (`--concurrency`) and one token bucket (`--rps`). Everything is read-only, and
//! Postgres is read only by primary key or the newest row per pubkey. Ctrl-C stops the fronts
//! early, draining for at most 90 s. The run ends with one report of correctness, speed and
//! coverage tables, and the exit code is non-zero when a correctness rule fails.
//!
//! Example against a dev2-style deployment, with the x-token in `PROCESSED_SUITE_X_TOKEN`:
//!
//! ```text
//! integration_tests processed-suite \
//!   --cloudbreak http://cloudbreak-api-0.dev2.example:26722 \
//!   --cloudbreak-metrics http://cloudbreak-api-0.dev2.example:9090/metrics \
//!   --reference agave-a=http://agave-rpc-a.dev2.example:8899 \
//!   --reference agave-b=http://agave-rpc-b.dev2.example:8899 \
//!   --grpc-endpoint https://yellowstone.dev2.example:443 \
//!   --db-url postgres://reader:PASSWORD@cloudbreak-db.dev2.example:5432/cloudbreak \
//!   --duration 10m --output processed-suite.json
//! ```

mod args;
mod bucket;
mod convergence;
mod cross_source;
mod ctx;
mod fork_check;
mod forks;
mod keys;
mod latency;
mod metrics;
mod read_after_write;
mod render;
mod report;
mod same_node;
mod sources;
mod tree;
mod watcher;

pub use args::{Args, Front, LONG_ABOUT};
pub use ctx::Ctx;
pub use report::Tally;

use anyhow::{Context, Result, anyhow};
use ctx::{Control, DRAIN};
use report::{FrontRow, RateRow, Report, Thresholds};
use sea_orm::Database;
use sources::Source;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use watcher::{Watcher, WatcherConfig};

pub const CLOUDBREAK: &str = "cloudbreak";
pub const MAX_EXAMPLES: usize = 20;
/// Wait for a sample's slot to resolve canonical or abandoned.
pub const RESOLVE_WAIT: Duration = Duration::from_secs(60);
const WARM_KEYS: usize = 200;
const WARM_WAIT: Duration = Duration::from_secs(30);
const RSS_EVERY: Duration = Duration::from_secs(30);

fn skip_reason(ctx: &Ctx, front: Front) -> Option<String> {
    let a = &ctx.args;
    let reason = match front {
        _ if !a.fronts.0.contains(&front) => "not selected",
        Front::SameNode if a.db_url.is_none() => "needs --db-url",
        Front::CrossSource if a.references.is_empty() => "needs --reference",
        Front::Forks | Front::ReadAfterWrite if a.grpc_endpoint.is_none() => {
            "needs --grpc-endpoint"
        }
        Front::Metrics if a.cloudbreak_metrics.is_none() => "needs --cloudbreak-metrics",
        Front::Metrics => return None,
        _ if ctx.pool.len() == 0 => "empty key pool, set --grpc-endpoint or --pubkeys-file",
        _ => return None,
    };
    Some(reason.to_string())
}

async fn gated<T>(skip: Option<String>, front: impl Future<Output = T>) -> Result<T, String> {
    match skip {
        Some(reason) => Err(reason),
        None => Ok(front.await),
    }
}

fn tally_of<T>(outcome: &Result<T, String>, tally: impl Fn(&T) -> Tally) -> Result<Tally, String> {
    outcome.as_ref().map(tally).map_err(Clone::clone)
}

fn push_row(r: &mut Report, front: &str, gating: bool, tally: Result<Tally, String>) {
    let (skipped, tally) = match tally {
        Ok(tally) => (None, tally),
        Err(reason) => {
            r.coverage.skipped.push((front.to_string(), reason.clone()));
            (Some(reason), Tally::default())
        }
    };
    r.correctness
        .push(FrontRow::new(front, gating, skipped, tally));
}

/// Resident set size in MB: resident pages from /proc/self/statm times 4 KiB.
fn rss_mb() -> f64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages = statm
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse::<f64>().ok());
    pages.unwrap_or_default() * 4096.0 / (1024.0 * 1024.0)
}

fn keep_max_rss(max_kb: &AtomicU64) {
    max_kb.fetch_max((rss_mb() * 1024.0) as u64, Ordering::Relaxed);
}

/// The first Ctrl-C stops the fronts, a second one exits at once.
async fn interrupt(ctx: Arc<Ctx>) {
    if tokio::signal::ctrl_c().await.is_err() {
        return;
    }
    let drain = DRAIN.as_secs();
    eprintln!("processed-suite: stopping, draining for at most {drain} s, Ctrl-C again to abort");
    ctx.stop_now();
    if tokio::signal::ctrl_c().await.is_ok() {
        std::process::exit(130);
    }
}

struct Outcomes {
    same: Result<Tally, String>,
    cross: Result<cross_source::CrossReport, String>,
    conv: Result<Tally, String>,
    forks: Result<forks::ForkReport, String>,
    raw: Result<read_after_write::RawReport, String>,
    load: Result<latency::LoadReport, String>,
    metrics: Result<metrics::MetricsReport, String>,
}

pub async fn run(args: Args) -> Result<()> {
    let started = Instant::now();
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let timeout = Duration::from_secs(args.timeout);
    let client = reqwest::Client::builder().timeout(timeout).build()?;
    let source = |name: &str, url: &str| Source::new(name, url, args.concurrency, args.rps);
    let cloudbreak = source(CLOUDBREAK, &args.cloudbreak);
    let references: Vec<Source> = (args.references.iter())
        .map(|(name, url)| source(name.as_str(), url.as_str()))
        .collect();
    let db = match &args.db_url {
        Some(url) => Some(
            Database::connect(url)
                .await
                .context("Failed to connect to Postgres")?,
        ),
        None => None,
    };
    let filter = match &db {
        Some(db) => same_node::load_owner_filter(db).await,
        None => None,
    };
    if db.is_some() && filter.is_none() {
        println!("warning: no program filter in environment_info, exclusion is probed per key");
    }
    let fixed = args.pubkeys_file.as_deref().map(keys::read_pubkeys_file);
    let pool = keys::KeyPool::new(fixed.transpose()?.unwrap_or_default());
    let mut tasks = Vec::new();
    let watcher = args.grpc_endpoint.clone().map(|endpoint| {
        let config = WatcherConfig {
            endpoint,
            x_token: args.grpc_x_token.clone(),
            window: args.watcher_window,
            max_slots: args.watcher_max_slots,
        };
        let oracle = references.first().map(|r| (r.clone(), client.clone()));
        let (watcher, handles) = Watcher::spawn(config, oracle);
        tasks.extend(handles);
        tasks.push(pool.follow(&watcher));
        watcher
    });
    let warm_until = Instant::now() + WARM_WAIT;
    while watcher.is_some() && pool.len() < WARM_KEYS && Instant::now() < warm_until {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (keys, duration) = (pool.len(), args.duration);
    println!("processed-suite: {keys} keys in pool, running for {duration:?}");

    let deadline = Instant::now() + args.duration;
    let control = Control::new(deadline);
    let ctx = Arc::new(Ctx {
        args,
        client,
        cloudbreak,
        references,
        watcher,
        pool,
        filter,
        db,
        deadline,
        control,
    });
    tasks.push(tokio::spawn(interrupt(ctx.clone())));
    let max_rss_kb = Arc::new(AtomicU64::new(0));
    let rss = max_rss_kb.clone();
    tasks.push(tokio::spawn(async move {
        loop {
            keep_max_rss(&rss);
            tokio::time::sleep(RSS_EVERY).await;
        }
    }));
    let skip = |front| skip_reason(&ctx, front);
    let (same, cross, conv, forks, raw, load, metrics) = tokio::join!(
        gated(skip(Front::SameNode), same_node::run(ctx.clone())),
        gated(skip(Front::CrossSource), cross_source::run(ctx.clone())),
        gated(skip(Front::Convergence), convergence::run(ctx.clone())),
        gated(skip(Front::Forks), forks::run(ctx.clone())),
        gated(
            skip(Front::ReadAfterWrite),
            read_after_write::run(ctx.clone())
        ),
        gated(skip(Front::Latency), latency::run_load(ctx.clone())),
        gated(skip(Front::Metrics), metrics::run(ctx.clone())),
    );
    tasks.iter().for_each(tokio::task::JoinHandle::abort);
    keep_max_rss(&max_rss_kb);

    let outcomes = Outcomes {
        same,
        cross,
        conv,
        forks,
        raw,
        load,
        metrics,
    };
    let mut r = assemble(&ctx, outcomes);
    (r.started_at, r.duration_secs) = (started_at, started.elapsed().as_secs_f64());
    let c = &mut r.coverage;
    (c.run_secs, c.interrupted) = (started.elapsed().as_secs(), ctx.interrupted());
    c.max_rss_mb = max_rss_kb.load(Ordering::Relaxed) as f64 / 1024.0;
    for s in ctx.sources() {
        let requests = s.calls.load(Ordering::Relaxed);
        let rps = requests as f64 / r.duration_secs.max(1.0);
        let (source, cap_rps) = (s.name, ctx.args.rps);
        r.coverage.source_rates.push(RateRow {
            source,
            requests,
            rps,
            cap_rps,
        });
    }

    let a = &ctx.args;
    r.inputs = a.inputs();
    r.thresholds = Thresholds {
        max_cross_mismatch: a.max_cross_mismatch,
        max_abandoned_pct: a.max_abandoned_served_pct,
        min_fresh_pct: a.min_fresh_pct,
        max_p99_ms: a.max_p99_ms,
    };
    r.failures = report::decide(&r.correctness, &r.latency, &r.thresholds);
    for row in &mut r.correctness {
        row.verdict = report::verdict(row, &r.failures).to_string();
    }
    print!("{}", render::render(&r));
    if let Some(path) = &a.output {
        let json = serde_json::to_string_pretty(&r)?;
        std::fs::write(path, json).with_context(|| format!("write {}", path.display()))?;
    }
    match r.failures.len() {
        0 => Ok(()),
        n => Err(anyhow!("{n} correctness rule(s) failed")),
    }
}

/// Correctness rows, speed rows and the watcher part of coverage.
fn assemble(ctx: &Ctx, o: Outcomes) -> Report {
    let mut r = Report::default();
    push_row(&mut r, Front::SameNode.name(), true, o.same);
    let cross = tally_of(&o.cross, |c| c.processed.clone());
    push_row(&mut r, Front::CrossSource.name(), true, cross);
    if let Ok(c) = &o.cross {
        let control = Ok(c.control.clone());
        push_row(&mut r, "cross-source confirmed control", false, control);
    }
    push_row(&mut r, Front::Convergence.name(), true, o.conv);
    let forks = tally_of(&o.forks, |f| f.tally.clone());
    push_row(&mut r, Front::Forks.name(), true, forks);
    let raw = tally_of(&o.raw, |w| w.tally.clone());
    push_row(&mut r, Front::ReadAfterWrite.name(), false, raw);
    let load = tally_of(&o.load, |l| l.tally.clone());
    push_row(&mut r, Front::Latency.name(), true, load);
    let metrics = tally_of(&o.metrics, |_| Tally::default());
    push_row(&mut r, Front::Metrics.name(), false, metrics);
    r.slot_delta = o.cross.map(|c| c.deltas).unwrap_or_default();
    r.read_after_write = o.raw.map(|w| w.rows).unwrap_or_default();
    r.metrics = o.metrics.map(|m| m.rows).unwrap_or_default();
    let c = &mut r.coverage;
    if let Ok(l) = o.load {
        (r.latency, r.freshness, c.load_skipped_ticks) = (l.rows, l.freshness, l.skipped_ticks);
    }
    if let Ok(f) = o.forks {
        (c.bursts, c.burst_responses, c.burst_regressions) = (f.bursts, f.responses, f.regressions);
        r.fork_metric_deltas = f.metric_deltas;
    }
    (c.pool_keys, c.keys_sampled) = (ctx.pool.len(), ctx.pool.sampled());
    if let Some(w) = &ctx.watcher {
        let (s, load) = (&w.stats, |a: &AtomicU64| a.load(Ordering::Relaxed));
        (c.blocks, c.forks, c.dead_slots) = (load(&s.blocks), load(&s.forks), load(&s.dead));
        (c.restarted_slots, c.watcher_reconnects) = (load(&s.restarted), load(&s.reconnects));
        (c.getblocks_checked, c.getblocks_disagreements) =
            (load(&s.checked_slots), load(&s.disagreements));
        c.disagreement_examples = s.examples.lock().expect("examples lock").clone();
    }
    r
}
