// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! latency front: one open-loop ticker per source that rotates through getAccountInfo base64 and
//! jsonParsed and getMultipleAccounts base64, at processed and confirmed.
//!
//! The ticker runs at half of `--rps`, and the source's token bucket caps every front's requests
//! together. A tick is skipped when the runtime falls behind or when the source already has four
//! times `--concurrency` load requests waiting or in flight. Skipped ticks are reported per source.
//!
//! Latency percentiles cover successful replies only and start at the send, after the permit and
//! the token. Timeouts, HTTP errors and JSON-RPC errors are counted apart. A processed reply is
//! fresh when its slot is above the highest confirmed slot the source answered with before the
//! send. The watcher tip for `behind tip` is also read at the send.

use super::report::{FreshRow, LatencyRow, pct, summarize};
use super::sources::Source;
use super::watcher::Watcher;
use super::{CLOUDBREAK, Ctx, Tally};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

const MULTI_KEYS: usize = 20;
const COMMITMENTS: [&str; 2] = ["processed", "confirmed"];
/// Share of `--rps` the load ticker uses, the rest is left to the other fronts.
const LOAD_SHARE: f64 = 0.5;
const BACKLOG_PER_PERMIT: usize = 4;

#[derive(Clone, Copy)]
enum Method {
    AccountBase64,
    AccountParsed,
    Multiple,
}

const METHODS: [Method; 3] = [
    Method::AccountBase64,
    Method::AccountParsed,
    Method::Multiple,
];

impl Method {
    fn label(self) -> &'static str {
        match self {
            Method::AccountBase64 => "getAccountInfo base64",
            Method::AccountParsed => "getAccountInfo jsonParsed",
            Method::Multiple => "getMultipleAccounts base64 x20",
        }
    }

    /// RPC method and params, or None when the pool has no key of the right kind.
    fn request(self, ctx: &Ctx, commitment: &str) -> Option<(&'static str, JsonValue)> {
        let config = |encoding: &str| json!({"commitment": commitment, "encoding": encoding});
        let one = || ctx.pool.sample(1).pop();
        Some(match self {
            Method::AccountBase64 => ("getAccountInfo", json!([one()?, config("base64")])),
            Method::AccountParsed => (
                "getAccountInfo",
                json!([ctx.pool.token_key()?, config("jsonParsed")]),
            ),
            Method::Multiple => (
                "getMultipleAccounts",
                json!([ctx.pool.sample(MULTI_KEYS), config("base64")]),
            ),
        })
    }
}

#[derive(Default)]
struct Cell {
    ms: Vec<f64>,
    requests: u64,
    timeouts: u64,
    http_errors: u64,
    rpc_errors: BTreeMap<i64, u64>,
    bytes: u64,
}

#[derive(Default)]
struct Fresh {
    fresh: u64,
    total: u64,
    behind_tip: Vec<f64>,
}

#[derive(Default)]
struct LoadState {
    cells: BTreeMap<(String, &'static str, &'static str), Cell>,
    fresh: BTreeMap<String, Fresh>,
}

pub struct LoadReport {
    pub tally: Tally,
    pub rows: Vec<LatencyRow>,
    pub freshness: Vec<FreshRow>,
    pub skipped_ticks: BTreeMap<String, u64>,
}

pub async fn run_load(ctx: Arc<Ctx>) -> LoadReport {
    let state = Arc::new(Mutex::new(LoadState::default()));
    let drives = (ctx.sources().into_iter()).map(|s| drive(ctx.clone(), s, state.clone()));
    let skipped_ticks = futures::future::join_all(drives)
        .await
        .into_iter()
        .collect();
    let state = std::mem::take(&mut *state.lock().expect("load lock"));
    let rows = state
        .cells
        .into_iter()
        .map(|((source, method, commitment), c)| LatencyRow {
            source,
            method: method.to_string(),
            commitment: commitment.to_string(),
            latency_ms: summarize(c.ms),
            avg_bytes: c.bytes / c.requests.max(1),
            requests: c.requests,
            timeouts: c.timeouts,
            http_errors: c.http_errors,
            rpc_errors: c.rpc_errors,
        });
    let mut tally = Tally::default();
    if let Some(f) = state.fresh.get(CLOUDBREAK) {
        (tally.samples, tally.passed, tally.fresh, tally.fresh_of) =
            (f.total, f.fresh, f.fresh, f.total);
    }
    let freshness = state.fresh.into_iter().map(|(source, f)| FreshRow {
        source,
        fresh_pct: pct(f.fresh, f.total),
        samples: f.total,
        behind_tip: summarize(f.behind_tip),
    });
    LoadReport {
        tally,
        rows: rows.collect(),
        freshness: freshness.collect(),
        skipped_ticks,
    }
}

/// Runs one source's ticker until the stop. Returns the source name and its skipped ticks.
async fn drive(ctx: Arc<Ctx>, source: Source, state: Arc<Mutex<LoadState>>) -> (String, u64) {
    let confirmed_seen = Arc::new(AtomicU64::new(0));
    let period = Duration::from_secs_f64(1.0 / (ctx.args.rps * LOAD_SHARE).max(0.1));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let backlog = ctx.args.concurrency.max(1) * BACKLOG_PER_PERMIT;
    let (mut tasks, mut turn, mut skipped) = (JoinSet::new(), 0usize, 0u64);
    let mut last: Option<tokio::time::Instant> = None;
    while ctx.running() {
        let at = tokio::select! {
            _ = ctx.stopped() => break,
            at = tick.tick() => at,
        };
        let gap = last.map_or(1.0, |prev| (at - prev).as_secs_f64() / period.as_secs_f64());
        skipped += (gap.round() as u64).saturating_sub(1);
        last = Some(at);
        while tasks.try_join_next().is_some() {}
        if tasks.len() >= backlog {
            skipped += 1;
            continue;
        }
        let (method, commitment) = (METHODS[turn / 2 % METHODS.len()], COMMITMENTS[turn % 2]);
        turn += 1;
        let Some((rpc_method, params)) = method.request(&ctx, commitment) else {
            continue;
        };
        let (ctx, source, state, seen) = (
            ctx.clone(),
            source.clone(),
            state.clone(),
            confirmed_seen.clone(),
        );
        tasks.spawn(async move {
            let at_send = || {
                let tip = ctx.watcher.as_ref().map(Watcher::tip).filter(|t| *t > 0);
                (tip, seen.load(Ordering::Relaxed))
            };
            let call = source.call_hook(&ctx.client, rpc_method, params, at_send);
            let (reply, (tip, confirmed)) = call.await;
            let slot = reply.slot();
            if let (Some(s), "confirmed") = (slot, commitment) {
                seen.fetch_max(s, Ordering::Relaxed);
            }
            let mut st = state.lock().expect("load lock");
            let key = (source.name.clone(), method.label(), commitment);
            let cell = st.cells.entry(key).or_default();
            (cell.requests, cell.bytes) = (cell.requests + 1, cell.bytes + reply.bytes as u64);
            match (&reply.body, reply.rpc_error()) {
                (Err(_), _) if reply.timed_out => cell.timeouts += 1,
                (Err(_), _) => cell.http_errors += 1,
                (Ok(_), Some(code)) => *cell.rpc_errors.entry(code).or_default() += 1,
                (Ok(_), None) => cell.ms.push(reply.ms),
            }
            if let (Some(s), "processed") = (slot, commitment) {
                let fresh = st.fresh.entry(source.name.clone()).or_default();
                if confirmed > 0 {
                    (fresh.total, fresh.fresh) =
                        (fresh.total + 1, fresh.fresh + u64::from(s > confirmed));
                }
                fresh.behind_tip.extend(tip.map(|t| t as f64 - s as f64));
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    (source.name, skipped)
}
