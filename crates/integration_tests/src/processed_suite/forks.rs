// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! forks front: request bursts around fork and dead-slot events, each answer checked against its
//! own chain by [`super::fork_check`].
//!
//! A burst sends processed getMultipleAccounts every 100 ms for `--fork-burst`, over random keys
//! touched by every branch above the fork point, topped up from the pool. A fork or dead event
//! during a burst is dropped, except that one dead event is queued and bursts right after it.
//! Once the watcher knows the canonical status of a response slot S, or 60 s pass, the response
//! gets one class. A response with no covered key does not pass.
//!
//! After resolution, one more read of the burst keys must match its chain once confirmed and the
//! tip pass the fork. A read on an abandoned slot is retried once after the tip passes that slot.
//! A second abandoned read is `convergence_abandoned`, reported but not a failure.

use super::fork_check::{self, Anchor, Classified, ForkClass};
use super::tree::Event;
use super::watcher::Watcher;
use super::{Ctx, RESOLVE_WAIT, Tally, metrics};
use serde_json::{Value as JsonValue, json};
use solana_pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinSet;
use tokio::time::Instant;

const TICK: Duration = Duration::from_millis(100);
const MAX_KEYS: usize = 100;
/// Slots the tip must be past a slot before a convergence read.
const TIP_LEAD: u64 = 2;

pub fn count_regressions(slots: &[u64]) -> u64 {
    slots.windows(2).filter(|w| w[1] < w[0]).count() as u64
}

pub struct ForkReport {
    pub tally: Tally,
    pub bursts: u64,
    pub responses: u64,
    pub regressions: u64,
    pub metric_deltas: BTreeMap<String, f64>,
}

pub async fn run(ctx: Arc<Ctx>) -> ForkReport {
    let watcher = ctx.watcher.clone().expect("forks needs --grpc-endpoint");
    let mut events = watcher.subscribe();
    let (mut tasks, mut bursts) = (JoinSet::new(), 0);
    let (mut busy_until, mut queued) = (Instant::now(), None);
    loop {
        let wake = busy_until.max(Instant::now());
        let event = tokio::select! {
            _ = ctx.stopped() => break,
            _ = tokio::time::sleep_until(wake), if queued.is_some() => None,
            event = events.recv() => match event {
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(_)) => continue,
                Ok(event) => Some(event),
            },
        };
        let busy = Instant::now() < busy_until;
        let fork_point = match event {
            None if !busy => queued.take(),
            Some(Event::Fork { fork_point }) if !busy => Some(fork_point),
            Some(Event::Dead { slot }) => {
                let parent = watcher.read(|t| t.first_at(slot).map(|b| b.parent_slot));
                let parent = parent.unwrap_or(slot.saturating_sub(1));
                if busy {
                    queued.get_or_insert(parent);
                }
                (!busy).then_some(parent)
            }
            _ => None,
        };
        let Some(fork_point) = fork_point else {
            continue;
        };
        busy_until = Instant::now() + ctx.args.fork_burst;
        bursts += 1;
        tasks.spawn(burst(ctx.clone(), watcher.clone(), fork_point));
    }
    drop(events);
    let mut report = ForkReport {
        tally: Tally::default(),
        bursts,
        responses: 0,
        regressions: 0,
        metric_deltas: BTreeMap::new(),
    };
    for outcome in tasks.join_all().await {
        report.responses += outcome.responses;
        report.regressions += outcome.regressions;
        report.tally.merge(outcome.tally);
        for (k, v) in outcome.deltas {
            *report.metric_deltas.entry(k).or_default() += v;
        }
    }
    report
}

struct Response {
    at: Instant,
    slot: Option<u64>,
    values: Option<Vec<JsonValue>>,
}

struct BurstOutcome {
    tally: Tally,
    responses: u64,
    regressions: u64,
    deltas: BTreeMap<String, f64>,
}

async fn read(ctx: &Ctx, keys: &[String]) -> Response {
    let config = json!({"commitment": "processed", "encoding": "base64",
        "dataSlice": {"offset": 0, "length": 0}});
    let at = Instant::now();
    let reply = (ctx.cloudbreak)
        .call(&ctx.client, "getMultipleAccounts", json!([keys, config]))
        .await;
    Response {
        at,
        slot: reply.slot(),
        values: reply.values().cloned(),
    }
}

async fn burst(ctx: Arc<Ctx>, watcher: Watcher, fork_point: u64) -> BurstOutcome {
    let confirmed_before = ctx.confirmed_before().await.unwrap_or(0);
    let url = ctx.args.cloudbreak_metrics.clone();
    let before = match &url {
        Some(url) => metrics::scrape(&ctx, url).await,
        None => None,
    };
    let branch = watcher.read(|t| t.branch_keys(fork_point, MAX_KEYS));
    let mut keys: Vec<String> = branch.iter().map(Pubkey::to_string).collect();
    keys.extend(ctx.pool.sample(MAX_KEYS - keys.len()));
    keys.sort_unstable();
    keys.dedup();

    let sends = async {
        let (mut calls, mut tick) = (JoinSet::new(), tokio::time::interval(TICK));
        let end = Instant::now() + ctx.args.fork_burst;
        while Instant::now() < end {
            tick.tick().await;
            let (ctx, keys) = (ctx.clone(), keys.clone());
            calls.spawn(async move { read(&ctx, &keys).await });
        }
        calls.join_all().await
    };
    let (mut responses, anchor) = tokio::join!(sends, fork_check::anchor(&ctx, fork_point, &keys));
    responses.sort_by_key(|r| r.at);
    let slots: Vec<u64> = responses.iter().filter_map(|r| r.slot).collect();
    let deltas = match (before, &url) {
        (Some(before), Some(url)) => metrics::scrape(&ctx, url)
            .await
            .map(|after| metrics::fork_deltas(&before, &after)),
        _ => None,
    };

    let mut check = Checker {
        ctx: &ctx,
        watcher: &watcher,
        keys: &keys,
        fork_point,
        anchor: anchor.as_ref(),
        probes: HashMap::new(),
    };
    let mut tally = Tally::default();
    let canonical = check.resolve(slots.iter().copied().collect()).await;
    for response in &responses {
        tally.samples += 1;
        let (Some(slot), Some(values)) = (response.slot, &response.values) else {
            tally.note("error");
            tally.cover(0, keys.len() as u64);
            continue;
        };
        let classified = check
            .check(slot, values, canonical[&slot], confirmed_before)
            .await;
        tally.cover(classified.covered, keys.len() as u64);
        record(&mut tally, classified, false);
    }
    check.converge(&mut tally).await;
    BurstOutcome {
        tally,
        responses: responses.len() as u64,
        regressions: count_regressions(&slots),
        deltas: deltas.unwrap_or_default(),
    }
}

/// Burst state shared by the response checks and the convergence read.
struct Checker<'a> {
    ctx: &'a Ctx,
    watcher: &'a Watcher,
    keys: &'a [String],
    fork_point: u64,
    anchor: Option<&'a Anchor>,
    /// Probe result per key, so each key is probed at most once per burst.
    probes: HashMap<String, Option<bool>>,
}

impl Checker<'_> {
    /// Canonical status per slot, polled until all are known or the wait ends.
    async fn resolve(&self, slots: BTreeSet<u64>) -> HashMap<u64, Option<bool>> {
        let give_up = self.ctx.give_up(RESOLVE_WAIT);
        loop {
            let status: HashMap<u64, Option<bool>> =
                (self.watcher).read(|t| slots.iter().map(|s| (*s, t.canonical(*s))).collect());
            if status.values().all(Option::is_some) || self.ctx.past(give_up) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn check(
        &mut self,
        slot: u64,
        values: &[JsonValue],
        canonical: Option<bool>,
        confirmed_before: u64,
    ) -> Classified {
        let chain = (self.fork_point, self.anchor);
        let (ctx, watcher, keys) = (self.ctx, self.watcher, self.keys);
        let mut facts = fork_check::facts(ctx, watcher, keys, slot, canonical, chain);
        for i in fork_check::unknown_nulls(values, &facts) {
            let (key, want) = &facts.expected[i];
            let owner = want.as_ref().map(|(_, owner)| owner.as_str());
            let probe = match self.probes.get(key) {
                Some(probe) => *probe,
                None => {
                    let probe = ctx.probe(key, owner.unwrap_or_default()).await;
                    *self.probes.entry(key.clone()).or_default() = probe;
                    probe
                }
            };
            facts.excluded[i] = probe;
        }
        fork_check::classify(slot, values, &facts, confirmed_before)
    }

    /// Waits until confirmed passes `floor` and the tip is `TIP_LEAD` past it.
    async fn wait_past(&self, floor: u64, give_up: Instant) -> bool {
        loop {
            let passed = |t: &super::tree::BlockTree| {
                t.highest_confirmed().is_some_and(|c| c > floor) && t.tip() > floor + TIP_LEAD
            };
            if self.watcher.read(passed) {
                return true;
            }
            if self.ctx.past(give_up) {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn converge(&mut self, tally: &mut Tally) {
        let give_up = self.ctx.give_up(RESOLVE_WAIT);
        let mut floor = self.fork_point;
        for attempt in 0..2 {
            if !self.wait_past(floor, give_up).await {
                return tally.note("convergence_unresolved");
            }
            let confirmed_before = self.ctx.confirmed_before().await.unwrap_or(0);
            let response = read(self.ctx, self.keys).await;
            let (Some(slot), Some(values)) = (response.slot, response.values) else {
                return tally.note("convergence_error");
            };
            let canonical = self.resolve(BTreeSet::from([slot])).await[&slot];
            let classified = self.check(slot, &values, canonical, confirmed_before).await;
            if classified.class == ForkClass::AbandonedConsistent && attempt == 0 {
                tally.note("convergence_retried");
                floor = slot;
                continue;
            }
            return record(tally, classified, true);
        }
    }
}

fn record(tally: &mut Tally, c: Classified, convergence: bool) {
    let prefix = if convergence { "convergence_" } else { "" };
    let name = format!("{prefix}{}", c.class.name());
    match c.class {
        ForkClass::Mismatch | ForkClass::DeadServed | ForkClass::BehindConfirmed => {
            tally.fail(&name, c.example.unwrap_or_default())
        }
        ForkClass::AbandonedConsistent if convergence => tally.note("convergence_abandoned"),
        ForkClass::CanonicalOk | ForkClass::AbandonedConsistent if c.covered == 0 => {
            tally.note(&format!("{name}_uncovered"))
        }
        ForkClass::CanonicalOk | ForkClass::AbandonedConsistent => {
            tally.pass();
            tally.note(&name);
        }
        ForkClass::Unresolved => tally.note(&name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classified(class: ForkClass, covered: u64) -> Classified {
        Classified {
            class,
            example: Some("e".into()),
            covered,
        }
    }

    #[test]
    fn records_classes_and_regressions() {
        let mut t = Tally::default();
        record(&mut t, classified(ForkClass::CanonicalOk, 3), false);
        record(&mut t, classified(ForkClass::AbandonedConsistent, 0), false);
        record(&mut t, classified(ForkClass::AbandonedConsistent, 2), true);
        record(&mut t, classified(ForkClass::DeadServed, 0), true);
        assert_eq!((t.passed, t.failed), (1, 1));
        assert_eq!(t.classes["abandoned_consistent_uncovered"], 1);
        assert_eq!(t.classes["convergence_abandoned"], 1);
        assert_eq!(t.failures["convergence_dead_served"], 1);
        assert_eq!(count_regressions(&[5, 6, 5, 7, 6]), 2);
    }
}
