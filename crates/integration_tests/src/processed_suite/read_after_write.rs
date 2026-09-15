// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! read-after-write front: how soon a block's lamports are visible on each source.
//!
//! For one in `--raw-every` watcher blocks, `--raw-keys` random non-vote touched keys are polled on
//! every source every 200 ms for up to 10 s, until the block's lamports show at a slot at or above
//! the block. A key the watcher sees written again is dropped. Each source runs at most 8 probes
//! at once, and a block that finds the cap full is counted `busy` for that source.
//!
//! Propagation is block receipt to the matching reply's receipt, minus the local wait for permits
//! and tokens across that probe's polls. The local wait is reported on its own. A null for a live
//! account is an exclusion only when the owner filter or a confirmed getAccountInfo -32010 probe
//! proves it.

use super::report::{RawRow, summarize};
use super::sources::{KeyCheck, Source, check_chain};
use super::watcher::Watcher;
use super::{Ctx, Tally};
use rand::seq::SliceRandom;
use serde_json::json;
use solana_pubkey::Pubkey;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinSet;

const POLL: Duration = Duration::from_millis(200);
const WAIT: Duration = Duration::from_secs(10);
const PROBES_PER_SOURCE: usize = 8;

#[derive(Clone)]
struct Probe {
    key: Pubkey,
    lamports: u64,
    owner: String,
    /// Exclusion from a confirmed getAccountInfo probe, asked at most once.
    excluded: Option<bool>,
}

#[derive(Default)]
struct RawCell {
    ms: Vec<f64>,
    queued_ms: Vec<f64>,
    timeouts: u64,
    dropped: u64,
    excluded: u64,
    busy: u64,
}

impl RawCell {
    fn merge(&mut self, other: RawCell) {
        self.ms.extend(other.ms);
        self.queued_ms.extend(other.queued_ms);
        self.timeouts += other.timeouts;
        self.dropped += other.dropped;
        self.excluded += other.excluded;
        self.busy += other.busy;
    }
}

pub struct RawReport {
    pub tally: Tally,
    pub rows: Vec<RawRow>,
}

pub async fn run(ctx: Arc<Ctx>) -> RawReport {
    let watcher = (ctx.watcher.clone()).expect("read-after-write needs --grpc-endpoint");
    let state: Arc<Mutex<BTreeMap<String, RawCell>>> = Arc::default();
    let limits: HashMap<String, Arc<Semaphore>> = (ctx.sources().iter())
        .map(|s| (s.name.clone(), Arc::new(Semaphore::new(PROBES_PER_SOURCE))))
        .collect();
    let (mut events, mut tasks, mut blocks) = (watcher.subscribe(), JoinSet::new(), 0u64);
    loop {
        let event = tokio::select! {
            _ = ctx.stopped() => break,
            event = events.recv() => event,
        };
        let (slot, received_at, touched) = match event {
            Err(RecvError::Closed) => break,
            Ok(super::tree::Event::Block {
                slot,
                received_at,
                touched,
            }) => (slot, received_at, touched),
            Ok(_) | Err(RecvError::Lagged(_)) => continue,
        };
        blocks += 1;
        if blocks % ctx.args.raw_every.max(1) != 0 {
            continue;
        }
        let eligible: Vec<_> = touched.iter().filter(|t| !t.is_vote()).collect();
        let picked = eligible.choose_multiple(&mut rand::thread_rng(), ctx.args.raw_keys);
        let probes: Vec<Probe> = watcher.read(|t| {
            let probe = |x: &&&super::tree::Touch| Probe {
                key: x.key,
                lamports: x.lamports,
                owner: t.owner(x.owner).to_string(),
                excluded: None,
            };
            picked.map(|x| probe(&x)).collect()
        });
        if probes.is_empty() {
            continue;
        }
        for source in ctx.sources() {
            let Ok(permit) = limits[&source.name].clone().try_acquire_owned() else {
                let mut st = state.lock().expect("raw lock");
                st.entry(source.name.clone()).or_default().busy += 1;
                continue;
            };
            let (ctx, watcher, state, probes) =
                (ctx.clone(), watcher.clone(), state.clone(), probes.clone());
            tasks.spawn(async move {
                let cell = poll(&ctx, &watcher, &source, slot, received_at, probes).await;
                drop(permit);
                let mut st = state.lock().expect("raw lock");
                st.entry(source.name.clone()).or_default().merge(cell);
            });
        }
        while tasks.try_join_next().is_some() {}
    }
    drop(events);
    while tasks.join_next().await.is_some() {}
    let state = std::mem::take(&mut *state.lock().expect("raw lock"));
    let mut tally = Tally::default();
    for cell in state.values() {
        let done = cell.ms.len() as u64;
        let samples = done + cell.timeouts + cell.dropped + cell.excluded;
        (tally.samples, tally.passed) = (tally.samples + samples, tally.passed + done);
        tally.cover(samples - cell.timeouts, samples);
        let classes = [
            ("timeout", cell.timeouts),
            ("dropped", cell.dropped),
            ("excluded", cell.excluded),
            ("busy", cell.busy),
        ];
        for (class, n) in classes {
            *tally.classes.entry(class.to_string()).or_default() += n;
        }
    }
    let rows = state.into_iter().map(|(source, c)| RawRow {
        source,
        propagation_ms: summarize(c.ms),
        queued_ms: summarize(c.queued_ms),
        timeouts: c.timeouts,
        dropped: c.dropped,
        excluded: c.excluded,
    });
    RawReport {
        tally,
        rows: rows.collect(),
    }
}

async fn poll(
    ctx: &Ctx,
    watcher: &Watcher,
    source: &Source,
    slot: u64,
    t0: Instant,
    mut pending: Vec<Probe>,
) -> RawCell {
    let mut cell = RawCell::default();
    let config = json!({"commitment": "processed", "encoding": "base64",
        "dataSlice": {"offset": 0, "length": 0}});
    let mut queued = 0.0;
    while !pending.is_empty() && t0.elapsed() < WAIT {
        let keys: Vec<String> = pending.iter().map(|p| p.key.to_string()).collect();
        let reply = source
            .call(&ctx.client, "getMultipleAccounts", json!([keys, config]))
            .await;
        queued += reply.queued_ms;
        let seen = reply.received.into_std().saturating_duration_since(t0);
        let at = (seen.as_secs_f64() * 1000.0 - queued).max(0.0);
        let at_slot = reply.slot().is_some_and(|s| s >= slot);
        let values = reply
            .values()
            .filter(|v| at_slot && v.len() == pending.len());
        let mut keep = Vec::new();
        for (i, mut probe) in pending.into_iter().enumerate() {
            let excluded = ctx.owner_excluded(&probe.owner).or(probe.excluded);
            let want = (probe.lamports, probe.owner.as_str());
            let mut check = values.map(|v| check_chain(&v[i], want, excluded));
            if check == Some(KeyCheck::ExcludedUnknown) && probe.excluded.is_none() {
                probe.excluded = ctx.probe(&probe.key.to_string(), &probe.owner).await;
                check = values.map(|v| check_chain(&v[i], want, probe.excluded));
            }
            match check {
                Some(KeyCheck::Match) => (cell.ms.push(at), cell.queued_ms.push(queued)).0,
                Some(KeyCheck::Excluded) => cell.excluded += 1,
                _ if watcher.read(|t| t.touched_after(&probe.key, slot)) => cell.dropped += 1,
                _ => keep.push(probe),
            }
        }
        pending = keep;
        tokio::time::sleep(POLL).await;
    }
    cell.timeouts = pending.len() as u64;
    cell
}
