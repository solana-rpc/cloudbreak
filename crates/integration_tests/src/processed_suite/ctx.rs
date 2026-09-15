// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Run context shared by the fronts: sources, watcher, key pool, the stop signal and the helpers
//! that settle canonical status and owner exclusion.
//!
//! An excluded owner reads as null from memory and as -32010 from the Postgres path. Without a
//! loaded owner filter, a null for a live key is settled by a confirmed getAccountInfo probe.
//!
//! The run stops at `--duration` or on Ctrl-C, whichever comes first. After the stop, every wait
//! a front does to resolve its samples ends by the hard stop, at most 90 s after the stop.

use super::Args;
use super::keys::KeyPool;
use super::sources::{KeyCheck, OwnerFilter, Source, owner_excluded, settle_probe};
use super::watcher::Watcher;
use sea_orm::DatabaseConnection;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// Bound on all post-stop draining.
pub const DRAIN: Duration = Duration::from_secs(90);
/// Wait before `canonical` also asks the reference while the watcher has no answer.
const ORACLE_AFTER: Duration = Duration::from_secs(5);
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const MAX_EXCLUDED_KEYS: usize = 50_000;

/// Stop signal and caches, built once per run.
pub struct Control {
    stop: watch::Sender<bool>,
    hard_stop: Mutex<Instant>,
    canonical_cache: Mutex<HashMap<u64, bool>>,
    excluded_keys: Mutex<HashSet<String>>,
}

impl Control {
    pub fn new(deadline: Instant) -> Self {
        Self {
            stop: watch::channel(false).0,
            hard_stop: Mutex::new(deadline + DRAIN),
            canonical_cache: Mutex::default(),
            excluded_keys: Mutex::default(),
        }
    }
}

pub struct Ctx {
    pub args: Args,
    pub client: reqwest::Client,
    pub cloudbreak: Source,
    pub references: Vec<Source>,
    pub watcher: Option<Watcher>,
    pub pool: KeyPool,
    pub filter: Option<OwnerFilter>,
    pub db: Option<DatabaseConnection>,
    pub deadline: Instant,
    pub control: Control,
}

impl Ctx {
    pub fn running(&self) -> bool {
        !self.interrupted() && Instant::now() < self.deadline
    }

    pub fn interrupted(&self) -> bool {
        *self.control.stop.borrow()
    }

    /// Completes at the deadline or on Ctrl-C.
    pub async fn stopped(&self) {
        let mut stop = self.control.stop.subscribe();
        tokio::select! {
            _ = tokio::time::sleep_until(self.deadline) => {}
            _ = stop.wait_for(|s| *s) => {}
        }
    }

    /// Pulls the stop forward to now, and the hard stop to at most `DRAIN` from now.
    pub fn stop_now(&self) {
        self.control.stop.send_replace(true);
        let mut hard = self.control.hard_stop.lock().expect("stop lock");
        *hard = (*hard).min(Instant::now() + DRAIN);
    }

    fn hard_stop(&self) -> Instant {
        *self.control.hard_stop.lock().expect("stop lock")
    }

    /// End of a wait of `wait` from now, never past the hard stop.
    pub fn give_up(&self, wait: Duration) -> Instant {
        (Instant::now() + wait).min(self.hard_stop())
    }

    /// True once `give_up` or the hard stop has passed.
    pub fn past(&self, give_up: Instant) -> bool {
        Instant::now() >= give_up.min(self.hard_stop())
    }

    /// Cloudbreak first, then every reference.
    pub fn sources(&self) -> Vec<Source> {
        let refs = self.references.iter().cloned();
        std::iter::once(self.cloudbreak.clone())
            .chain(refs)
            .collect()
    }

    /// Cloudbreak's cached confirmed slot, the floor a processed answer must not go below.
    pub async fn confirmed_before(&self) -> Option<u64> {
        let key = self.pool.sample(1).pop();
        let key = key.unwrap_or_else(|| SYSTEM_PROGRAM.to_string());
        self.cloudbreak.cached_confirmed(&self.client, &key).await
    }

    /// Canonical status of `slot` from the watcher, or the first reference's getBlocks once its
    /// confirmed slot passes. None when neither answers within `wait`.
    pub async fn canonical(&self, slot: u64, wait: Duration) -> Option<bool> {
        let (start, give_up) = (Instant::now(), self.give_up(wait));
        let cache = &self.control.canonical_cache;
        loop {
            if let Some(known) = cache.lock().expect("cache lock").get(&slot) {
                return Some(*known);
            }
            let watcher = self.watcher.as_ref();
            let mut answer = watcher.and_then(|w| w.read(|t| t.canonical(slot)));
            if answer.is_none() && (watcher.is_none() || start.elapsed() > ORACLE_AFTER) {
                answer = self.oracle_canonical(slot).await;
            }
            if let Some(answer) = answer {
                cache.lock().expect("cache lock").insert(slot, answer);
                return Some(answer);
            }
            if self.past(give_up) {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn oracle_canonical(&self, slot: u64) -> Option<bool> {
        let oracle = self.references.first()?;
        if oracle.slot_at(&self.client, "confirmed").await? < slot {
            return None;
        }
        let blocks = oracle.blocks(&self.client, slot, slot).await?;
        Some(blocks.contains(&slot))
    }

    /// Newest watcher write to `key` in `(s, c]`, see [`super::tree::BlockTree::write_between`].
    pub fn write_between(&self, key: &str, s: u64, c: u64) -> Option<Option<(u64, String)>> {
        if s == c {
            return Some(None);
        }
        let key = Pubkey::from_str(key).ok()?;
        let watcher = self.watcher.as_ref()?;
        let write = watcher.read(|t| t.write_between(&key, s, c))?;
        Some(write.map(|(lamports, owner)| (lamports, owner.to_string())))
    }

    /// Owner exclusion from the loaded filter, None without one.
    pub fn owner_excluded(&self, owner: &str) -> Option<bool> {
        self.filter.as_ref().map(|f| owner_excluded(f, owner))
    }

    /// Exclusion of `key` from a confirmed getAccountInfo probe. Proven exclusions are cached.
    pub async fn probe(&self, key: &str, owner: &str) -> Option<bool> {
        let cached = &self.control.excluded_keys;
        if cached.lock().expect("excluded lock").contains(key) {
            return Some(true);
        }
        let verdict = (self.cloudbreak)
            .probe_excluded(&self.client, key, owner)
            .await;
        if verdict == Some(true) {
            let mut keys = cached.lock().expect("excluded lock");
            if keys.len() >= MAX_EXCLUDED_KEYS {
                keys.clear();
            }
            keys.insert(key.to_string());
        }
        verdict
    }

    /// Settles an `ExcludedUnknown` for `key` with a probe. Other checks pass through.
    pub async fn settle(&self, check: KeyCheck, key: &str, owner: &str) -> KeyCheck {
        match check {
            KeyCheck::ExcludedUnknown => settle_probe(check, self.probe(key, owner).await, owner),
            check => check,
        }
    }
}
