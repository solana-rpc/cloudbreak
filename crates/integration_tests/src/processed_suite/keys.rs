// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Key pool shared by the fronts: fixed keys from `--pubkeys-file` plus a bounded ring of keys
//! touched by recent watcher blocks. Token accounts go to a second ring for the token methods.
//!
//! Vote-program owned accounts from watcher blocks never enter the pool, since they change every
//! slot. The block tree still keeps them, so a vote account listed in `--pubkeys-file` is checked
//! like any other key.

use super::tree::Touch;
use super::watcher::Watcher;
use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use solana_pubkey::Pubkey;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

const MAX_RECENT: usize = 20_000;
const MAX_TOKENS: usize = 2_000;
const PER_BLOCK: usize = 32;

#[derive(Default)]
struct Inner {
    fixed: Vec<String>,
    recent: VecDeque<String>,
    seen: HashSet<String>,
    tokens: VecDeque<String>,
}

#[derive(Clone, Default)]
pub struct KeyPool {
    inner: Arc<RwLock<Inner>>,
    sampled: Arc<AtomicU64>,
}

impl KeyPool {
    pub fn new(fixed: Vec<String>) -> Self {
        let inner = Inner {
            fixed,
            ..Inner::default()
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
            sampled: Arc::default(),
        }
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.read().expect("key pool lock");
        inner.fixed.len() + inner.recent.len()
    }

    /// Keys handed out to requests so far.
    pub fn sampled(&self) -> u64 {
        self.sampled.load(Ordering::Relaxed)
    }

    /// Up to `n` distinct keys drawn from the fixed and recent keys.
    pub fn sample(&self, n: usize) -> Vec<String> {
        let inner = self.inner.read().expect("key pool lock");
        let total = inner.fixed.len() + inner.recent.len();
        let mut rng = rand::thread_rng();
        let picks = rand::seq::index::sample(&mut rng, total, n.min(total));
        let at = |i: usize| match i.checked_sub(inner.fixed.len()) {
            None => inner.fixed[i].clone(),
            Some(j) => inner.recent[j].clone(),
        };
        let keys: Vec<String> = picks.into_iter().map(at).collect();
        self.sampled.fetch_add(keys.len() as u64, Ordering::Relaxed);
        keys
    }

    pub fn token_key(&self) -> Option<String> {
        let inner = self.inner.read().expect("key pool lock");
        let (a, b) = inner.tokens.as_slices();
        let key = [a, b].concat().choose(&mut rand::thread_rng()).cloned();
        self.sampled
            .fetch_add(u64::from(key.is_some()), Ordering::Relaxed);
        key
    }

    /// Adds a random handful of a block's non-vote touched keys.
    pub fn add_block(&self, touched: &[Touch]) {
        let eligible: Vec<&Touch> = touched.iter().filter(|t| !t.is_vote()).collect();
        let picks = eligible.choose_multiple(&mut rand::thread_rng(), PER_BLOCK);
        let mut inner = self.inner.write().expect("key pool lock");
        for touch in picks {
            let key = touch.key.to_string();
            if touch.is_token() && !inner.tokens.contains(&key) {
                inner.tokens.push_back(key.clone());
                if inner.tokens.len() > MAX_TOKENS {
                    inner.tokens.pop_front();
                }
            }
            if inner.seen.insert(key.clone()) {
                inner.recent.push_back(key);
            }
            if inner.recent.len() > MAX_RECENT
                && let Some(old) = inner.recent.pop_front()
            {
                inner.seen.remove(&old);
            }
        }
    }

    /// Feeds the pool from the watcher's block events until the channel closes.
    pub fn follow(&self, watcher: &Watcher) -> JoinHandle<()> {
        let (pool, mut events) = (self.clone(), watcher.subscribe());
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(super::tree::Event::Block { touched, .. }) => pool.add_block(&touched),
                    Ok(_) | Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => return,
                }
            }
        })
    }
}

pub fn read_pubkeys_file(path: &std::path::Path) -> Result<Vec<String>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    parse_pubkeys(&text)
}

/// One base58 pubkey per line. Blank lines and # comments are ignored.
pub fn parse_pubkeys(text: &str) -> Result<Vec<String>> {
    let lines = text
        .lines()
        .map(|line| line.split('#').next().unwrap_or_default().trim());
    let mut seen = HashSet::new();
    let keys = lines
        .filter(|key| !key.is_empty())
        .filter(|key| seen.insert(key.to_string()));
    keys.map(|key| {
        Ok(Pubkey::from_str(key)
            .context(format!("invalid pubkey {key}"))?
            .to_string())
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processed_suite::tree::tests::key;

    const KEY: &str = "4Nd1mBQtrMJVYVfKf2PJy9NZUZdTAsp7D4xWLs4gDB4T";
    const OWNER: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    #[test]
    fn parses_pubkeys_file() {
        let text = format!("# hot keys\n\n{KEY}\n  {OWNER}  # token program\n{KEY}\n");
        assert_eq!(parse_pubkeys(&text).unwrap(), [KEY, OWNER]);
        assert!(parse_pubkeys("not-a-pubkey\n").is_err());
    }

    #[test]
    fn vote_touches_stay_out_of_the_pool() {
        let touch = |n, owner| Touch {
            key: key(n),
            lamports: 1,
            owner,
        };
        let pool = KeyPool::new(vec![key(9).to_string()]);
        pool.add_block(&[touch(1, 2), touch(2, 3), touch(3, 0)]);
        assert_eq!(pool.len(), 3);
        let mut keys = pool.sample(10);
        keys.sort();
        let mut want = [key(2), key(3), key(9)].map(|k| k.to_string());
        want.sort();
        assert_eq!(keys, want);
        assert_eq!(pool.token_key(), Some(key(3).to_string()));
    }
}
