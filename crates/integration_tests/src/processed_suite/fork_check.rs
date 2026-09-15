// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Classification of one fork burst response against its own chain.
//!
//! The expected value of a key at response slot S is the newest watcher touch on the chain ending
//! at S. With `--db-url`, a key the window leaves uncovered also gets an expected value when its
//! chain reaches the fork point with no write above it: the newest Postgres row at slot <= fork
//! point, read with the same indexed query as same-node. That catches a write from the other
//! branch served on this one. A null for a live key is an exclusion only when the owner filter or
//! a confirmed getAccountInfo -32010 probe proves it.

use super::Ctx;
use super::same_node::{self, SlotState};
use super::sources::{KeyCheck, check_chain};
use super::watcher::Watcher;
use serde_json::Value as JsonValue;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ForkClass {
    CanonicalOk,
    AbandonedConsistent,
    Mismatch,
    DeadServed,
    BehindConfirmed,
    Unresolved,
}

impl ForkClass {
    pub fn name(self) -> &'static str {
        match self {
            ForkClass::CanonicalOk => "canonical_ok",
            ForkClass::AbandonedConsistent => "abandoned_consistent",
            ForkClass::Mismatch => "mismatch",
            ForkClass::DeadServed => "dead_served",
            ForkClass::BehindConfirmed => "behind_confirmed",
            ForkClass::Unresolved => "unresolved",
        }
    }
}

/// `(lamports, owner)` per key at the fork point from Postgres. A missing account is `(0, "")`.
pub type Anchor = HashMap<String, (u64, String)>;

/// What the watcher knows about a response slot.
pub struct ChainFacts {
    pub dead: bool,
    pub canonical: Option<bool>,
    /// Expected `(lamports, owner)` per key, None where uncovered.
    pub expected: Vec<(String, Option<(u64, String)>)>,
    /// Owner exclusion per key from the filter or a probe, None while unknown.
    pub excluded: Vec<Option<bool>>,
}

pub struct Classified {
    pub class: ForkClass,
    pub example: Option<String>,
    /// Keys checked against a definite expected value.
    pub covered: u64,
}

pub fn classify(
    slot: u64,
    values: &[JsonValue],
    facts: &ChainFacts,
    confirmed_before: u64,
) -> Classified {
    let done = |class, example: Option<String>| Classified {
        class,
        example,
        covered: 0,
    };
    if facts.dead {
        let example = format!("slot {slot} is dead, restarted or descends from one");
        return done(ForkClass::DeadServed, Some(example));
    }
    if slot < confirmed_before {
        let example = format!("slot {slot} below confirmed {confirmed_before}");
        return done(ForkClass::BehindConfirmed, Some(example));
    }
    if values.len() != facts.expected.len() {
        let (got, want) = (values.len(), facts.expected.len());
        let example = format!("slot {slot}: {got} values for {want} keys");
        return done(ForkClass::Mismatch, Some(example));
    }
    let Some(canonical) = facts.canonical else {
        return done(ForkClass::Unresolved, None);
    };
    let mut covered = 0;
    let keys = facts.expected.iter().zip(&facts.excluded);
    for (value, ((key, want), excluded)) in values.iter().zip(keys) {
        let Some((lamports, owner)) = want else {
            continue;
        };
        match check_chain(value, (*lamports, owner), *excluded) {
            KeyCheck::Mismatch(m) => {
                let example = Some(format!("{key} at slot {slot}: {m}"));
                return Classified {
                    class: ForkClass::Mismatch,
                    example,
                    covered,
                };
            }
            KeyCheck::ExcludedUnknown => {}
            KeyCheck::Match | KeyCheck::Excluded => covered += 1,
        }
    }
    let class = match canonical {
        true => ForkClass::CanonicalOk,
        false => ForkClass::AbandonedConsistent,
    };
    Classified {
        class,
        example: None,
        covered,
    }
}

/// Key indexes whose null answer needs an exclusion probe before [`classify`].
pub fn unknown_nulls(values: &[JsonValue], facts: &ChainFacts) -> Vec<usize> {
    let keys = facts.expected.iter().zip(&facts.excluded);
    let rows = values.iter().zip(keys).enumerate();
    rows.filter(|(_, (value, ((_, want), excluded)))| {
        let live = want.as_ref().is_some_and(|(lamports, _)| *lamports > 0);
        value.is_null() && excluded.is_none() && live
    })
    .map(|(i, _)| i)
    .collect()
}

/// Watcher facts for `slot`. One short read lock per key, so the feed never waits on a burst.
pub fn facts(
    ctx: &Ctx,
    watcher: &Watcher,
    keys: &[String],
    slot: u64,
    canonical: Option<bool>,
    (fork_point, anchor): (u64, Option<&Anchor>),
) -> ChainFacts {
    let dead = watcher.read(|t| t.dead_chain(slot));
    let (mut expected, mut excluded) = (Vec::new(), Vec::new());
    for key in keys {
        let pubkey = Pubkey::from_str(key).ok();
        let value = pubkey.and_then(|pk| {
            watcher.read(|t| match t.lamports_at(&pk, slot) {
                Some((lamports, owner)) => Some((lamports, owner.to_string())),
                None if t.write_between(&pk, fork_point, slot) == Some(None) => {
                    anchor?.get(key).cloned()
                }
                None => None,
            })
        });
        let owner = value.as_ref().filter(|(lamports, _)| *lamports > 0);
        excluded.push(owner.and_then(|(_, owner)| ctx.owner_excluded(owner)));
        expected.push((key.clone(), value));
    }
    ChainFacts {
        dead,
        canonical,
        expected,
        excluded,
    }
}

/// Postgres values at the fork point once it is confirmed, None without `--db-url` or when the
/// fork point is abandoned, finalized already or not confirmed in time.
pub async fn anchor(ctx: &Ctx, fork_point: u64, keys: &[String]) -> Option<Anchor> {
    let db = ctx.db.as_ref()?;
    let give_up = ctx.give_up(Duration::from_secs(ctx.args.timeout));
    let rows = match same_node::settle(ctx, db, fork_point, keys, give_up).await {
        Ok(SlotState::Present(rows)) => rows,
        Ok(_) => return None,
        Err(e) => {
            eprintln!("forks: fork point read failed: {e:#}");
            return None;
        }
    };
    let value = |account: Option<JsonValue>| match account {
        Some(a) => (
            a["lamports"].as_u64().unwrap_or_default(),
            a["owner"].as_str().unwrap_or_default().to_string(),
        ),
        None => (0, String::new()),
    };
    Some(rows.into_iter().map(|(k, a)| (k, value(a))).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const OWNER: &str = "11111111111111111111111111111111";

    fn facts(dead: bool, canonical: Option<bool>, lamports: u64) -> ChainFacts {
        let expected = vec![
            ("k1".to_string(), Some((lamports, OWNER.to_string()))),
            ("k2".to_string(), None),
        ];
        ChainFacts {
            dead,
            canonical,
            expected,
            excluded: vec![None, None],
        }
    }

    #[test]
    fn classifies_each_class() {
        let values = [
            json!({"lamports": 5, "owner": OWNER}),
            json!({"lamports": 9, "owner": OWNER}),
        ];
        let class = |f: &ChainFacts, confirmed| classify(20, &values, f, confirmed).class;
        let ok = classify(20, &values, &facts(false, Some(true), 5), 19);
        assert_eq!((ok.class, ok.covered), (ForkClass::CanonicalOk, 1));
        let abandoned = facts(false, Some(false), 5);
        assert_eq!(class(&abandoned, 19), ForkClass::AbandonedConsistent);
        assert_eq!(class(&facts(false, Some(true), 6), 19), ForkClass::Mismatch);
        assert_eq!(
            class(&facts(false, Some(false), 6), 20),
            ForkClass::Mismatch
        );
        assert_eq!(
            class(&facts(true, Some(false), 5), 19),
            ForkClass::DeadServed
        );
        let behind = facts(false, Some(true), 5);
        assert_eq!(class(&behind, 21), ForkClass::BehindConfirmed);
        assert_eq!(class(&facts(false, None, 5), 19), ForkClass::Unresolved);
    }

    #[test]
    fn length_mismatch_is_a_mismatch() {
        let values = [json!({"lamports": 5, "owner": OWNER})];
        for canonical in [None, Some(true), Some(false)] {
            let c = classify(20, &values, &facts(false, canonical, 5), 19);
            assert_eq!(c.class, ForkClass::Mismatch);
        }
    }

    #[test]
    fn nulls_need_a_probe_until_exclusion_is_known() {
        let values = [JsonValue::Null, JsonValue::Null];
        let mut f = facts(false, Some(true), 5);
        assert_eq!(unknown_nulls(&values, &f), [0]);
        let unknown = classify(20, &values, &f, 19);
        assert_eq!(
            (unknown.class, unknown.covered),
            (ForkClass::CanonicalOk, 0)
        );
        f.excluded[0] = Some(true);
        assert!(unknown_nulls(&values, &f).is_empty());
        assert_eq!(classify(20, &values, &f, 19).covered, 1);
        f.excluded[0] = Some(false);
        assert_eq!(classify(20, &values, &f, 19).class, ForkClass::Mismatch);

        let served = [json!({"lamports": 5, "owner": OWNER}), JsonValue::Null];
        f.excluded[0] = Some(true);
        assert_eq!(classify(20, &served, &f, 19).class, ForkClass::Mismatch);
    }
}
