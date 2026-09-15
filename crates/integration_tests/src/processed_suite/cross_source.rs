// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! cross-source front: cloudbreak against every reference at equal context slots.
//!
//! Each sample sends the same getMultipleAccounts base64 to all sources at once. Equal slots
//! compare every account field exactly. Different slots only feed the slot delta distribution. A
//! processed mismatch counts as `mismatch` only when its slot is canonical, and as `fork_benign`
//! when it is not. The same run at confirmed is an informational control group.
//!
//! A cloudbreak null for a key the reference shows live is an exclusion only when the loaded
//! owner filter excludes the owner, or, without a filter, when a confirmed getAccountInfo probe
//! answers -32010. Anything else is a mismatch candidate.

use super::report::DeltaRow;
use super::sources::{KeyCheck, OwnerFilter, Reply, Source, compare_account};
use super::{Ctx, RESOLVE_WAIT, Tally};
use crate::utils::get_slot;
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;

const PACE: Duration = Duration::from_millis(500);

pub struct CrossReport {
    pub processed: Tally,
    pub control: Tally,
    pub deltas: Vec<DeltaRow>,
}

#[derive(Default)]
struct State {
    processed: Tally,
    control: Tally,
    deltas: BTreeMap<String, BTreeMap<i64, u64>>,
}

#[derive(Debug, PartialEq)]
pub enum Pair {
    Unusable,
    SlotDelta(i64),
    Same {
        slot: u64,
        checks: Vec<(usize, KeyCheck)>,
    },
}

/// Pairs two getMultipleAccounts responses, key index with its check when slots are equal.
pub fn pair(
    cb: Option<&JsonValue>,
    reference: Option<&JsonValue>,
    filter: Option<&OwnerFilter>,
) -> Pair {
    let (Some(cb), Some(reference)) = (cb, reference) else {
        return Pair::Unusable;
    };
    let (Some(cb_slot), Some(ref_slot)) = (get_slot(cb), get_slot(reference)) else {
        return Pair::Unusable;
    };
    if cb_slot != ref_slot {
        return Pair::SlotDelta(cb_slot as i64 - ref_slot as i64);
    }
    let values = |r: &JsonValue| r["result"]["value"].as_array().cloned();
    let (Some(cb_values), Some(ref_values)) = (values(cb), values(reference)) else {
        return Pair::Unusable;
    };
    let checks = match cb_values.len() == ref_values.len() {
        true => cb_values
            .iter()
            .zip(&ref_values)
            .map(|(c, r)| compare_account(c, r, filter))
            .enumerate()
            .collect(),
        false => vec![(0, KeyCheck::Mismatch("value counts differ".to_string()))],
    };
    Pair::Same {
        slot: cb_slot,
        checks,
    }
}

/// Class of a processed mismatch once its slot's canonical status is known.
pub fn classify_mismatch(canonical: Option<bool>) -> &'static str {
    match canonical {
        Some(true) => "mismatch",
        Some(false) => "fork_benign",
        None => "unresolved",
    }
}

pub async fn run(ctx: Arc<Ctx>) -> CrossReport {
    let state = Arc::new(Mutex::new(State::default()));
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(PACE);
    let mut processed = true;
    while ctx.running() {
        tokio::select! {
            _ = ctx.stopped() => break,
            _ = tick.tick() => {}
        }
        let keys = ctx.pool.sample(ctx.args.keys_per_sample);
        if !keys.is_empty() {
            tasks.spawn(sample(ctx.clone(), processed, keys, state.clone()));
        }
        processed = !processed;
        while tasks.try_join_next().is_some() {}
    }
    while tasks.join_next().await.is_some() {}
    let state = std::mem::take(&mut *state.lock().expect("cross-source lock"));
    let deltas = state.deltas.into_iter().map(|(reference, counts)| {
        let total = counts.values().sum();
        let equal_pct = super::report::pct(counts.get(&0).copied().unwrap_or(0), total);
        DeltaRow {
            reference,
            equal_pct,
            counts,
        }
    });
    CrossReport {
        processed: state.processed,
        control: state.control,
        deltas: deltas.collect(),
    }
}

async fn read(ctx: &Ctx, source: &Source, params: &JsonValue) -> Reply {
    let params = params.clone();
    source
        .call(&ctx.client, "getMultipleAccounts", params)
        .await
}

fn owner_at(reply: &Reply, i: usize) -> Option<String> {
    let value = reply.values()?.get(i)?;
    value["owner"].as_str().map(str::to_string)
}

async fn sample(ctx: Arc<Ctx>, processed: bool, keys: Vec<String>, state: Arc<Mutex<State>>) {
    let commitment = if processed { "processed" } else { "confirmed" };
    let params = json!([keys, {"commitment": commitment, "encoding": "base64"}]);
    let refs = futures::future::join_all(ctx.references.iter().map(|r| read(&ctx, r, &params)));
    let (cb, refs) = tokio::join!(read(&ctx, &ctx.cloudbreak, &params), refs);

    let mut outcomes = Vec::new();
    for (reference, reply) in ctx.references.iter().zip(&refs) {
        let mut outcome = pair(cb.json(), reply.json(), ctx.filter.as_ref());
        if let Pair::Same { checks, .. } = &mut outcome {
            for (i, check) in checks.iter_mut() {
                if *check != KeyCheck::ExcludedUnknown {
                    continue;
                }
                let owner = owner_at(reply, *i).unwrap_or_else(|| "unknown".to_string());
                *check = ctx.settle(check.clone(), &keys[*i], &owner).await;
            }
        }
        outcomes.push((reference.name.clone(), outcome));
    }

    let mut pending = Vec::new();
    {
        let mut st = state.lock().expect("cross-source lock");
        for (reference, outcome) in outcomes {
            let delta = match &outcome {
                Pair::SlotDelta(d) => Some(*d),
                Pair::Same { .. } => Some(0),
                Pair::Unusable => None,
            };
            if let (true, Some(d)) = (processed, delta) {
                let counts = st.deltas.entry(reference.clone()).or_default();
                *counts.entry(d).or_default() += 1;
            }
            let tally = if processed {
                &mut st.processed
            } else {
                &mut st.control
            };
            let asked = keys.len() as u64;
            match outcome {
                Pair::Unusable => (tally.note("unusable"), tally.cover(0, asked)).0,
                Pair::SlotDelta(_) => (tally.note("slot_differs"), tally.cover(0, asked)).0,
                Pair::Same { slot, checks } => {
                    for (i, check) in checks {
                        tally.samples += 1;
                        let known = check != KeyCheck::ExcludedUnknown;
                        tally.cover(u64::from(known), 1);
                        let example = || format!("{reference} key {} at slot {slot}", keys[i]);
                        match check {
                            KeyCheck::Match => tally.pass(),
                            KeyCheck::Excluded => tally.note("excluded"),
                            KeyCheck::ExcludedUnknown => tally.note("excluded_unknown"),
                            KeyCheck::Mismatch(m) if processed => {
                                pending.push((slot, format!("{}: {m}", example())))
                            }
                            KeyCheck::Mismatch(m) => {
                                tally.fail("mismatch", format!("{}: {m}", example()))
                            }
                        }
                    }
                }
            }
        }
    }
    let resolved = pending.into_iter().map(|(slot, example)| {
        let ctx = &ctx;
        async move {
            (
                classify_mismatch(ctx.canonical(slot, RESOLVE_WAIT).await),
                example,
            )
        }
    });
    for (class, example) in futures::future::join_all(resolved).await {
        let mut st = state.lock().expect("cross-source lock");
        match class {
            "mismatch" => st.processed.fail(class, example),
            _ => st.processed.note(class),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const OWNER: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    fn gma(slot: u64, values: JsonValue) -> JsonValue {
        json!({"result": {"context": {"slot": slot}, "value": values}})
    }

    #[test]
    fn pairs_and_classifies() {
        let account = |lamports: u64| {
            json!({"lamports": lamports, "owner": OWNER, "executable": false,
            "rentEpoch": 0, "space": 0, "data": ["", "base64"]})
        };
        let reference = gma(10, json!([account(5), account(7), null]));

        assert_eq!(
            pair(Some(&gma(12, json!([]))), Some(&reference), None),
            Pair::SlotDelta(2)
        );
        assert_eq!(pair(None, Some(&reference), None), Pair::Unusable);

        let cb = gma(10, json!([account(5), null, account(1)]));
        let Pair::Same { slot, checks } = pair(Some(&cb), Some(&reference), None) else {
            panic!("same slot")
        };
        assert_eq!(slot, 10);
        assert_eq!(checks[0], (0, KeyCheck::Match));
        assert_eq!(checks[1], (1, KeyCheck::ExcludedUnknown));
        assert!(matches!(checks[2], (2, KeyCheck::Mismatch(_))));

        let excluding = (false, HashSet::from([OWNER.to_string()]));
        let Pair::Same { checks, .. } = pair(Some(&cb), Some(&reference), Some(&excluding)) else {
            panic!("same slot")
        };
        assert_eq!(checks[1], (1, KeyCheck::Excluded));
        assert!(matches!(checks[0], (0, KeyCheck::Mismatch(_))));
        let including = (true, HashSet::from([OWNER.to_string()]));
        let Pair::Same { checks, .. } = pair(Some(&cb), Some(&reference), Some(&including)) else {
            panic!("same slot")
        };
        assert!(matches!(checks[1], (1, KeyCheck::Mismatch(_))));

        assert_eq!(classify_mismatch(Some(true)), "mismatch");
        assert_eq!(classify_mismatch(Some(false)), "fork_benign");
        assert_eq!(classify_mismatch(None), "unresolved");
    }
}
