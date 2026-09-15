// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! convergence front: a processed value at slot S holds once cloudbreak's confirmed passes S.
//!
//! For each sample the front reads processed at S, then confirmed with `minContextSlot` S until
//! the confirmed context slot C is at or above S. With no watcher write to a key on the canonical
//! chain in `(S, C]` the confirmed value must equal the processed one. With a write, confirmed
//! lamports and owner must equal that write. A non-canonical S is abandoned, and a range the
//! watcher does not cover is skipped as `uncovered`. A confirmed null after a live write is an
//! exclusion only when the owner filter or a confirmed getAccountInfo -32010 probe proves it.

use super::sources::{KeyCheck, check_chain, diff_fields};
use super::{Ctx, Tally};
use serde_json::{Value as JsonValue, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;

const PACE: Duration = Duration::from_millis(500);
const WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(400);

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Pass,
    Abandoned,
    Uncovered,
    Unresolved,
    Excluded,
    ExcludedUnknown,
    Mismatch(String),
}

impl From<KeyCheck> for Verdict {
    fn from(check: KeyCheck) -> Self {
        match check {
            KeyCheck::Match => Verdict::Pass,
            KeyCheck::Excluded => Verdict::Excluded,
            KeyCheck::ExcludedUnknown => Verdict::ExcludedUnknown,
            KeyCheck::Mismatch(m) => Verdict::Mismatch(format!("after a write in (S, C]: {m}")),
        }
    }
}

/// `window` is the watcher's newest write in `(S, C]`: None uncovered, Some(None) no write.
/// `excluded` is the filter's answer for the write's owner, None without a filter.
pub fn decide(
    canonical: Option<bool>,
    window: Option<Option<(u64, String)>>,
    processed: &JsonValue,
    confirmed: &JsonValue,
    excluded: Option<bool>,
) -> Verdict {
    match (canonical, window) {
        (None, _) => Verdict::Unresolved,
        (Some(false), _) => Verdict::Abandoned,
        (Some(true), None) => Verdict::Uncovered,
        (Some(true), Some(None)) if processed == confirmed => Verdict::Pass,
        (Some(true), Some(None)) if processed.is_null() || confirmed.is_null() => {
            Verdict::Mismatch("no write in (S, C] but one side is null".to_string())
        }
        (Some(true), Some(None)) => {
            let diff = diff_fields(processed, confirmed).join(", ");
            Verdict::Mismatch(format!("no write in (S, C] but fields differ: {diff}"))
        }
        (Some(true), Some(Some((lamports, owner)))) => {
            check_chain(confirmed, (lamports, &owner), excluded).into()
        }
    }
}

pub async fn run(ctx: Arc<Ctx>) -> Tally {
    let tally = Arc::new(Mutex::new(Tally::default()));
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(PACE);
    while ctx.running() {
        tokio::select! {
            _ = ctx.stopped() => break,
            _ = tick.tick() => {}
        }
        let keys = ctx.pool.sample(ctx.args.keys_per_sample);
        if !keys.is_empty() {
            tasks.spawn(sample(ctx.clone(), keys, tally.clone()));
        }
        while tasks.try_join_next().is_some() {}
    }
    while tasks.join_next().await.is_some() {}
    std::mem::take(&mut *tally.lock().expect("convergence lock"))
}

async fn sample(ctx: Arc<Ctx>, keys: Vec<String>, tally: Arc<Mutex<Tally>>) {
    let note = |class: &str| tally.lock().expect("convergence lock").note(class);
    let params = json!([keys, {"commitment": "processed", "encoding": "base64"}]);
    let reply = (ctx.cloudbreak)
        .call(&ctx.client, "getMultipleAccounts", params)
        .await;
    let values = reply.values().filter(|v| v.len() == keys.len());
    let (Some(s), Some(processed)) = (reply.slot(), values) else {
        return note("error");
    };
    let processed = processed.clone();
    let give_up = ctx.give_up(WAIT);
    let params =
        json!([keys, {"commitment": "confirmed", "encoding": "base64", "minContextSlot": s}]);
    let (c, confirmed) = loop {
        let reply = (ctx.cloudbreak)
            .call(&ctx.client, "getMultipleAccounts", params.clone())
            .await;
        if let (Some(c), Some(values)) = (reply.slot(), reply.values())
            && c >= s
            && values.len() == keys.len()
        {
            break (c, values.clone());
        }
        if ctx.past(give_up) {
            return note("confirmed_timeout");
        }
        tokio::time::sleep(POLL).await;
    };
    let canonical = ctx.canonical(s, WAIT).await;
    let mut verdicts = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let window = match canonical {
            Some(true) => ctx.write_between(key, s, c),
            _ => None,
        };
        let owner = window.clone().flatten().map(|(_, owner)| owner);
        let excluded = owner.as_deref().and_then(|o| ctx.owner_excluded(o));
        let mut verdict = decide(canonical, window, &processed[i], &confirmed[i], excluded);
        if verdict == Verdict::ExcludedUnknown {
            let owner = owner.unwrap_or_default();
            let check = ctx.settle(KeyCheck::ExcludedUnknown, key, &owner);
            verdict = check.await.into();
        }
        verdicts.push(verdict);
    }
    let mut t = tally.lock().expect("convergence lock");
    for (key, verdict) in keys.iter().zip(verdicts) {
        t.samples += 1;
        let uncertain = matches!(
            verdict,
            Verdict::Uncovered | Verdict::Unresolved | Verdict::ExcludedUnknown
        );
        t.cover(u64::from(!uncertain), 1);
        match verdict {
            Verdict::Pass => t.pass(),
            Verdict::Abandoned => {
                t.abandoned += 1;
                t.note("abandoned");
            }
            Verdict::Uncovered => t.note("uncovered"),
            Verdict::Unresolved => t.note("unresolved"),
            Verdict::Excluded => t.note("excluded"),
            Verdict::ExcludedUnknown => t.note("excluded_unknown"),
            Verdict::Mismatch(m) => t.fail("mismatch", format!("{key} S={s} C={c}: {m}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "11111111111111111111111111111111";

    fn account(lamports: u64) -> JsonValue {
        json!({"lamports": lamports, "owner": OWNER, "executable": false, "rentEpoch": 0,
            "space": 1, "data": ["AA==", "base64"]})
    }

    #[test]
    fn decision_without_write() {
        let (a, b) = (account(5), account(6));
        assert_eq!(decide(Some(true), Some(None), &a, &a, None), Verdict::Pass);
        assert!(matches!(
            decide(Some(true), Some(None), &a, &b, None),
            Verdict::Mismatch(_)
        ));
        assert!(matches!(
            decide(Some(true), Some(None), &a, &JsonValue::Null, None),
            Verdict::Mismatch(_)
        ));
        assert_eq!(decide(Some(true), None, &a, &b, None), Verdict::Uncovered);
        assert_eq!(
            decide(Some(false), Some(None), &a, &b, None),
            Verdict::Abandoned
        );
        assert_eq!(decide(None, Some(None), &a, &b, None), Verdict::Unresolved);
    }

    #[test]
    fn decision_with_write() {
        let (a, b, null) = (account(5), account(6), JsonValue::Null);
        let write = |lamports| Some(Some((lamports, OWNER.to_string())));
        assert_eq!(decide(Some(true), write(6), &a, &b, None), Verdict::Pass);
        assert!(matches!(
            decide(Some(true), write(7), &a, &b, None),
            Verdict::Mismatch(_)
        ));
        assert_eq!(decide(Some(true), write(0), &a, &null, None), Verdict::Pass);
        let unknown = decide(Some(true), write(6), &a, &null, None);
        assert_eq!(unknown, Verdict::ExcludedUnknown);
        let proven = decide(Some(true), write(6), &a, &null, Some(true));
        assert_eq!(proven, Verdict::Excluded);
        let wrong_null = decide(Some(true), write(6), &a, &null, Some(false));
        assert!(matches!(wrong_null, Verdict::Mismatch(_)));
        let served_excluded = decide(Some(true), write(6), &a, &b, Some(true));
        assert!(matches!(served_excluded, Verdict::Mismatch(_)));
    }
}
