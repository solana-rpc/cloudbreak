// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `compare-processed-accounts`: correctness and speed checks for processed commitment.
//!
//! Every call is read-only and each check is bounded by a sample or key count. Keys come from
//! `--pubkeys-file` and from writable keys of non-vote transactions in recent confirmed blocks of
//! the first reference, which is also the canonical chain oracle through `getBlocks(S, S)`.
//!
//! - Cross-source: the same processed getMultipleAccounts goes to cloudbreak and every reference
//!   at once. At equal context slots every value must match.
//! - Processed matches confirmed: a processed answer at slot S must equal any confirmed answer,
//!   from cloudbreak or the first reference, whose context slot is exactly S. Both are the state of
//!   bank S, so no write history is needed. A sample with no such answer is a fork or a miss.
//! - Token methods: getBalance, getTokenAccountBalance and getTokenSupply at processed must match
//!   the first reference at equal context slots. An error on one side only is a mismatch.
//!
//! A mismatch on a non-canonical slot is a fork, and one whose status stays unknown fails. A
//! cloudbreak null is an excluded key when a confirmed getAccountInfo on cloudbreak at S or later
//! answers -32010, or null while the reference serves the account. A token method -32010 is
//! excluded too. A check fails when it compares nothing, or when cloudbreak errors where the
//! reference answers.

use crate::benchmark::RequestType::{GetBalance, GetMultipleAccounts};
use crate::benchmark::percentile;
use crate::config::RpcEndpoint;
use crate::response_comparison::compare_responses;
use crate::utils::{get_slot, send_rpc_request};
use anyhow::{Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::Parser;
use rand::seq::SliceRandom;
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const TOKEN_PROGRAMS: [&str; 2] = [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
];
const VOTE_PROGRAM: &str = "Vote111111111111111111111111111111111111111";
/// Owner-excluded error of the Postgres read path.
const EXCLUDED_CODE: i64 = -32010;
const MAX_KEYS: usize = 2_000;
const MAX_FAILURES: usize = 40;
/// Tries per token method call to land both sources on one context slot.
const SLOT_TRIES: usize = 5;
const POLL: Duration = Duration::from_millis(100);

#[derive(Parser, Debug)]
#[command(name = "compare-processed-accounts")]
#[command(about = "\
Validate processed getMultipleAccounts, getBalance, getTokenAccountBalance and getTokenSupply \
against reference Agave RPCs at equal context slots and against confirmed answers at the same \
slot, and report latency per source. A mismatch on a slot that is not canonical is a fork.")]
pub struct Args {
    /// Cloudbreak RPC endpoint URL, pinned to one API instance
    #[arg(long, default_value = "http://10.43.10.2:26722")]
    pub rpc: String,
    /// Agave reference RPC URL, repeatable. The first one is the canonical chain oracle
    #[arg(long = "reference", required = true)]
    pub references: Vec<String>,
    /// File with one base58 pubkey per line
    #[arg(long)]
    pub pubkeys_file: Option<std::path::PathBuf>,
    /// Recent confirmed blocks on the first reference to take writable keys from
    #[arg(long, default_value_t = 4)]
    pub discover_blocks: usize,
    /// Cross-source samples
    #[arg(long, default_value_t = 100)]
    pub samples: usize,
    /// Processed matches confirmed samples
    #[arg(long, default_value_t = 20)]
    pub confirm_samples: usize,
    /// Keys per method in the token methods pass
    #[arg(long, default_value_t = 20)]
    pub smoke_keys: usize,
    #[arg(long, default_value_t = 20)]
    pub keys_per_sample: usize,
    /// Pause between samples in milliseconds
    #[arg(long, default_value_t = 200)]
    pub interval_ms: u64,
    /// HTTP timeout, and the wait for a slot to confirm, in seconds
    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

struct Ctx {
    client: reqwest::Client,
    cloudbreak: RpcEndpoint,
    references: Vec<RpcEndpoint>,
    wait: Duration,
}

/// One differing value, held until exclusion and canonical status are known.
struct Mismatch {
    slot: u64,
    key: String,
    detail: String,
    /// Cloudbreak answered null, so an excluded key may explain it.
    maybe_excluded: bool,
    /// The slot is known to be on the confirmed chain.
    settled: bool,
}

pub async fn run(args: &Args) -> Result<()> {
    let endpoint = |name: String, url: &String| RpcEndpoint {
        url: url.clone(),
        name,
    };
    let ctx = Ctx {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(args.timeout))
            .build()?,
        cloudbreak: endpoint("cloudbreak".to_string(), &args.rpc),
        references: (args.references.iter().enumerate())
            .map(|(i, url)| endpoint(format!("reference-{i}"), url))
            .collect(),
        wait: Duration::from_secs(args.timeout),
    };
    let keys = load_keys(&ctx, args).await?;
    if keys.is_empty() {
        return Err(anyhow!("no keys, set --pubkeys-file or --discover-blocks"));
    }
    println!("{} keys, {} reference(s)", keys.len(), ctx.references.len());

    let (mut mismatches, mut failures) = (Vec::new(), Vec::new());
    let tokens = cross_source(&ctx, args, &keys, &mut mismatches, &mut failures).await;
    processed_matches_confirmed(&ctx, args, &keys, &mut mismatches, &mut failures).await;
    token_methods(&ctx, args, &keys, &tokens, &mut mismatches, &mut failures).await;

    let (mut excluded, mut forks, mut unclassified) = (0, 0, 0);
    let (mut slots, mut probes) = (HashMap::new(), HashMap::new());
    for m in &mismatches {
        if failures.len() >= MAX_FAILURES {
            unclassified += 1;
            continue;
        }
        let status = match (m.settled, slots.get(&m.slot).copied()) {
            (true, _) => Some(true),
            (false, Some(status)) => status,
            (false, None) => *slots.entry(m.slot).or_insert(canonical(&ctx, m.slot).await),
        };
        if status == Some(false) {
            forks += 1;
            continue;
        }
        let is_excluded = match (m.maybe_excluded, probes.get(&m.key).copied()) {
            (false, _) => false,
            (true, Some(known)) => known,
            (true, None) => *probes
                .entry(&m.key)
                .or_insert(probe_excluded(&ctx, &m.key, m.slot).await),
        };
        if is_excluded {
            excluded += 1;
            continue;
        }
        let note = status.map_or(" (canonical unknown)", |_| "");
        failures.push(format!("{} at slot {}: {}{note}", m.key, m.slot, m.detail));
    }
    println!("mismatches: {excluded} excluded keys, {forks} on forks, {unclassified} unclassified");

    if failures.is_empty() {
        println!("PASS: processed reads consistent with references and confirmed data");
        Ok(())
    } else {
        for f in &failures {
            println!("  FAIL {f}");
        }
        Err(anyhow!("{} check(s) failed", failures.len() + unclassified))
    }
}

/// Check 1. Returns token accounts and mints seen in cloudbreak's answers.
async fn cross_source(
    ctx: &Ctx,
    args: &Args,
    keys: &[String],
    mismatches: &mut Vec<Mismatch>,
    failures: &mut Vec<String>,
) -> (Vec<String>, Vec<String>) {
    let sources: Vec<&RpcEndpoint> = std::iter::once(&ctx.cloudbreak)
        .chain(&ctx.references)
        .collect();
    let (mut compared, mut cb_errors) = (0, 0);
    let mut latencies = vec![Vec::new(); sources.len()];
    let mut highest = vec![0usize; sources.len()];
    let (mut accounts, mut mints) = (HashSet::new(), HashSet::new());
    for _ in 0..args.samples {
        let sample = sample_keys(keys, args.keys_per_sample);
        let params = json!([sample, {"commitment": "processed", "encoding": "base64"}]);
        let calls = (sources.iter()).map(|s| call(ctx, s, "getMultipleAccounts", params.clone()));
        let replies: Vec<Option<(JsonValue, u128)>> = futures::future::join_all(calls)
            .await
            .into_iter()
            .map(|r| r.ok().filter(|(json, _)| get_slot(json).is_some()))
            .collect();
        let top = replies.iter().flatten().map(|r| get_slot(&r.0)).max();
        for (i, reply) in replies.iter().enumerate() {
            if let Some((json, ms)) = reply {
                latencies[i].push(*ms);
                highest[i] += usize::from(Some(get_slot(json)) == top);
            }
        }
        if replies[0].is_none() && replies[1..].iter().any(Option::is_some) {
            cb_errors += 1;
        }
        if let Some((cb, _)) = &replies[0]
            && let Some(slot) = get_slot(cb)
        {
            let values = cb["result"]["value"].as_array().into_iter().flatten();
            for (key, value) in sample.iter().zip(values) {
                let owner = value["owner"].as_str().unwrap_or_default();
                let data = BASE64.decode(value["data"][0].as_str().unwrap_or_default());
                let data = data.unwrap_or_default();
                // A mint is 82 bytes, a token account 165, and a longer one has a type byte at 165.
                match (TOKEN_PROGRAMS.contains(&owner), data.len(), data.get(165)) {
                    (true, 82, _) | (true, 166.., Some(1)) => mints.insert(key.clone()),
                    (true, 165, _) | (true, 166.., Some(2)) => accounts.insert(key.clone()),
                    _ => false,
                };
            }
            for (i, reply) in replies.iter().enumerate().skip(1) {
                if let Some((reference, _)) = reply
                    && get_slot(reference) == Some(slot)
                {
                    compared += 1;
                    let found =
                        differing_keys(&sample, slot, cb, reference, &sources[i].name, false);
                    mismatches.extend(found);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
    }

    println!("cross-source: {} samples", args.samples);
    println!("  source             ok  p50ms  p90ms  p99ms  highest");
    for (i, source) in sources.iter().enumerate() {
        latencies[i].sort_unstable();
        let [p50, p90, p99] = [50.0, 90.0, 99.0].map(|p| percentile(&latencies[i], p));
        let (name, ok) = (&source.name, latencies[i].len());
        let share = 100.0 * highest[i] as f64 / args.samples.max(1) as f64;
        println!("  {name:<14} {ok:>6} {p50:>6} {p90:>6} {p99:>6} {share:>7.1}%");
    }
    let n = args.samples;
    if compared == 0 {
        failures.push(format!("cross-source: no equal-slot pair in {n} samples"));
    }
    if cb_errors > 0 {
        failures.push(format!(
            "cross-source: cloudbreak errored on {cb_errors} samples"
        ));
    }
    (accounts.into_iter().collect(), mints.into_iter().collect())
}

/// Check 2. A processed answer at S against confirmed answers at exactly S.
async fn processed_matches_confirmed(
    ctx: &Ctx,
    args: &Args,
    keys: &[String],
    mismatches: &mut Vec<Mismatch>,
    failures: &mut Vec<String>,
) {
    let oracles = [&ctx.cloudbreak, &ctx.references[0]];
    let mut classes: HashMap<&str, usize> = HashMap::new();
    for _ in 0..args.confirm_samples {
        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
        let sample = sample_keys(keys, args.keys_per_sample);
        let params = json!([sample, {"commitment": "processed", "encoding": "base64"}]);
        let reply = call(ctx, &ctx.cloudbreak, "getMultipleAccounts", params).await;
        let Some((s, processed)) = reply.ok().and_then(|(j, _)| Some((get_slot(&j)?, j))) else {
            *classes.entry("error").or_default() += 1;
            continue;
        };
        let config = json!({"commitment": "confirmed", "encoding": "base64", "minContextSlot": s});
        let params = json!([sample, config]);
        let (deadline, mut passed, mut hits) = (Instant::now() + ctx.wait, [false; 2], 0);
        while passed.contains(&false) && Instant::now() < deadline {
            for (i, oracle) in oracles.iter().enumerate() {
                if passed[i] {
                    continue;
                }
                let reply = call(ctx, oracle, "getMultipleAccounts", params.clone()).await;
                // Below S the node answers a minContextSlot error with no slot.
                let Some((c, confirmed)) = reply.ok().and_then(|(j, _)| Some((get_slot(&j)?, j)))
                else {
                    continue;
                };
                passed[i] = true;
                if c == s {
                    hits += 1;
                    let against = format!("{} confirmed", oracle.name);
                    let found = differing_keys(&sample, s, &processed, &confirmed, &against, true);
                    mismatches.extend(found);
                }
            }
            tokio::time::sleep(POLL).await;
        }
        let status = if hits == 0 {
            canonical(ctx, s).await
        } else {
            None
        };
        *classes.entry(sample_class(hits, status)).or_default() += 1;
    }
    println!("processed-confirmed: {classes:?}");
    if !classes.contains_key("compared") {
        let n = args.confirm_samples;
        failures.push(format!(
            "processed-confirmed: no exact-S answer in {n} samples"
        ));
    }
}

/// Check 3. Single-key methods at processed against the first reference.
async fn token_methods(
    ctx: &Ctx,
    args: &Args,
    keys: &[String],
    (accounts, mints): &(Vec<String>, Vec<String>),
    mismatches: &mut Vec<Mismatch>,
    failures: &mut Vec<String>,
) {
    let methods = [
        ("getBalance", keys),
        ("getTokenAccountBalance", &accounts[..]),
        ("getTokenSupply", &mints[..]),
    ];
    let reference = &ctx.references[0];
    for (method, keys) in methods {
        let keys = &keys[..keys.len().min(args.smoke_keys)];
        let (mut equal, mut excluded, mut cb_errors) = (0, 0, 0);
        for key in keys {
            let params = json!([key, {"commitment": "processed"}]);
            for _ in 0..SLOT_TRIES {
                let (cb, rf) = tokio::join!(
                    call(ctx, &ctx.cloudbreak, method, params.clone()),
                    call(ctx, reference, method, params.clone())
                );
                cb_errors += usize::from(cb.is_err() && rf.is_ok());
                let (Ok((cb, _)), Ok((rf, _))) = (cb, rf) else {
                    continue;
                };
                if cb["error"]["code"] == EXCLUDED_CODE {
                    excluded += 1;
                    break;
                }
                let slot = match (get_slot(&cb), get_slot(&rf)) {
                    (Some(a), Some(b)) if a == b => a,
                    (Some(_), Some(_)) => continue,
                    (None, None) => break,
                    // One side errored, so the pair is a mismatch at the slot the other side names.
                    (Some(slot), None) | (None, Some(slot)) => slot,
                };
                equal += 1;
                // All three methods compare result.value directly, so one request type fits.
                if !compare_responses(&cb, &rf, "none", GetBalance).matches {
                    let shown =
                        |r: &JsonValue| r.get("error").unwrap_or(&r["result"]["value"]).to_string();
                    let (c, r, name) = (shown(&cb), shown(&rf), &reference.name);
                    mismatches.push(Mismatch {
                        slot,
                        key: format!("{method} {key}"),
                        detail: format!("cloudbreak {c}, {name} {r}"),
                        maybe_excluded: false,
                        settled: false,
                    });
                }
                break;
            }
        }
        let n = keys.len();
        println!("{method}: {n} keys, {equal} compared at equal slots, {excluded} excluded");
        if equal == 0 {
            failures.push(format!("{method}: nothing compared for {n} keys"));
        }
        if cb_errors > 0 {
            failures.push(format!("{method}: cloudbreak errored on {cb_errors} calls"));
        }
    }
}

/// Keys whose values differ between two getMultipleAccounts answers at the same slot.
fn differing_keys(
    keys: &[String],
    slot: u64,
    cloudbreak: &JsonValue,
    other: &JsonValue,
    against: &str,
    settled: bool,
) -> Vec<Mismatch> {
    let mismatch = |key: &str, detail: String, maybe_excluded| Mismatch {
        slot,
        key: key.to_string(),
        detail,
        maybe_excluded,
        settled,
    };
    if compare_responses(cloudbreak, other, "base64", GetMultipleAccounts).matches {
        return Vec::new();
    }
    let (cb, other) = (&cloudbreak["result"]["value"], &other["result"]["value"]);
    match (cb.as_array(), other.as_array()) {
        (Some(a), Some(b)) if a.len() == keys.len() && b.len() == keys.len() => (keys.iter())
            .zip(a.iter().zip(b))
            .filter(|(_, (a, b))| a != b)
            .map(|(key, (a, b))| mismatch(key, format!("{a} against {against} {b}"), a.is_null()))
            .collect(),
        _ => vec![mismatch(
            "response",
            format!("shape differs from {against}"),
            false,
        )],
    }
}

/// Class of a processed matches confirmed sample.
fn sample_class(hits: usize, canonical: Option<bool>) -> &'static str {
    match (hits, canonical) {
        (1.., _) => "compared",
        (0, Some(false)) => "fork",
        (0, Some(true)) => "miss",
        (0, None) => "unresolved",
    }
}

/// True when cloudbreak's confirmed getAccountInfo at `slot` or later answers -32010, or answers
/// null while the reference serves the account, so the node does not index the key.
async fn probe_excluded(ctx: &Ctx, key: &str, slot: u64) -> bool {
    let config = json!({"commitment": "confirmed", "encoding": "base64", "minContextSlot": slot,
        "dataSlice": {"offset": 0, "length": 0}});
    let params = json!([key, config]);
    let deadline = Instant::now() + ctx.wait;
    loop {
        let (cb, rf) = tokio::join!(
            call(ctx, &ctx.cloudbreak, "getAccountInfo", params.clone()),
            call(ctx, &ctx.references[0], "getAccountInfo", params.clone())
        );
        let (cb, rf) = (cb.unwrap_or_default().0, rf.unwrap_or_default().0);
        if cb["error"]["code"] == EXCLUDED_CODE {
            return true;
        }
        // Both answer with a slot once their confirmed slot reaches `slot`.
        if get_slot(&cb).is_some() && get_slot(&rf).is_some() {
            return cb["result"]["value"].is_null() && rf["result"]["value"].is_object();
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Canonical status of `slot` from the first reference once its confirmed slot reaches it.
async fn canonical(ctx: &Ctx, slot: u64) -> Option<bool> {
    let oracle = &ctx.references[0];
    let deadline = Instant::now() + ctx.wait;
    loop {
        let tip = call(ctx, oracle, "getSlot", json!([{"commitment": "confirmed"}])).await;
        if tip.ok().and_then(|(json, _)| json["result"].as_u64()) >= Some(slot) {
            let params = json!([slot, slot, {"commitment": "confirmed"}]);
            let (blocks, _) = call(ctx, oracle, "getBlocks", params).await.ok()?;
            return Some(blocks["result"].as_array()?.contains(&json!(slot)));
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Keys from `--pubkeys-file`, then writable keys of non-vote transactions in recent blocks.
async fn load_keys(ctx: &Ctx, args: &Args) -> Result<Vec<String>> {
    let mut keys: Vec<String> = match &args.pubkeys_file {
        Some(path) => std::fs::read_to_string(path)?
            .lines()
            .filter_map(|line| line.split('#').next().map(str::trim))
            .filter(|key| !key.is_empty())
            .map(str::to_string)
            .collect(),
        None => Vec::new(),
    };
    let oracle = &ctx.references[0];
    let (tip, _) = call(ctx, oracle, "getSlot", json!([{"commitment": "confirmed"}])).await?;
    let mut slot = tip["result"].as_u64().unwrap_or_default();
    let mut found = 0;
    // Walks back past skipped slots and blocks not yet available.
    for _ in 0..args.discover_blocks * 4 {
        if found == args.discover_blocks || slot == 0 {
            break;
        }
        let config = json!({"commitment": "confirmed", "maxSupportedTransactionVersion": 0,
            "transactionDetails": "accounts", "rewards": false});
        let (block, _) = call(ctx, oracle, "getBlock", json!([slot, config])).await?;
        slot -= 1;
        let Some(txs) = block["result"]["transactions"].as_array() else {
            continue;
        };
        found += 1;
        let tx_keys = txs
            .iter()
            .filter_map(|tx| tx["transaction"]["accountKeys"].as_array());
        let non_vote = tx_keys.filter(|ks| !ks.iter().any(|k| k["pubkey"] == VOTE_PROGRAM));
        let writable = non_vote.flatten().filter(|k| k["writable"] == true);
        keys.extend(writable.filter_map(|k| k["pubkey"].as_str().map(str::to_string)));
    }
    let mut seen = HashSet::new();
    keys.retain(|k| seen.insert(k.clone()));
    keys.truncate(MAX_KEYS);
    Ok(keys)
}

fn sample_keys(keys: &[String], n: usize) -> Vec<String> {
    let picks = keys.choose_multiple(&mut rand::thread_rng(), n);
    picks.cloned().collect()
}

async fn call(
    ctx: &Ctx,
    endpoint: &RpcEndpoint,
    method: &str,
    params: JsonValue,
) -> Result<(JsonValue, u128)> {
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    send_rpc_request(&ctx.client, endpoint, &request, None).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gma(slot: u64, values: JsonValue) -> JsonValue {
        json!({"result": {"context": {"slot": slot}, "value": values}})
    }

    #[test]
    fn equal_slot_comparison_flags_each_differing_key() {
        let account = |lamports: u64| json!({"lamports": lamports, "owner": VOTE_PROGRAM});
        let keys = ["a", "b", "c", "d"].map(str::to_string);
        let cb = gma(7, json!([account(1), null, account(2), account(3)]));
        let other = gma(7, json!([account(1), account(5), null, account(4)]));
        assert!(differing_keys(&keys, 7, &cb, &cb, "ref", false).is_empty());

        let found = differing_keys(&keys, 7, &cb, &other, "ref", true);
        let rows = found.iter().map(|m| (m.key.as_str(), m.maybe_excluded));
        assert!(rows.eq([("b", true), ("c", false), ("d", false)]));
        assert!(found.iter().all(|m| m.slot == 7 && m.settled));

        let short = differing_keys(&keys[..1], 7, &cb, &other, "ref", false);
        assert_eq!((short.len(), short[0].key.as_str()), (1, "response"));
    }

    #[test]
    fn confirmed_samples_split_into_fork_and_miss() {
        assert_eq!(sample_class(1, None), "compared");
        assert_eq!(sample_class(2, Some(false)), "compared");
        assert_eq!(sample_class(0, Some(false)), "fork");
        assert_eq!(sample_class(0, Some(true)), "miss");
        assert_eq!(sample_class(0, None), "unresolved");
    }
}
