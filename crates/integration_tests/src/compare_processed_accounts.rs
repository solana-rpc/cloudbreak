// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Validates processed account reads against Postgres on the same node. Each processed response
//! names a slot S, and once S is confirmed the newest row per key at slot <= S must match it.
//! Postgres reads use primary keys or the (pubkey, slot DESC) index only, never a table scan.

use crate::config::RpcEndpoint;
use crate::utils::{get_slot, send_rpc_request};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::Parser;
use rand::seq::SliceRandom;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, IsolationLevel};
use sea_orm::{QueryResult, Statement, TransactionTrait};
use serde_json::{Value as JsonValue, json};
use solana_pubkey::Pubkey;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, Instant};

/// Commitment ids in the slots table.
const CONFIRMED: i32 = 1;
const FINALIZED: i32 = 2;
const EXCLUDED_CODE: i64 = -32010;
const VOTE_PROGRAM: &str = "Vote111111111111111111111111111111111111111";
const LIVE_LOOKUPS_METRIC: &str = "cloudbreak_api_processed_lookups_total";
const MAX_POOL: usize = 20_000;
/// getBlock budget: blocks collected, and slots walked back past skipped ones.
const MAX_BLOCKS: usize = 16;
const MAX_SLOT_WALK: u64 = 64;

#[derive(Parser, Debug)]
#[command(name = "compare-processed-accounts")]
#[command(about = "\
Validate processed getMultipleAccounts and getBalance against Postgres on the same node. Once a \
response slot S is confirmed, the newest row per key at slot <= S, read in one REPEATABLE READ \
transaction, must match it exactly. Keys come from --pubkeys-file or from writable accounts of \
recent confirmed blocks on --cluster-rpc. Slots at or below finalized are expired, and slots with \
no blockhash row once confirmed passes them are abandoned.")]
pub struct Args {
    /// File with one base58 pubkey per line, blank lines and # comments ignored
    #[arg(long)]
    pub pubkeys_file: Option<std::path::PathBuf>,

    /// Agave RPC whose recent confirmed blocks supply writable keys, used without --pubkeys-file
    #[arg(long)]
    pub cluster_rpc: Option<String>,

    /// Cloudbreak RPC endpoint URL, pinned to one API instance
    #[arg(long, default_value = "http://10.43.10.2:26722")]
    pub rpc: String,

    #[arg(long, default_value = "cloudbreak")]
    pub rpc_name: String,

    /// Postgres URL of the database the API reads
    #[arg(long)]
    pub db_url: String,

    /// Prometheus metrics URL of the same API instance
    #[arg(long)]
    pub metrics_url: Option<String>,

    #[arg(long, default_value_t = 200)]
    pub samples: usize,

    #[arg(long, default_value_t = 20)]
    pub keys_per_sample: usize,

    /// Max percentage of reads on abandoned or never confirmed slots
    #[arg(long, default_value_t = 5.0)]
    pub max_abandoned_pct: f64,

    /// Min percentage of reads whose slot is above the confirmed slot
    #[arg(long, default_value_t = 50.0)]
    pub min_fresh_pct: f64,

    /// HTTP timeout, and the per-sample wait for confirmed to reach S, in seconds
    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

/// Expected base64 account value per key at slot S, None for a missing or zero-lamport row.
type ExpectedMap = HashMap<String, Option<JsonValue>>;

/// Include mode, then the program list, from environment_info.
type OwnerFilter = (bool, HashSet<String>);

/// Ok(true) when an excluded owner is correctly withheld, Err on a mismatch.
type Verdict = Result<bool, String>;

enum SlotState {
    Expired,
    Abandoned,
    Present(ExpectedMap),
}

#[derive(Default)]
struct Tally {
    reads: usize,
    slotted: usize,
    fresh: usize,
    verified: usize,
    abandoned: usize,
    expired: usize,
    excluded: usize,
    depth: BTreeMap<u64, usize>,
    mismatches: Vec<String>,
}

impl Tally {
    fn observe(&mut self, slot: u64, confirmed_before: u64) {
        self.slotted += 1;
        if slot < confirmed_before {
            let m = format!("slot {slot} below confirmed {confirmed_before}");
            self.mismatches.push(m);
        } else if slot > confirmed_before {
            self.fresh += 1;
        }
        let depth = slot.saturating_sub(confirmed_before);
        *self.depth.entry(depth).or_default() += 1;
    }

    fn record(&mut self, method: &str, slot: u64, key: &str, verdict: Verdict) {
        match verdict {
            Ok(excluded) => self.excluded += usize::from(excluded),
            Err(m) => {
                let line = format!("{method} at slot {slot}: {key}: {m}");
                self.mismatches.push(line);
            }
        }
    }
}

pub async fn run(args: &Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;
    let rpc = RpcEndpoint {
        url: args.rpc.clone(),
        name: args.rpc_name.clone(),
    };
    let db = Database::connect(&args.db_url)
        .await
        .context("Failed to connect to Postgres")?;
    let filter = load_owner_filter(&db).await;
    if filter.is_none() {
        println!("warning: no program filter in environment_info, a -32010 is a mismatch");
    }
    let pool = key_pool(&client, args).await?;
    println!("{:<12} pool of {} keys", args.rpc_name, pool.len());
    let before = live_lookups(&client, args.metrics_url.as_deref()).await?;

    let mut tally = Tally::default();
    for _ in 0..args.samples {
        let keys: Vec<String> = pool
            .choose_multiple(&mut rand::thread_rng(), args.keys_per_sample)
            .cloned()
            .collect();
        let confirmed_before = confirmed_slot(&client, &rpc).await?;
        let params = json!([keys, {"commitment": "processed", "encoding": "base64"}]);
        let multiple = call(&client, &rpc, "getMultipleAccounts", params).await?;
        let mut reads = vec![(None, multiple)];
        for (i, key) in keys.iter().enumerate() {
            let params = json!([key, {"commitment": "processed"}]);
            reads.push((Some(i), call(&client, &rpc, "getBalance", params).await?));
        }
        let deadline = Instant::now() + Duration::from_secs(args.timeout);
        let sample = Sample {
            keys: &keys,
            confirmed_before,
            deadline,
            filter: filter.as_ref(),
        };
        verify_sample(&db, &sample, reads, &mut tally).await?;
    }
    let lookups = before.zip(live_lookups(&client, args.metrics_url.as_deref()).await?);

    println!(
        "reads {}, verified {}, fresh {:.1}%, abandoned {} ({:.1}%), expired {}, excluded {}",
        tally.reads,
        tally.verified,
        pct(tally.fresh, tally.slotted),
        tally.abandoned,
        pct(tally.abandoned, tally.reads),
        tally.expired,
        tally.excluded
    );
    let depth = tally.depth.iter().map(|(d, n)| format!("{d}:{n}"));
    println!(
        "depth (S minus confirmed): {}",
        depth.collect::<Vec<_>>().join(" ")
    );
    if let Some((before, after)) = lookups {
        println!("live lookups: {before} -> {after}");
    }
    for m in tally.mismatches.iter().take(40) {
        println!("  MISMATCH {m}");
    }

    let failures = exit_failures(&tally, args.max_abandoned_pct, args.min_fresh_pct, lookups);
    if failures.is_empty() {
        println!("PASS: processed reads match Postgres at their slot");
        return Ok(());
    }
    for f in &failures {
        println!("  FAIL {f}");
    }
    Err(anyhow!("{} check(s) failed", failures.len()))
}

fn pct(n: usize, d: usize) -> f64 {
    n as f64 * 100.0 / d.max(1) as f64
}

fn exit_failures(
    t: &Tally,
    max_ab: f64,
    min_fresh: f64,
    lookups: Option<(f64, f64)>,
) -> Vec<String> {
    let mut failures = Vec::new();
    if !t.mismatches.is_empty() {
        failures.push(format!("{} same-slot mismatch(es)", t.mismatches.len()));
    }
    let abandoned = pct(t.abandoned, t.reads);
    if abandoned > max_ab {
        failures.push(format!("abandoned {abandoned:.1}% above {max_ab}%"));
    }
    let fresh = pct(t.fresh, t.slotted);
    if fresh < min_fresh {
        failures.push(format!("fresh {fresh:.1}% below {min_fresh}%"));
    }
    if let Some((before, after)) = lookups.filter(|(before, after)| after <= before) {
        failures.push(format!("live lookups did not grow ({before} -> {after})"));
    }
    failures
}

async fn load_owner_filter(db: &DatabaseConnection) -> Option<OwnerFilter> {
    let sql = "SELECT mode = 'include', programs FROM environment_info WHERE id = 1";
    let row = query(db, sql.to_string()).await.ok()?.pop()?;
    let include: bool = row.try_get_by_index(0).ok()?;
    let programs: String = row.try_get_by_index(1).ok()?;
    let programs = programs.split(',').map(str::trim).filter(|p| !p.is_empty());
    Some((include, programs.map(str::to_string).collect()))
}

/// Keys from --pubkeys-file, else from writable accounts of recent confirmed cluster blocks.
async fn key_pool(client: &reqwest::Client, args: &Args) -> Result<Vec<String>> {
    let want = (args.samples * args.keys_per_sample).clamp(1, MAX_POOL);
    let mut pool = match (&args.pubkeys_file, &args.cluster_rpc) {
        (Some(path), _) => parse_pubkeys(&std::fs::read_to_string(path)?)?,
        (None, Some(url)) => cluster_block_keys(client, url, want).await?,
        (None, None) => return Err(anyhow!("--pubkeys-file or --cluster-rpc is required")),
    };
    let mut seen = HashSet::new();
    pool.retain(|k| seen.insert(k.clone()));
    if pool.is_empty() {
        return Err(anyhow!("the key source returned no pubkeys"));
    }
    pool.shuffle(&mut rand::thread_rng());
    pool.truncate(want);
    Ok(pool)
}

/// One base58 pubkey per line. Blank lines and # comments are ignored.
fn parse_pubkeys(text: &str) -> Result<Vec<String>> {
    text.lines()
        .map(|line| line.split('#').next().unwrap_or_default().trim())
        .filter(|key| !key.is_empty())
        .map(|key| {
            Ok(Pubkey::from_str(key)
                .context(format!("invalid pubkey {key}"))?
                .to_string())
        })
        .collect()
}

/// Walks back from the confirmed slot, skipping slots with no block, until enough keys or blocks.
async fn cluster_block_keys(
    client: &reqwest::Client,
    url: &str,
    want: usize,
) -> Result<Vec<String>> {
    let cluster = RpcEndpoint {
        url: url.to_string(),
        name: "cluster".to_string(),
    };
    let top = confirmed_slot(client, &cluster).await?;
    let options = json!({"encoding": "json", "transactionDetails": "accounts", "rewards": false,
        "maxSupportedTransactionVersion": 0, "commitment": "confirmed"});
    let (mut keys, mut blocks) = (Vec::new(), 0);
    for slot in (top.saturating_sub(MAX_SLOT_WALK)..=top).rev() {
        if blocks >= MAX_BLOCKS || keys.len() >= want {
            break;
        }
        let block = call(client, &cluster, "getBlock", json!([slot, options])).await?;
        if !block["result"].is_null() {
            blocks += 1;
            keys.extend(writable_block_keys(&block));
        }
    }
    println!("cluster: {blocks} blocks at or below slot {top}");
    Ok(keys)
}

/// Writable account keys of non-vote transactions in a getBlock response.
fn writable_block_keys(block: &JsonValue) -> Vec<String> {
    let txs = block["result"]["transactions"].as_array();
    txs.into_iter()
        .flatten()
        .filter_map(|tx| tx["transaction"]["accountKeys"].as_array())
        .filter(|keys| !keys.iter().any(|k| k["pubkey"] == VOTE_PROGRAM))
        .flatten()
        .filter(|k| k["writable"] == true)
        .filter_map(|k| Some(k["pubkey"].as_str()?.to_string()))
        .collect()
}

struct Sample<'a> {
    keys: &'a [String],
    confirmed_before: u64,
    deadline: Instant,
    filter: Option<&'a OwnerFilter>,
}

/// Reads are (getBalance key index, response), the first one getMultipleAccounts over all keys.
/// An error response has no slot, so it is checked at the getMultipleAccounts slot.
async fn verify_sample(
    db: &DatabaseConnection,
    sample: &Sample<'_>,
    reads: Vec<(Option<usize>, JsonValue)>,
    tally: &mut Tally,
) -> Result<()> {
    let keys = sample.keys;
    let anchor = get_slot(&reads[0].1)
        .ok_or_else(|| anyhow!("getMultipleAccounts has no context slot: {}", reads[0].1))?;
    for (key, response) in reads {
        tally.reads += 1;
        let slot = get_slot(&response).inspect(|&s| tally.observe(s, sample.confirmed_before));
        let slot = slot.unwrap_or(anchor);
        let expected = match settle(db, slot, keys, sample.deadline).await? {
            SlotState::Expired => {
                tally.expired += 1;
                continue;
            }
            SlotState::Abandoned => {
                tally.abandoned += 1;
                continue;
            }
            SlotState::Present(expected) => expected,
        };
        tally.verified += 1;
        let want = |k: &str| expected.get(k).and_then(Option::as_ref);
        let excluded = |w: Option<&JsonValue>| w.is_some_and(|w| excluded(sample.filter, w));
        match (key, response["result"]["value"].as_array()) {
            (Some(i), _) => {
                let (k, w) = (&keys[i], want(&keys[i]));
                let verdict = check_balance(&response, w, excluded(w));
                tally.record("getBalance", slot, k, verdict);
            }
            (None, Some(values)) if values.len() == keys.len() => {
                for (k, value) in keys.iter().zip(values) {
                    let verdict = check_account(value, want(k), excluded(want(k)));
                    tally.record("getMultipleAccounts", slot, k, verdict);
                }
            }
            (None, _) => {
                let verdict = Err(format!("no value per key in {response}"));
                tally.record("getMultipleAccounts", slot, "sample", verdict);
            }
        }
    }
    Ok(())
}

/// Waits for confirmed to reach `slot`, then reads the expected state. A timeout counts as abandoned.
async fn settle(
    db: &DatabaseConnection,
    slot: u64,
    keys: &[String],
    deadline: Instant,
) -> Result<SlotState> {
    loop {
        if commitment_slot(db, CONFIRMED).await? >= slot {
            let isolation = Some(IsolationLevel::RepeatableRead);
            let txn = db.begin_with_config(isolation, None).await?;
            let state = read_expected(&txn, slot, keys).await;
            txn.rollback().await?;
            if let Some(state) = state? {
                return Ok(state);
            }
        }
        if Instant::now() >= deadline {
            return Ok(SlotState::Abandoned);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// None while confirmed equals S and its blockhash row is not written yet.
async fn read_expected<C: ConnectionTrait>(
    conn: &C,
    slot: u64,
    keys: &[String],
) -> Result<Option<SlotState>> {
    if commitment_slot(conn, FINALIZED).await? >= slot {
        return Ok(Some(SlotState::Expired));
    }
    let sql = format!("SELECT 1 FROM recent_blockhashes WHERE slot = {slot}");
    if query(conn, sql).await?.is_empty() {
        let past = commitment_slot(conn, CONFIRMED).await? > slot;
        return Ok(past.then_some(SlotState::Abandoned));
    }
    let mut literals = Vec::new();
    for key in keys {
        let bytes = bs58::decode(key).into_vec()?;
        literals.push(format!("'\\x{}'::bytea", hex::encode(bytes)));
    }
    // Same newest-row lateral read as crates/api/src/db/getMultipleAccounts.sql.
    let sql = format!(
        "SELECT k.pubkey, u.owner, u.lamports, u.executable, u.rent_epoch::text, u.data \
         FROM unnest(ARRAY[{}]) AS k (pubkey) JOIN LATERAL (SELECT * FROM ( \
           (SELECT owner, lamports, slot, executable, rent_epoch, data FROM accounts \
            WHERE pubkey = k.pubkey AND slot <= {slot} ORDER BY slot DESC LIMIT 1) \
           UNION ALL \
           (SELECT owner, lamports, slot, executable, rent_epoch, data FROM snapshot_accounts \
            WHERE pubkey = k.pubkey AND slot <= {slot} ORDER BY slot DESC LIMIT 1) \
         ) AS newest ORDER BY slot DESC LIMIT 1) AS u ON TRUE",
        literals.join(", ")
    );
    let mut out: ExpectedMap = keys.iter().map(|k| (k.clone(), None)).collect();
    for row in query(conn, sql).await? {
        let pubkey: Vec<u8> = row.try_get_by_index(0)?;
        let owner: Vec<u8> = row.try_get_by_index(1)?;
        let lamports: i64 = row.try_get_by_index(2)?;
        let executable: bool = row.try_get_by_index(3)?;
        let rent_epoch: String = row.try_get_by_index(4)?;
        let data: Vec<u8> = row.try_get_by_index(5)?;
        let account = json!({
            "lamports": lamports as u64, "owner": bs58::encode(owner).into_string(),
            "executable": executable, "rentEpoch": rent_epoch.parse::<u64>()?,
            "space": data.len(), "data": [BASE64.encode(&data), "base64"],
        });
        let pubkey = bs58::encode(pubkey).into_string();
        out.insert(pubkey, (lamports > 0).then_some(account));
    }
    Ok(Some(SlotState::Present(out)))
}

async fn query<C: ConnectionTrait>(conn: &C, sql: String) -> Result<Vec<QueryResult>> {
    let statement = Statement::from_string(DbBackend::Postgres, sql.clone());
    let rows = conn.query_all(statement).await;
    rows.with_context(|| format!("Failed to run {sql}"))
}

async fn commitment_slot<C: ConnectionTrait>(conn: &C, commitment: i32) -> Result<u64> {
    let sql = format!("SELECT COALESCE(MAX(slot), 0) FROM slots WHERE commitment = {commitment}");
    let rows = query(conn, sql).await?;
    Ok(rows[0].try_get_by_index::<i64>(0)? as u64)
}

/// Exact compare of one base64 account value. An excluded owner must read as null.
fn check_account(value: &JsonValue, want: Option<&JsonValue>, excluded: bool) -> Verdict {
    match want {
        None if value.is_null() => Ok(false),
        None => Err("account returned, want null".to_string()),
        Some(_) if excluded && value.is_null() => Ok(true),
        Some(w) if excluded => Err(format!("served excluded owner {}", w["owner"])),
        Some(w) if value == w => Ok(false),
        Some(w) => {
            let fields = w.as_object().into_iter().flatten();
            let diff = fields.filter(|(f, v)| value.get(f.as_str()) != Some(v));
            let diff: Vec<&str> = diff.map(|(f, _)| f.as_str()).collect();
            Err(format!("fields differ: {}", diff.join(", ")))
        }
    }
}

/// getBalance must equal the expected lamports, or answer -32010 for an excluded owner.
fn check_balance(response: &JsonValue, want: Option<&JsonValue>, excluded: bool) -> Verdict {
    let lamports = want.map_or(json!(0), |w| w["lamports"].clone());
    match (response.get("error"), &response["result"]["value"]) {
        (Some(err), _) if excluded && err["code"] == EXCLUDED_CODE => Ok(true),
        (Some(err), _) => Err(format!("unexpected error {err}")),
        (None, _) if excluded => Err("served, want -32010".to_string()),
        (None, got) if *got == lamports => Ok(false),
        (None, got) => Err(format!("balance {got}, want {lamports}")),
    }
}

fn excluded(filter: Option<&OwnerFilter>, account: &JsonValue) -> bool {
    let owner = account["owner"].as_str().unwrap_or_default();
    filter.is_some_and(|(include, programs)| programs.contains(owner) != *include)
}

/// Sums every live-source series of the processed lookup counter, None without a metrics URL.
async fn live_lookups(client: &reqwest::Client, url: Option<&str>) -> Result<Option<f64>> {
    let Some(url) = url else { return Ok(None) };
    let body = client.get(url).send().await?.text().await?;
    Ok(Some(parse_live_lookups(&body)))
}

fn parse_live_lookups(body: &str) -> f64 {
    let prefix = format!("{LIVE_LOOKUPS_METRIC}{{");
    body.lines()
        .filter(|line| line.starts_with(&prefix) && line.contains("source=\"live\""))
        .filter_map(|line| line.rsplit_once('}')?.1.trim().parse::<f64>().ok())
        .sum()
}

async fn call(
    client: &reqwest::Client,
    rpc: &RpcEndpoint,
    method: &str,
    params: JsonValue,
) -> Result<JsonValue> {
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    Ok(send_rpc_request(client, rpc, &request, None).await?.0)
}

async fn confirmed_slot(client: &reqwest::Client, rpc: &RpcEndpoint) -> Result<u64> {
    let params = json!([{"commitment": "confirmed"}]);
    let response = call(client, rpc, "getSlot", params).await?;
    let slot = response["result"].as_u64();
    slot.ok_or_else(|| anyhow!("getSlot from {} returned {response}", rpc.name))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "4Nd1mBQtrMJVYVfKf2PJy9NZUZdTAsp7D4xWLs4gDB4T";
    const OWNER: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    #[test]
    fn parses_pubkeys_file() {
        let text = format!("# hot keys\n\n{KEY}\n  {OWNER}  # token program\n");
        assert_eq!(parse_pubkeys(&text).unwrap(), [KEY, OWNER]);
        assert!(parse_pubkeys("not-a-pubkey\n").is_err());
    }

    #[test]
    fn extracts_writable_block_keys() {
        let block = json!({"result": {"transactions": [
            {"transaction": {"accountKeys": [
                {"pubkey": "payer", "writable": true},
                {"pubkey": OWNER, "writable": false},
                {"pubkey": "lookup", "source": "lookupTable", "writable": true},
            ]}},
            {"transaction": {"accountKeys": [
                {"pubkey": "voter", "writable": true},
                {"pubkey": VOTE_PROGRAM, "writable": false},
            ]}},
        ]}});
        assert_eq!(writable_block_keys(&block), ["payer", "lookup"]);
        assert!(writable_block_keys(&json!({"result": null})).is_empty());
    }

    #[test]
    fn percentage_and_exit_decision() {
        assert_eq!((pct(0, 0), pct(1, 4)), (0.0, 25.0));
        let mut t = Tally::default();
        (t.reads, t.slotted, t.fresh, t.abandoned) = (100, 100, 60, 5);
        assert!(exit_failures(&t, 5.0, 50.0, Some((1.0, 2.0))).is_empty());
        assert_eq!(exit_failures(&t, 5.0, 50.0, Some((2.0, 2.0))).len(), 1);
        assert_eq!(exit_failures(&t, 4.0, 70.0, None).len(), 2);
        t.mismatches.push("x".to_string());
        assert_eq!(exit_failures(&t, 5.0, 50.0, None).len(), 1);
        let body = "cloudbreak_api_processed_lookups_total{source=\"live\"} 10\n\
                    cloudbreak_api_processed_lookups_total{source=\"db\"} 100\n";
        assert_eq!(parse_live_lookups(body), 10.0);
    }

    #[test]
    fn row_compare() {
        let want = json!({"lamports": 5, "owner": OWNER, "executable": false,
            "rentEpoch": u64::MAX, "space": 3, "data": [BASE64.encode([1, 2, 3]), "base64"]});
        let (w, null) = (Some(&want), JsonValue::Null);
        let mut value = want.clone();
        assert_eq!(check_account(&value, w, false), Ok(false));
        assert!(check_account(&value, None, false).is_err());
        assert_eq!(check_account(&null, None, false), Ok(false));
        assert_eq!(check_account(&null, w, true), Ok(true));
        value["data"] = json!([BASE64.encode([1, 2, 4]), "base64"]);
        value["lamports"] = json!(6);
        let drift = Err("fields differ: data, lamports".to_string());
        assert_eq!(check_account(&value, w, false), drift);

        let balance = |v: u64| json!({"result": {"context": {"slot": 42}, "value": v}});
        assert_eq!(check_balance(&balance(5), w, false), Ok(false));
        assert_eq!(check_balance(&balance(0), None, false), Ok(false));
        assert!(check_balance(&balance(4), w, false).is_err());
        let error = json!({"error": {"code": -32010, "message": "excluded"}});
        assert_eq!(check_balance(&error, w, true), Ok(true));
        assert!(check_balance(&error, w, false).is_err());

        let filter = (false, HashSet::from([OWNER.to_string()]));
        assert!(excluded(Some(&filter), &want) && !excluded(None, &want));
    }
}
