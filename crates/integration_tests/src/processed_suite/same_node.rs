// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! same-node front: processed reads against Postgres on the same node. Each processed response
//! names a slot S, and once S is confirmed the newest row per key at slot <= S must match it.
//!
//! One sample per second is one getMultipleAccounts base64 at processed. The floor for
//! `behind_confirmed` is the confirmed `context.slot` of a getMultipleAccounts on the same
//! instance, which reads the same slot cache a fallback processed read does. The expected state is
//! read in one REPEATABLE READ transaction. A slot at or below finalized is expired, and a slot
//! with no `recent_blockhashes` row once confirmed passes it is abandoned. Postgres reads use
//! primary keys or the `(pubkey, slot DESC)` index only, never a table scan. A key whose newest
//! row has an excluded owner must read as null.

use super::sources::{OwnerFilter, Reply, owner_excluded};
use super::{Ctx, Tally};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, IsolationLevel};
use sea_orm::{QueryResult, Statement, TransactionTrait};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior};

/// Commitment ids in the slots table.
const CONFIRMED: i32 = 1;
const FINALIZED: i32 = 2;
const PACE: Duration = Duration::from_secs(1);

/// Expected base64 account value per key at slot S, None for a missing or zero-lamport row.
pub type ExpectedMap = HashMap<String, Option<JsonValue>>;

/// Ok(true) when an excluded owner is correctly withheld, Err on a mismatch.
type Verdict = Result<bool, String>;

pub enum SlotState {
    Expired,
    Abandoned,
    Present(ExpectedMap),
}

struct Sample<'a> {
    keys: &'a [String],
    confirmed_before: u64,
    give_up: Instant,
    filter: Option<&'a OwnerFilter>,
}

pub async fn run(ctx: Arc<Ctx>) -> Tally {
    let db = ctx.db.as_ref().expect("same-node needs --db-url");
    let mut tally = Tally::default();
    let mut tick = tokio::time::interval(PACE);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while ctx.running() {
        tokio::select! {
            _ = ctx.stopped() => break,
            _ = tick.tick() => {}
        }
        let keys = ctx.pool.sample(ctx.args.keys_per_sample);
        let Some(confirmed_before) = ctx.confirmed_before().await else {
            tally.note("error");
            continue;
        };
        let params = json!([keys, {"commitment": "processed", "encoding": "base64"}]);
        let reply = (ctx.cloudbreak)
            .call(&ctx.client, "getMultipleAccounts", params)
            .await;
        let sample = Sample {
            keys: &keys,
            confirmed_before,
            give_up: ctx.give_up(Duration::from_secs(ctx.args.timeout)),
            filter: ctx.filter.as_ref(),
        };
        if let Err(e) = verify_sample(&ctx, db, &sample, &reply, &mut tally).await {
            tally.note("db_error");
            eprintln!("same-node: {e:#}");
        }
    }
    tally
}

pub async fn load_owner_filter(db: &DatabaseConnection) -> Option<OwnerFilter> {
    let sql = "SELECT mode = 'include', programs FROM environment_info WHERE id = 1";
    let row = query(db, sql.to_string()).await.ok()?.pop()?;
    let include: bool = row.try_get_by_index(0).ok()?;
    let programs: String = row.try_get_by_index(1).ok()?;
    let programs = programs.split(',').map(str::trim).filter(|p| !p.is_empty());
    Some((include, programs.map(str::to_string).collect()))
}

async fn verify_sample(
    ctx: &Ctx,
    db: &DatabaseConnection,
    sample: &Sample<'_>,
    reply: &Reply,
    tally: &mut Tally,
) -> Result<()> {
    let keys = sample.keys;
    let (Some(response), Some(slot)) = (reply.json(), reply.slot()) else {
        tally.note("error");
        return Ok(());
    };
    tally.samples += 1;
    observe(tally, slot, sample.confirmed_before);
    let expected = match settle(ctx, db, slot, keys, sample.give_up).await? {
        SlotState::Expired => {
            tally.note("expired");
            tally.cover(0, 1);
            return Ok(());
        }
        SlotState::Abandoned => {
            tally.note("abandoned");
            (tally.abandoned, tally.covered_of) = (tally.abandoned + 1, tally.covered_of + 1);
            return Ok(());
        }
        SlotState::Present(expected) => expected,
    };
    tally.cover(1, 1);
    let want = |k: &str| expected.get(k).and_then(Option::as_ref);
    let excluded = |w: Option<&JsonValue>| w.is_some_and(|w| excluded(sample.filter, w));
    let verdicts: Vec<(&str, Verdict)> = match response["result"]["value"].as_array() {
        Some(values) if values.len() == keys.len() => (keys.iter().zip(values))
            .map(|(k, value)| (k.as_str(), check_account(value, want(k), excluded(want(k)))))
            .collect(),
        _ => vec![("sample", Err(format!("no value per key in {response}")))],
    };
    let before = tally.failed;
    for (k, verdict) in verdicts {
        match verdict {
            Ok(true) => tally.note("excluded"),
            Ok(false) => {}
            Err(m) => tally.fail("mismatch", format!("slot {slot}: {k}: {m}")),
        }
    }
    if tally.failed == before {
        tally.pass();
    }
    Ok(())
}

fn observe(tally: &mut Tally, slot: u64, confirmed_before: u64) {
    tally.fresh_of += 1;
    if slot < confirmed_before {
        tally.fail(
            "behind_confirmed",
            format!("slot {slot} below cached confirmed {confirmed_before}"),
        );
    } else if slot > confirmed_before {
        tally.fresh += 1;
    }
}

/// Waits for confirmed to reach `slot`, then reads the expected state. A timeout, `give_up` or
/// the hard stop, counts as abandoned.
pub async fn settle(
    ctx: &Ctx,
    db: &DatabaseConnection,
    slot: u64,
    keys: &[String],
    give_up: Instant,
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
        if ctx.past(give_up) {
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
    let statement = Statement::from_string(DbBackend::Postgres, sql);
    conn.query_all(statement)
        .await
        .map_err(|e| anyhow!("Postgres query failed: {e}"))
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

fn excluded(filter: Option<&OwnerFilter>, account: &JsonValue) -> bool {
    let owner = account["owner"].as_str().unwrap_or_default();
    filter.is_some_and(|f| owner_excluded(f, owner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const OWNER: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

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

        let filter = (false, HashSet::from([OWNER.to_string()]));
        assert!(excluded(Some(&filter), &want) && !excluded(None, &want));
    }
}
