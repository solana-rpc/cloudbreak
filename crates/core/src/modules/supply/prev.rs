// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The two map-free by-pubkey DB reads: the per-block miss read that resolves an
//! evicted account's previous balance, and the bootstrap resolve read that
//! anchors a startup touch against `snapshot_accounts`.
//!
//! Both ride `idx_accounts_pubkey_slot` / `idx_snapshot_accounts_pubkey_slot` and
//! the `(pubkey, slot)` primary key. Neither carries an owner predicate, so there
//! is no wrong-partition miss and no owner-change over-count class.

use crate::metrics::SUPPLY_DB_MICROSECONDS;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value, sea_query::ArrayType,
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, timeout};

fn bytea_array(pubkeys: &[Pubkey]) -> Value {
    Value::Array(
        ArrayType::Bytes,
        Some(Box::new(
            pubkeys
                .iter()
                .map(|pubkey| Value::Bytes(Some(Box::new(pubkey.to_bytes().to_vec()))))
                .collect(),
        )),
    )
}

fn parse_pubkey(bytes: Vec<u8>) -> Result<Pubkey, sea_orm::DbErr> {
    Pubkey::try_from(bytes.as_slice())
        .map_err(|_| sea_orm::DbErr::Custom("invalid pubkey bytes in query result".to_string()))
}

/// Returns the latest `(lamports, slot)` row by pubkey for every miss account.
/// No row means a new account. A live block passes its own slot as `below_slot`
/// so the read skips the block's own row and can overlap the block's inserts. A
/// repaired block passes `None` and reads the newest row: a gap write already
/// absorbed by a later live block must come back, so the caller counts zero.
pub async fn fetch_prev_balances(
    db: &DatabaseConnection,
    pubkeys: &[Pubkey],
    below_slot: Option<u64>,
    query_timeout: Duration,
) -> Result<HashMap<Pubkey, (u64, u64)>, sea_orm::DbErr> {
    let mut out = HashMap::with_capacity(pubkeys.len());
    if pubkeys.is_empty() {
        return Ok(out);
    }

    let query = db.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        // `lamports DESC` breaks a same-slot mask tie. It cannot fire under the
        // `(pubkey, slot)` key but keeps the read exact if it ever does.
        r#"
        SELECT v.pubkey, prev.lamports, prev.slot
        FROM unnest($1::bytea[]) AS v(pubkey)
        LEFT JOIN LATERAL (
            SELECT lamports, slot FROM (
                SELECT lamports, slot FROM accounts          WHERE pubkey = v.pubkey AND slot < $2
                UNION ALL
                SELECT lamports, slot FROM snapshot_accounts WHERE pubkey = v.pubkey AND slot < $2
            ) u
            ORDER BY slot DESC, lamports DESC
            LIMIT 1
        ) prev ON true
        "#,
        [
            bytea_array(pubkeys),
            Value::BigInt(Some(below_slot.map_or(i64::MAX, |slot| slot as i64))),
        ],
    ));

    let started = Instant::now();
    let rows = timeout(query_timeout, query).await.map_err(|elapsed| {
        sea_orm::DbErr::Custom(format!("fetch_prev_balances timeout: {elapsed}"))
    });
    SUPPLY_DB_MICROSECONDS
        .with_label_values(&["prev_balances"])
        .observe(started.elapsed().as_micros() as f64);

    for row in rows?? {
        let pubkey = parse_pubkey(row.try_get("", "pubkey")?)?;
        let lamports: Option<i64> = row.try_get("", "lamports")?;
        let slot: Option<i64> = row.try_get("", "slot")?;
        if let (Some(lamports), Some(slot)) = (lamports, slot) {
            out.insert(pubkey, (lamports as u64, slot as u64));
        }
    }

    Ok(out)
}

/// Resolves the balance at or below `startup_slot` for every startup touch,
/// by pubkey against `snapshot_accounts`. A missing account resolves to 0, so
/// the bootstrap window delta counts its full balance on the first live touch.
pub async fn fetch_startup_balances(
    db: &DatabaseConnection,
    pubkeys: &[Pubkey],
    startup_slot: u64,
    query_timeout: Duration,
) -> Result<HashMap<Pubkey, u64>, sea_orm::DbErr> {
    let mut out = HashMap::with_capacity(pubkeys.len());
    if pubkeys.is_empty() {
        return Ok(out);
    }

    let query = db.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        r#"
        SELECT v.pubkey, COALESCE(prev.lamports, 0) AS lamports
        FROM unnest($1::bytea[]) AS v(pubkey)
        LEFT JOIN LATERAL (
            SELECT lamports FROM snapshot_accounts
            WHERE pubkey = v.pubkey AND slot <= $2
            ORDER BY slot DESC, lamports DESC
            LIMIT 1
        ) prev ON true
        "#,
        [
            bytea_array(pubkeys),
            Value::BigInt(Some(startup_slot as i64)),
        ],
    ));

    let started = Instant::now();
    let rows = timeout(query_timeout, query).await.map_err(|elapsed| {
        sea_orm::DbErr::Custom(format!("fetch_startup_balances timeout: {elapsed}"))
    });
    SUPPLY_DB_MICROSECONDS
        .with_label_values(&["startup_balances"])
        .observe(started.elapsed().as_micros() as f64);

    for row in rows?? {
        let pubkey = parse_pubkey(row.try_get("", "pubkey")?)?;
        let lamports: i64 = row.try_get("", "lamports")?;
        out.insert(pubkey, lamports as u64);
    }

    Ok(out)
}
