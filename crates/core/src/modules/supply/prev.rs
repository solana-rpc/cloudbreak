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

use crate::modules::supply::cache::PrevRow;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value, sea_query::ArrayType,
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

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

/// Returns the latest row by pubkey for every miss account. A key with no row is
/// absent from the map, meaning a new account. The `prev_slot >= block_slot` test
/// is applied by the caller in Rust, not with a `slot < block` SQL filter, which
/// would double-count a gap write for an evicted account.
pub async fn fetch_prev_balances(
    db: &DatabaseConnection,
    pubkeys: &[Pubkey],
    query_timeout: Duration,
) -> Result<HashMap<Pubkey, (u64, u64, u64)>, sea_orm::DbErr> {
    let mut out = HashMap::with_capacity(pubkeys.len());
    if pubkeys.is_empty() {
        return Ok(out);
    }

    let query = db.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        // No owner predicate and no slot filter. `lamports DESC` is the same-slot
        // mask tie-break; it cannot fire under the `(pubkey, slot)` primary key
        // but keeps the read exact if a mask and a real row ever coexist.
        r#"
        SELECT v.pubkey, prev.lamports, prev.slot, prev.write_version
        FROM unnest($1::bytea[]) AS v(pubkey)
        LEFT JOIN LATERAL (
            SELECT lamports, slot, write_version FROM (
                SELECT lamports, slot, write_version FROM accounts          WHERE pubkey = v.pubkey
                UNION ALL
                SELECT lamports, slot, write_version FROM snapshot_accounts WHERE pubkey = v.pubkey
            ) u
            ORDER BY slot DESC, lamports DESC
            LIMIT 1
        ) prev ON true
        "#,
        [bytea_array(pubkeys)],
    ));

    let rows = timeout(query_timeout, query)
        .await
        .map_err(|elapsed| sea_orm::DbErr::Custom(format!("fetch_prev_balances timeout: {elapsed}")))??;

    for row in rows {
        let pubkey = parse_pubkey(row.try_get("", "pubkey")?)?;
        let lamports: Option<i64> = row.try_get("", "lamports")?;
        let slot: Option<i64> = row.try_get("", "slot")?;
        let wv: Option<i64> = row.try_get("", "write_version")?;
        if let (Some(lamports), Some(slot), Some(wv)) = (lamports, slot, wv) {
            out.insert(pubkey, (lamports as u64, slot as u64, wv as u64));
        }
    }

    Ok(out)
}

/// Convenience wrapper so a caller can look up one miss result as a [`PrevRow`].
pub fn prev_row(map: &HashMap<Pubkey, (u64, u64, u64)>, pubkey: &Pubkey) -> PrevRow {
    map.get(pubkey).copied()
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
        [bytea_array(pubkeys), Value::BigInt(Some(startup_slot as i64))],
    ));

    let rows = timeout(query_timeout, query).await.map_err(|elapsed| {
        sea_orm::DbErr::Custom(format!("fetch_startup_balances timeout: {elapsed}"))
    })??;

    for row in rows {
        let pubkey = parse_pubkey(row.try_get("", "pubkey")?)?;
        let lamports: i64 = row.try_get("", "lamports")?;
        out.insert(pubkey, lamports as u64);
    }

    Ok(out)
}

/// Fetches the latest lamports/slot by pubkey for the non-circulating members
/// that are not stake accounts (the pinned-list holders). Owner-blind, one
/// batched read. Stake member balances come from the recomputer's scan instead.
pub async fn fetch_member_balances(
    db: &DatabaseConnection,
    pubkeys: &[Pubkey],
    query_timeout: Duration,
) -> Result<Vec<(Pubkey, u64, u64)>, sea_orm::DbErr> {
    let map = fetch_prev_balances(db, pubkeys, query_timeout).await?;
    Ok(map
        .into_iter()
        .map(|(pubkey, (lamports, slot, _wv))| (pubkey, lamports, slot))
        .collect())
}
