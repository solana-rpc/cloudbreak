// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use crate::config::{DbCheckConfig, RpcEndpoint};
use crate::response_comparison::ReponseComparison;
use crate::utils;

pub struct AccountDbInfo {
    pub pubkey: String,
    pub slot: u64,
    pub lamports: i64,
}

pub async fn check_differing_accounts(
    client: &reqwest::Client,
    response_comparison: &ReponseComparison,
    db_check_config: &DbCheckConfig,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
) -> Result<()> {
    let Some(diffs) = utils::diff_accounts(
        &response_comparison.response1,
        &response_comparison.response2,
    ) else {
        return Ok(());
    };

    if diffs.is_empty() {
        return Ok(());
    }

    let all_pubkeys: Vec<String> = diffs.iter().map(|d| d.pubkey.clone()).collect();

    let slot1 = utils::get_slot(&response_comparison.response1);
    let slot2 = utils::get_slot(&response_comparison.response2);

    let db = Database::connect(&db_check_config.db_url)
        .await
        .context("Failed to connect to database for db_check")?;

    let db_slots = fetch_db_slots(&db).await?;

    let db_accounts = find_accounts_in_db(&db, &all_pubkeys).await?;
    let db_map: HashMap<&str, &AccountDbInfo> =
        db_accounts.iter().map(|a| (a.pubkey.as_str(), a)).collect();

    tracing::info!(
        target: "bench_db_check",
        "🔍 {} differing accounts | response slots: {} = {} | {} = {} | db confirmed: {} | db finalized: {}",
        diffs.len(),
        rpc1.name,
        slot1.map(|s| s.to_string()).unwrap_or("none".into()),
        rpc2.name,
        slot2.map(|s| s.to_string()).unwrap_or("none".into()),
        db_slots.confirmed.map(|s| s.to_string()).unwrap_or("n/a".into()),
        db_slots.finalized.map(|s| s.to_string()).unwrap_or("n/a".into()),
    );

    let response_slot = slot1.or(slot2);

    let should_get_sigs = db_check_config.get_last_signature && db_check_config.rpc_url.is_some();
    let rpc_url = db_check_config.rpc_url.as_deref().unwrap_or("");

    for diff in &diffs {
        let pk = &diff.pubkey;
        let kind_label = match diff.kind {
            utils::DiffKind::DataMismatch => "mismatched data",
            utils::DiffKind::OnlyRpc1 => &format!("only in {}", rpc1.name),
            utils::DiffKind::OnlyRpc2 => &format!("only in {}", rpc2.name),
        };

        let db_info = db_map.get(pk.as_str());
        let behind_info = match (db_info, response_slot) {
            (Some(info), Some(rs)) => format!(
                "db slot: {} ({} slots behind) | lamports: {}",
                info.slot,
                rs as i64 - info.slot as i64,
                info.lamports,
            ),
            (Some(info), None) => format!("db slot: {} | lamports: {}", info.slot, info.lamports),
            _ => "not in db".to_string(),
        };

        tracing::info!(
            target: "bench_db_check",
            "  {} | {} | {}",
            pk,
            kind_label,
            behind_info,
        );

        if should_get_sigs {
            let db_slot = db_info.map(|i| i.slot).unwrap_or(0);
            match get_missed_tx_slots(client, rpc_url, pk, db_slot).await {
                Ok(result) => {
                    let latest_label = result
                        .latest_tx_slot
                        .map(|s| {
                            let behind = response_slot
                                .map(|rs| format!(" ({} slots behind)", rs as i64 - s as i64))
                                .unwrap_or_default();
                            format!("latest tx slot: {s}{behind}")
                        })
                        .unwrap_or_else(|| "no txs found via RPC".to_string());

                    if result.missed_slots.is_empty() {
                        tracing::info!(
                            target: "bench_db_check",
                            "    └─ no missed txs | {}",
                            latest_label,
                        );
                    } else {
                        let slot_strs: Vec<String> =
                            result.missed_slots.iter().map(|s| s.to_string()).collect();
                        tracing::info!(
                            target: "bench_db_check",
                            "    └─ {} missed txs at slots: [{}] | {}",
                            result.missed_slots.len(),
                            slot_strs.join(", "),
                            latest_label,
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        target: "bench_db_check",
                        "    └─ getSignaturesForAddress err: {}",
                        e,
                    );
                }
            }
        }
    }

    Ok(())
}

struct DbSlots {
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

async fn fetch_db_slots(db: &sea_orm::DatabaseConnection) -> Result<DbSlots> {
    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "SELECT commitment, slot FROM slots WHERE commitment IN (1, 2)".to_string(),
    );
    let rows = db
        .query_all(stmt)
        .await
        .context("Failed to query slots table")?;

    let mut confirmed = None;
    let mut finalized = None;
    for row in &rows {
        let commitment: i32 = row.try_get_by_index(0).unwrap_or(0);
        let slot: i64 = row.try_get_by_index(1).unwrap_or(0);
        match commitment {
            1 => confirmed = Some(slot as u64),
            2 => finalized = Some(slot as u64),
            _ => {}
        }
    }

    Ok(DbSlots {
        confirmed,
        finalized,
    })
}

async fn find_accounts_in_db(
    db: &sea_orm::DatabaseConnection,
    pubkeys: &[String],
) -> Result<Vec<AccountDbInfo>> {
    if pubkeys.is_empty() {
        return Ok(vec![]);
    }

    let pubkey_literals: Vec<String> = pubkeys
        .iter()
        .filter_map(|pk| bs58::decode(pk).into_vec().ok())
        .map(|bytes| format!("'\\x{}'::bytea", hex::encode(&bytes)))
        .collect();

    let in_clause = pubkey_literals.join(", ");
    let sql = format!(
        "SELECT pubkey, slot, lamports FROM (\
            SELECT DISTINCT ON (combined.pubkey) combined.pubkey, combined.slot, combined.lamports \
            FROM (\
                SELECT pubkey, slot, lamports FROM accounts WHERE pubkey IN ({in_clause}) \
                UNION ALL \
                SELECT pubkey, slot, lamports FROM snapshot_accounts WHERE pubkey IN ({in_clause})\
            ) combined \
            ORDER BY combined.pubkey ASC, combined.slot DESC\
        ) latest"
    );

    let stmt = Statement::from_string(DbBackend::Postgres, sql);
    let rows = db
        .query_all(stmt)
        .await
        .context("Failed to query accounts for db_check")?;

    let results: Vec<AccountDbInfo> = rows
        .iter()
        .filter_map(|row| {
            let pubkey: Vec<u8> = row.try_get_by_index(0).ok()?;
            let slot: i64 = row.try_get_by_index(1).ok()?;
            let lamports: i64 = row.try_get_by_index(2).ok()?;

            Some(AccountDbInfo {
                pubkey: bs58::encode(&pubkey).into_string(),
                slot: slot as u64,
                lamports,
            })
        })
        .collect();

    Ok(results)
}

struct SigCheckResult {
    missed_slots: Vec<u64>,
    latest_tx_slot: Option<u64>,
}

/// Calls `getSignaturesForAddress` and returns all tx slots that are strictly
/// higher than `after_slot` (i.e. transactions the DB missed), sorted ascending.
/// Also returns the latest (highest) tx slot seen across all signatures.
async fn get_missed_tx_slots(
    client: &reqwest::Client,
    rpc_url: &str,
    pubkey: &str,
    after_slot: u64,
) -> Result<SigCheckResult> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [pubkey, {"limit": 1000}]
    });

    let response: JsonValue = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await
        .context("getSignaturesForAddress request failed")?
        .json()
        .await
        .context("getSignaturesForAddress response parse failed")?;

    if let Some(error) = response.get("error") {
        anyhow::bail!(
            "getSignaturesForAddress RPC error for {}: {}",
            pubkey,
            error
        );
    }

    let result_array = response.get("result").and_then(|r| r.as_array());
    let total_sigs = result_array.map(|a| a.len()).unwrap_or(0);

    let all_slots: Vec<u64> = result_array
        .map(|arr| {
            arr.iter()
                .filter_map(|entry| entry.get("slot")?.as_u64())
                .collect()
        })
        .unwrap_or_default();

    tracing::debug!(
        target: "bench_db_check",
        "    getSignaturesForAddress {} | rpc: {} | total sigs: {} | after_slot: {} | slot range: {:?}..{:?}",
        pubkey,
        rpc_url,
        total_sigs,
        after_slot,
        all_slots.last(),
        all_slots.first(),
    );

    let latest_tx_slot = all_slots.iter().copied().max();

    let mut missed_slots: Vec<u64> = all_slots.into_iter().filter(|&s| s > after_slot).collect();

    missed_slots.sort_unstable();
    missed_slots.dedup();
    Ok(SigCheckResult {
        missed_slots,
        latest_tx_slot,
    })
}

// ---------------------------------------------------------------------------
// Per-iteration DB probe for getBalance (concurrent with the rpc1+rpc2 join)
// ---------------------------------------------------------------------------

/// One row of the `slots` table observed at probe time. `commitment` mirrors
/// `crates/entity/src/lib.rs::CommitmentLevel`: 0 = Processed, 1 = Confirmed,
/// 2 = Finalized.
#[derive(Clone, Debug)]
pub struct DbSlotRow {
    pub commitment: i32,
    pub slot: i64,
}

/// One `(slot, lamports)` row found for the probed pubkey in either
/// `accounts` or `snapshot_accounts`.
#[derive(Clone, Debug)]
pub struct DbAccountRow {
    pub table: &'static str,
    pub slot: i64,
    pub lamports: i64,
}

/// Captured state of the Cloudbreak DB at a single instant, used to cross-check
/// what `getBalance` would have seen. Stored on each `IterationCapture` when
/// `comparison.save_db_probe_iterations = true`.
#[derive(Clone, Debug)]
pub struct DbProbeResult {
    pub probed_at: SystemTime,
    pub slots: Vec<DbSlotRow>,
    pub accounts: Vec<DbAccountRow>,
}

/// Per-request context carried alongside each `getBalance` iteration. The
/// pubkey bytes (bytea form) are pre-computed once in `process_request` so
/// every iteration can reuse them without re-parsing the request JSON.
#[derive(Clone)]
pub struct DbProbeCtx {
    pub db: Arc<DatabaseConnection>,
    pub pubkey_bytes: Vec<u8>,
}

const DB_PROBE_ACCOUNTS_LIMIT: u32 = 20;

/// Set once when the first DB-probe failure is logged so we don't spam the
/// log with the same `WARN`. Subsequent failures still bubble up as `None`
/// from `probe_get_balance_state` and the saved iteration records the absence
/// as `"db_probe": null`.
static DB_PROBE_WARNED: AtomicBool = AtomicBool::new(false);

/// Runs both probe queries (slots row dump + top-N account rows for the
/// pubkey across `accounts` and `snapshot_accounts`) and returns the captured
/// state. Returns `None` on failure after warning once at WARN.
pub async fn probe_get_balance_state(ctx: &DbProbeCtx) -> Option<DbProbeResult> {
    let probed_at = SystemTime::now();
    let db = ctx.db.as_ref();

    let slots_stmt = Statement::from_string(
        DbBackend::Postgres,
        "SELECT commitment, slot FROM slots".to_string(),
    );

    let pubkey_literal = format!("'\\x{}'::bytea", hex::encode(&ctx.pubkey_bytes));
    // Two separate top-N queries unioned so the LIMIT applies per source table.
    let accounts_sql = format!(
        "SELECT table_name, slot, lamports FROM (\
            (SELECT 'accounts'::text AS table_name, slot, lamports \
             FROM accounts WHERE pubkey = {pk} \
             ORDER BY slot DESC LIMIT {limit}) \
            UNION ALL \
            (SELECT 'snapshot_accounts'::text AS table_name, slot, lamports \
             FROM snapshot_accounts WHERE pubkey = {pk} \
             ORDER BY slot DESC LIMIT {limit})\
        ) combined ORDER BY slot DESC",
        pk = pubkey_literal,
        limit = DB_PROBE_ACCOUNTS_LIMIT,
    );
    let accounts_stmt = Statement::from_string(DbBackend::Postgres, accounts_sql);

    let (slots_res, accounts_res) =
        tokio::join!(db.query_all(slots_stmt), db.query_all(accounts_stmt));

    let slots_rows = match slots_res {
        Ok(r) => r,
        Err(e) => {
            log_db_probe_failure(&format!("slots query: {e}"));
            return None;
        }
    };
    let accounts_rows = match accounts_res {
        Ok(r) => r,
        Err(e) => {
            log_db_probe_failure(&format!("accounts query: {e}"));
            return None;
        }
    };

    let slots: Vec<DbSlotRow> = slots_rows
        .iter()
        .filter_map(|row| {
            let commitment: i32 = row.try_get_by_index(0).ok()?;
            let slot: i64 = row.try_get_by_index(1).ok()?;
            Some(DbSlotRow { commitment, slot })
        })
        .collect();

    let accounts: Vec<DbAccountRow> = accounts_rows
        .iter()
        .filter_map(|row| {
            let table: String = row.try_get_by_index(0).ok()?;
            let slot: i64 = row.try_get_by_index(1).ok()?;
            let lamports: i64 = row.try_get_by_index(2).ok()?;
            let table_static: &'static str = match table.as_str() {
                "accounts" => "accounts",
                "snapshot_accounts" => "snapshot_accounts",
                _ => return None,
            };
            Some(DbAccountRow {
                table: table_static,
                slot,
                lamports,
            })
        })
        .collect();

    Some(DbProbeResult {
        probed_at,
        slots,
        accounts,
    })
}

fn log_db_probe_failure(detail: &str) {
    if !DB_PROBE_WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            "DB probe failed (this warning fires once per run): {}. Subsequent iterations will record `db_probe: null` silently.",
            detail,
        );
    }
}

/// Builds the shared `DatabaseConnection` used by every per-iteration probe.
/// Returns `Err` if the `db_check` section is missing — callers should have
/// validated that already, this just turns the wiring into a single call.
pub async fn build_db_probe_pool(
    db_check_config: &Option<DbCheckConfig>,
) -> Result<Arc<DatabaseConnection>> {
    let cfg = db_check_config
        .as_ref()
        .context("save_db_probe_iterations requires [db_check] to be configured")?;
    let db = Database::connect(&cfg.db_url)
        .await
        .context("Failed to connect to db_check.db_url for per-iteration probe")?;
    Ok(Arc::new(db))
}
