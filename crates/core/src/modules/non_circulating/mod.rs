// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Non-circulating membership: the in-memory set plus the background recomputer
//! that resolves it.
//!
//! The recomputer periodically resolves which accounts are non-circulating and
//! calls [`NonCirculatingTracker::set_members`]. The block ingest path reads it
//! with [`NonCirculatingTracker::is_non_circulating`] to classify each account
//! for the `getLargestAccounts` circulating/non-circulating filter. It also
//! fetches the members' balances to seed the largest-accounts class sentinels.

pub mod lists;

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::TryStreamExt;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, StreamTrait, Value,
    sea_query::ArrayType,
};
use solana_program::clock::Clock;
use solana_pubkey::Pubkey;
use solana_stake_interface::state::StakeStateV2;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::metrics::TokioTaskCounterGuard;
use crate::modules::account_owner_map::AccountOwnerMap;
use crate::modules::largest_accounts::{LargestAccountsTracker, persist_largest_outcome};
use crate::modules::service_health::is_healthy;
use crate::{IndexConfig, STAKE_PROGRAM_ID};
use lists::{NON_CIRCULATING_ACCOUNTS, WITHDRAW_AUTHORITY};

/// One non-circulating holder's balance at a slot. The recomputer fetches these
/// to seed the largest-accounts class sentinels.
#[derive(Clone, Debug)]
pub struct NonCirculatingBalance {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub lamports: u64,
}

/// Cheap-clone handle to the non-circulating membership set. The default
/// (`None`) means the feature is disabled and every operation is a no-op.
#[derive(Clone, Default)]
pub struct NonCirculatingTracker(Option<Arc<RwLock<Option<HashSet<Pubkey>>>>>);

impl NonCirculatingTracker {
    pub fn new() -> Self {
        Self(Some(Arc::new(RwLock::new(None))))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub fn is_non_circulating(&self, pubkey: &Pubkey) -> bool {
        let Some(inner) = &self.0 else { return false };
        inner
            .read()
            .expect("Failed to read non-circulating members")
            .as_ref()
            .is_some_and(|members| members.contains(pubkey))
    }

    pub fn set_members(&self, accounts: Vec<Pubkey>) {
        let Some(inner) = &self.0 else { return };
        let members: HashSet<Pubkey> = accounts.into_iter().collect();
        *inner
            .write()
            .expect("Failed to write non-circulating members") = Some(members);
    }
}

const POLL_INTERVAL: Duration = Duration::from_secs(60);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(600);

const SYSVAR_OWNER_ID: Pubkey =
    Pubkey::from_str_const("Sysvar1111111111111111111111111111111111111");
const CLOCK_SYSVAR_ID: Pubkey =
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111");

/// Latest live state per account for a given owner, across the live and snapshot tables.
pub const LATEST_BY_OWNER_SQL: &str = r#"
WITH latest AS (
    SELECT DISTINCT ON (pubkey) pubkey, data, lamports
    FROM (
        SELECT pubkey, slot, data, lamports FROM accounts WHERE owner = $1
        UNION ALL
        SELECT pubkey, slot, data, lamports FROM snapshot_accounts WHERE owner = $1
    ) AS u
    ORDER BY pubkey, slot DESC
)
SELECT pubkey, data FROM latest WHERE lamports > 0
"#;

const LATEST_ACCOUNT_ROW_SQL: &str = r#"
            SELECT lamports, slot FROM (
                SELECT lamports, slot FROM accounts
                WHERE owner = v.owner AND pubkey = v.pubkey
                UNION ALL
                SELECT lamports, slot FROM snapshot_accounts
                WHERE owner = v.owner AND pubkey = v.pubkey
            ) u
            ORDER BY slot DESC
            LIMIT 1
"#;

/// Spawns the background task that periodically recomputes the non-circulating
/// membership set and, when largest-accounts is enabled, seeds its class
/// sentinels. No-op when the tracker is disabled.
pub fn spawn_non_circulating_recomputer(
    db: DatabaseConnection,
    config: IndexConfig,
    non_circulating: NonCirculatingTracker,
    accounts_owner_map: AccountOwnerMap,
    largest_accounts: LargestAccountsTracker,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _guard = TokioTaskCounterGuard::new("non_circulating_recomputer");

        if !non_circulating.is_enabled() {
            return;
        }

        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_recompute: Option<Instant> = None;
        let mut next_lockup_expiry: Option<i64> = None;
        let mut class_sentinels_seeded = false;
        loop {
            interval.tick().await;

            if !is_healthy(&db).await {
                continue;
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let expiry_due = next_lockup_expiry.is_some_and(|ts| now >= ts);
            let seed_due = !class_sentinels_seeded && largest_accounts.is_live();

            if !expiry_due
                && !seed_due
                && last_recompute.is_some_and(|last| last.elapsed() < HEARTBEAT_INTERVAL)
            {
                continue;
            }

            let (slot, accounts, next_expiry) = match recompute(&db).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(
                        target: "non_circulating_recomputer",
                        "failed to recompute non-circulating membership: {:?}",
                        e
                    );
                    continue;
                }
            };
            let members: Vec<(Pubkey, Pubkey)> = accounts
                .iter()
                .filter_map(|pubkey| {
                    accounts_owner_map
                        .get_owner(pubkey)
                        .map(|owner| (owner, *pubkey))
                })
                .collect();
            let balances = match fetch_non_circulating_balances(&db, &members).await {
                Ok(balances) => balances,
                Err(e) => {
                    tracing::error!(
                        target: "non_circulating_recomputer",
                        "failed to fetch non-circulating balances: {:?}",
                        e
                    );
                    continue;
                }
            };

            last_recompute = Some(Instant::now());
            next_lockup_expiry = next_expiry;
            let member_set: HashSet<Pubkey> = accounts.iter().copied().collect();
            non_circulating.set_members(accounts);
            if let Some(outcome) =
                largest_accounts.seed_class_sentinels(slot, &member_set, &balances)
            {
                persist_largest_outcome(&largest_accounts, outcome, slot, &db, &config).await;
                class_sentinels_seeded = true;
            }
        }
    })
}

async fn recompute(
    db: &DatabaseConnection,
) -> Result<(u64, Vec<Pubkey>, Option<i64>), anyhow::Error> {
    let start_time = Instant::now();
    let clock = read_clock(db)
        .await
        .ok_or_else(|| anyhow::anyhow!("Clock sysvar not found in index"))?;

    let withdraw_authorities: HashSet<Pubkey> = WITHDRAW_AUTHORITY.iter().copied().collect();
    let mut set: HashSet<Pubkey> = NON_CIRCULATING_ACCOUNTS.iter().copied().collect();
    let mut next_expiry: Option<i64> = None;

    let mut stream = db
        .stream(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            LATEST_BY_OWNER_SQL,
            [STAKE_PROGRAM_ID.to_bytes().to_vec().into()],
        ))
        .await?;
    while let Some(row) = stream.try_next().await? {
        let pubkey_bytes: Vec<u8> = row.try_get("", "pubkey")?;
        let data: Vec<u8> = row.try_get("", "data")?;
        let Ok(pubkey) = Pubkey::try_from(pubkey_bytes.as_slice()) else {
            continue;
        };
        let Ok(state) = bincode::deserialize::<StakeStateV2>(&data) else {
            continue;
        };
        let meta = match state {
            StakeStateV2::Initialized(meta) => meta,
            StakeStateV2::Stake(meta, _stake, _flags) => meta,
            _ => continue,
        };
        let in_force = meta.lockup.is_in_force(&clock, None);
        if in_force
            && meta.lockup.epoch <= clock.epoch
            && meta.lockup.unix_timestamp > clock.unix_timestamp
        {
            let ts = meta.lockup.unix_timestamp;
            next_expiry = Some(next_expiry.map_or(ts, |current| current.min(ts)));
        }
        if in_force || withdraw_authorities.contains(&meta.authorized.withdrawer) {
            set.insert(pubkey);
        }
    }

    tracing::debug!(
        target: "non_circulating_recomputer",
        "recomputed membership ({} accounts) in {:.3}s",
        set.len(),
        start_time.elapsed().as_secs_f64()
    );
    Ok((clock.slot, set.into_iter().collect(), next_expiry))
}

async fn read_clock(db: &DatabaseConnection) -> Option<Clock> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            r#"
            SELECT data FROM (
                SELECT slot, data, lamports FROM accounts
                    WHERE owner = $1 AND pubkey = $2
                UNION ALL
                SELECT slot, data, lamports FROM snapshot_accounts
                    WHERE owner = $1 AND pubkey = $2
            ) AS u
            WHERE lamports > 0
            ORDER BY slot DESC
            LIMIT 1
            "#,
            [
                SYSVAR_OWNER_ID.to_bytes().to_vec().into(),
                CLOCK_SYSVAR_ID.to_bytes().to_vec().into(),
            ],
        ))
        .await
        .ok()
        .flatten()?;

    let data: Vec<u8> = row.try_get("", "data").ok()?;
    bincode::deserialize::<Clock>(&data).ok()
}

/// Fetches the latest lamports/slot for each `(owner, pubkey)` member, routed by
/// owner so the query prunes to the owner's partition.
async fn fetch_non_circulating_balances(
    db: &DatabaseConnection,
    members: &[(Pubkey, Pubkey)],
) -> Result<Vec<NonCirculatingBalance>, sea_orm::DbErr> {
    if members.is_empty() {
        return Ok(Vec::new());
    }

    let (owners, pubkeys) = owner_pubkey_arrays(members);

    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            format!(
                r#"
            SELECT v.pubkey, latest.lamports, latest.slot
            FROM unnest($1::bytea[], $2::bytea[]) AS v(owner, pubkey)
            JOIN LATERAL ({LATEST_ACCOUNT_ROW_SQL}) latest ON true
            "#
            ),
            [owners, pubkeys],
        ))
        .await?;

    rows.into_iter()
        .map(|row| {
            let pubkey = parse_pubkey(row.try_get("", "pubkey")?)?;
            let lamports: i64 = row.try_get("", "lamports")?;
            let slot: i64 = row.try_get("", "slot")?;
            Ok(NonCirculatingBalance {
                pubkey,
                slot: slot as u64,
                lamports: lamports as u64,
            })
        })
        .collect()
}

fn bytea_array(items: Vec<Vec<u8>>) -> Value {
    Value::Array(
        ArrayType::Bytes,
        Some(Box::new(
            items
                .into_iter()
                .map(|bytes| Value::Bytes(Some(Box::new(bytes))))
                .collect(),
        )),
    )
}

fn parse_pubkey(bytes: Vec<u8>) -> Result<Pubkey, sea_orm::DbErr> {
    Pubkey::try_from(bytes.as_slice())
        .map_err(|_| sea_orm::DbErr::Custom("invalid pubkey bytes in query result".to_string()))
}

fn owner_pubkey_arrays(pairs: &[(Pubkey, Pubkey)]) -> (Value, Value) {
    let owners = pairs
        .iter()
        .map(|(owner, _)| owner.to_bytes().to_vec())
        .collect();
    let pubkeys = pairs
        .iter()
        .map(|(_, pubkey)| pubkey.to_bytes().to_vec())
        .collect();
    (bytea_array(owners), bytea_array(pubkeys))
}
