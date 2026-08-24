use std::collections::HashSet;
use std::time::Duration;

use cloudbreak_core::STAKE_PROGRAM_ID;
use cloudbreak_core::modules::account_owner_map::AccountOwnerMap;
use cloudbreak_core::modules::supply_tracker::SupplyTracker;
use futures::TryStreamExt;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, StreamTrait};
use solana_program::clock::Clock;
use solana_pubkey::Pubkey;
use solana_stake_interface::state::StakeStateV2;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::modules::epoch_stakes::{LATEST_BY_OWNER_SQL, is_healthy};
use crate::{db_queries, metrics};
use crate::modules::non_circulating_lists::{NON_CIRCULATING_ACCOUNTS, WITHDRAW_AUTHORITY};

const POLL_INTERVAL: Duration = Duration::from_secs(60);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(600);

const SYSVAR_OWNER_ID: Pubkey =
    Pubkey::from_str_const("Sysvar1111111111111111111111111111111111111");
const CLOCK_SYSVAR_ID: Pubkey =
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111");

pub fn spawn_non_circulating_recomputer(
    db: DatabaseConnection,
    supply_tracker: SupplyTracker,
    accounts_owner_map: AccountOwnerMap,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("non_circulating_recomputer");

        if !supply_tracker.is_enabled() {
            return;
        }

        let mut last_recompute: Option<Instant> = None;
        let mut next_lockup_expiry: Option<i64> = None;
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if !is_healthy(&db).await {
                continue;
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let expiry_due = next_lockup_expiry.is_some_and(|ts| now >= ts);

            if !expiry_due && last_recompute.is_some_and(|last| last.elapsed() < HEARTBEAT_INTERVAL)
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
            let balances = match db_queries::fetch_non_circulating_balances(&db, &members).await {
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
            db_queries::upsert_non_circulating_accounts(&db, slot, &accounts).await;
            supply_tracker.set_non_circulating_accounts(accounts, balances);
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
