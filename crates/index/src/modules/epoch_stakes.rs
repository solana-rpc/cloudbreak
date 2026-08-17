use std::collections::HashMap;
use std::time::Duration;

use cloudbreak_core::{IndexConfig, STAKE_PROGRAM_ID, VOTE_PROGRAM_ID};
use cloudbreak_snapshot::persist_epoch_stakes;
use cloudbreak_snapshot::stake_data::{SnapshotStakeData, VoterStakeRow};
use futures::TryStreamExt;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, StreamTrait};
use solana_pubkey::Pubkey;
use solana_stake_interface::{stake_history::StakeHistory, state::StakeStateV2};
use solana_vote_interface::state::VoteStateV4;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use crate::metrics;

const SLOTS_PER_EPOCH: u64 = 432_000;
/// Mainnet `reduce_stake_warmup_cooldown` feature activation epoch (slot 244_080_000).
/// Equivalent to `Some(0)` for any current epoch (rate is 9% past this epoch).
const NEW_RATE_ACTIVATION_EPOCH: Option<u64> = Some(565);
/// Loop tick: how often we poll health/epoch and (while refining) recompute.
const POLL_INTERVAL: Duration = Duration::from_secs(60);
/// Once an epoch's stakes have converged we don't freeze permanently, we keep recomputing on
/// this slower cadence as a self-healing backstop. If a late reward write lands (or an ingestion
/// gap heals) and the total moves, we drop back to fast refinement
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(600);
/// Recompute each epoch until its total stake holds steady across this many consecutive polls.
const STABLE_POLLS_REQUIRED: u32 = 3;
/// Bounds the *fast* refinement phase. Normally we converge once the EpochRewards sysvar reports
/// distribution complete AND the total is stable; this only matters if that signal never arrives
/// (e.g. the Sysvar owner isn't in the account filter), in which case we converge on stability
/// alone after this many fast polls (~30 min) and fall back to the slow heartbeat.
const MAX_RECOMPUTES_PER_EPOCH: u32 = 30;

/// The EpochRewards sysvar and its owning program. We read the sysvar from our *own* indexed
/// accounts so its `active` flag is ordered, in the same gRPC stream, after every reward-
/// distribution write
const SYSVAR_OWNER_ID: Pubkey =
    Pubkey::from_str_const("Sysvar1111111111111111111111111111111111111");
const EPOCH_REWARDS_SYSVAR_ID: Pubkey =
    Pubkey::from_str_const("SysvarEpochRewards1111111111111111111111111");

/// Latest live state per account for a given owner, across the live and snapshot tables.
const LATEST_BY_OWNER_SQL: &str = r#"
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

/// Recomputes `epoch_stakes` from the indexed Stake accounts so `getVoteAccounts` reflects
/// the current epoch's effective stake
pub fn spawn_epoch_stakes_recomputer(
    db: DatabaseConnection,
    config: IndexConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("epoch_stakes_recomputer");

        if !config.programs.supports_vote_accounts() {
            return;
        }

        let mut working_epoch: Option<u64> = None;
        let mut prev_total: Option<u128> = None;
        let mut stable_polls = 0u32;
        let mut recomputes = 0u32;
        let mut seen_distributing = false;
        let mut converged = false;
        let mut last_recompute: Option<Instant> = None;
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if !is_healthy(&db).await {
                continue;
            }
            let Some(finalized_slot) = finalized_slot(&db).await else {
                continue;
            };
            let epoch = finalized_slot / SLOTS_PER_EPOCH;

            if working_epoch != Some(epoch) {
                working_epoch = Some(epoch);
                prev_total = None;
                stable_polls = 0;
                recomputes = 0;
                seen_distributing = false;
                converged = false;
                last_recompute = None;
            }

            // Once converged, recompute only on the slow heartbeat cadence
            if converged && last_recompute.is_some_and(|last| last.elapsed() < HEARTBEAT_INTERVAL) {
                continue;
            }

            let (voters, total) = match recompute(&db, epoch).await {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(
                        target: "epoch_stakes_recomputer",
                        "failed to recompute epoch_stakes for epoch {}: {:?}",
                        epoch,
                        e
                    );
                    continue;
                }
            };
            last_recompute = Some(Instant::now());
            recomputes += 1;
            stable_polls = if prev_total == Some(total) {
                stable_polls + 1
            } else {
                0
            };
            prev_total = Some(total);

            // Distribution-complete signal from our own indexed EpochRewards sysvar. `None` means it isn't indexed
            let rewards_done = match read_epoch_rewards(&db).await {
                Some(rewards) => {
                    if rewards.active {
                        seen_distributing = true;
                    }
                    seen_distributing && !rewards.active
                }
                None => true,
            };

            if converged {
                if stable_polls == 0 {
                    converged = false;
                    recomputes = 0;
                    tracing::info!(
                        target: "epoch_stakes_recomputer",
                        "epoch_stakes for epoch {} moved on heartbeat (total {}); resuming refinement",
                        epoch,
                        total
                    );
                }
                continue;
            }

            let stable = stable_polls >= STABLE_POLLS_REQUIRED;
            if (rewards_done && stable) || recomputes >= MAX_RECOMPUTES_PER_EPOCH {
                converged = true;
                tracing::info!(
                    target: "epoch_stakes_recomputer",
                    "converged epoch_stakes for epoch {} ({} voters, {} recomputes, rewards_done={})",
                    epoch,
                    voters,
                    recomputes,
                    rewards_done
                );
            } else {
                tracing::debug!(
                    target: "epoch_stakes_recomputer",
                    "refining epoch_stakes for epoch {} ({} voters, stable {}/{}, rewards_done={})",
                    epoch,
                    voters,
                    stable_polls,
                    STABLE_POLLS_REQUIRED,
                    rewards_done
                );
            }
        }
    })
}

async fn is_healthy(db: &DatabaseConnection) -> bool {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT healthy FROM service_health WHERE id = 1".to_string(),
    ))
    .await
    .ok()
    .flatten()
    .and_then(|row| row.try_get::<bool>("", "healthy").ok())
    .unwrap_or(false)
}

async fn finalized_slot(db: &DatabaseConnection) -> Option<u64> {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT slot FROM slots WHERE commitment = $1",
        [(CommitmentLevel::Finalized as i32).into()],
    ))
    .await
    .ok()
    .flatten()
    .and_then(|row| row.try_get::<i64>("", "slot").ok())
    .map(|slot| slot as u64)
}

/// The fields of the EpochRewards sysvar the recomputer cares about.
struct EpochRewardsState {
    /// True while the epoch's reward calculation/distribution is in progress
    active: bool,
}

/// Reads the latest indexed EpochRewards sysvar from our own accounts
async fn read_epoch_rewards(db: &DatabaseConnection) -> Option<EpochRewardsState> {
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
                EPOCH_REWARDS_SYSVAR_ID.to_bytes().to_vec().into(),
            ],
        ))
        .await
        .ok()
        .flatten()?;

    let data: Vec<u8> = row.try_get("", "data").ok()?;
    parse_epoch_rewards(&data)
}

/// Parses the `active` flag from the EpochRewards sysvar
fn parse_epoch_rewards(data: &[u8]) -> Option<EpochRewardsState> {
    const EPOCH_REWARDS_LEN: usize = 81;
    if data.len() < EPOCH_REWARDS_LEN {
        return None;
    }
    Some(EpochRewardsState {
        active: data[EPOCH_REWARDS_LEN - 1] != 0,
    })
}

/// Returns `(voter_count, total_activated_stake)`. The total is used to detect when the
/// epoch's stakes have stabilized (see `spawn_epoch_stakes_recomputer`).
async fn recompute(db: &DatabaseConnection, epoch: u64) -> Result<(usize, u128), anyhow::Error> {
    let start_time = Instant::now();

    let node_pubkeys = load_node_pubkeys(db).await?;

    let history = StakeHistory::default();
    let mut by_voter: HashMap<Pubkey, u64> = HashMap::new();
    let mut stream = db
        .stream(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            LATEST_BY_OWNER_SQL,
            [STAKE_PROGRAM_ID.to_bytes().to_vec().into()],
        ))
        .await?;
    while let Some(row) = stream.try_next().await? {
        let data: Vec<u8> = row.try_get("", "data")?;
        let Ok(state) = bincode::deserialize::<StakeStateV2>(&data) else {
            continue;
        };
        let Some(delegation) = state.delegation() else {
            continue;
        };
        // stake_v2() is gated on upgrade_bpf_stake_program_to_v5_1, which is not active on
        // mainnet; agave still takes this path, so match it.
        #[allow(deprecated)]
        let effective = delegation.stake(epoch, &history, NEW_RATE_ACTIVATION_EPOCH);
        if effective > 0 {
            *by_voter.entry(delegation.voter_pubkey).or_default() += effective;
        }
    }
    drop(stream);

    let total: u128 = by_voter.values().map(|&s| s as u128).sum();
    let voters = by_voter
        .into_iter()
        .map(|(vote_pubkey, activated_stake)| VoterStakeRow {
            vote_pubkey,
            node_pubkey: node_pubkeys.get(&vote_pubkey).copied().unwrap_or_default(),
            activated_stake,
            in_epoch_set: true,
        })
        .collect::<Vec<_>>();

    let count = voters.len();
    persist_epoch_stakes(db, &SnapshotStakeData { epoch, voters }).await?;

    tracing::debug!(
        target: "epoch_stakes_recomputer",
        "recompute for epoch {} took {:.3}s",
        epoch,
        start_time.elapsed().as_secs_f64()
    );
    Ok((count, total))
}

async fn load_node_pubkeys(
    db: &DatabaseConnection,
) -> Result<HashMap<Pubkey, Pubkey>, anyhow::Error> {
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            LATEST_BY_OWNER_SQL,
            [VOTE_PROGRAM_ID.to_bytes().to_vec().into()],
        ))
        .await?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        let pubkey_bytes: Vec<u8> = row.try_get("", "pubkey")?;
        let data: Vec<u8> = row.try_get("", "data")?;
        let Ok(vote_pubkey) = Pubkey::try_from(pubkey_bytes.as_slice()) else {
            continue;
        };
        if let Ok(vote_state) = VoteStateV4::deserialize(&data, &vote_pubkey) {
            map.insert(vote_pubkey, vote_state.node_pubkey);
        }
    }
    Ok(map)
}
