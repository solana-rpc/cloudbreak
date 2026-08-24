use cloudbreak_core::VOTE_PROGRAM_ID;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde::Deserialize;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{RpcVoteAccountInfo, RpcVoteAccountStatus};
use solana_vote_interface::state::VoteStateV4;

use crate::{
    error::RpcError, http::CloudbreakRpcState, modules::vote_accounts_cache::StakesSnapshot,
};

/// Default per Solana docs and Agave's `DELINQUENT_VALIDATOR_SLOT_DISTANCE`.
const DEFAULT_DELINQUENT_SLOT_DISTANCE: u64 = 128;

/// Up to 5 most recent epochs of credits, matching Agave's
/// `MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY`.
const MAX_EPOCH_CREDITS_HISTORY: usize = 5;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVoteAccountsConfig {
    pub commitment: Option<CommitmentLevel>,
    pub vote_pubkey: Option<String>,
    pub keep_unstaked_delinquents: Option<bool>,
    pub delinquent_slot_distance: Option<u64>,
}

pub async fn get_vote_accounts(
    state: &CloudbreakRpcState,
    config: Option<GetVoteAccountsConfig>,
) -> Result<RpcVoteAccountStatus, RpcError> {
    if !state.vote_accounts_supported {
        return Err(RpcError::InvalidParamsWithMessage(
            "getVoteAccounts is not supported on this node".to_string(),
        ));
    }

    let config = config.unwrap_or_default();

    let optional_filter = config
        .vote_pubkey
        .as_deref()
        .map(|s| {
            s.parse::<Pubkey>()
                .map_err(|e| RpcError::InvalidParamsWithMessage(format!("invalid votePubkey: {e}")))
        })
        .transpose()?;
    let keep_unstaked_delinquents = config.keep_unstaked_delinquents.unwrap_or(false);
    let delinquent_slot_distance = config
        .delinquent_slot_distance
        .unwrap_or(DEFAULT_DELINQUENT_SLOT_DISTANCE);

    let stakes = state.stakes_cache.read().unwrap().clone();
    if stakes.voters.is_empty() {
        // The indexer has not finished its first snapshot pass with stake data yet
        tracing::warn!(
            target: "get_vote_accounts",
            "stakes cache not yet populated; indexer has not finished a snapshot pass with stake data"
        );
        return Err(state.node_unhealthy());
    }

    let finalized_slot = match state
        .slot_syncronizer_data
        .as_ref()
        .map(|d| d.read().unwrap().finalized_slot.slot)
    {
        Some(slot) if slot > 0 => slot,
        _ => crate::db_query::get_slot_data(&state.database)
            .await
            .map(|d| d.finalized_slot.slot)
            .unwrap_or(0),
    };

    let vote_accounts = load_vote_account_rows(state, optional_filter.as_ref()).await?;

    let (current, delinquent) = build_status(
        &vote_accounts,
        &stakes,
        finalized_slot,
        delinquent_slot_distance,
        keep_unstaked_delinquents,
    );

    Ok(RpcVoteAccountStatus {
        current,
        delinquent,
    })
}

struct VoteAccountRow {
    pubkey: Pubkey,
    data: Vec<u8>,
}

async fn load_vote_account_rows(
    state: &CloudbreakRpcState,
    optional_filter: Option<&Pubkey>,
) -> Result<Vec<VoteAccountRow>, RpcError> {
    let owner_bytes = VOTE_PROGRAM_ID.to_bytes().to_vec();

    let sql = if optional_filter.is_some() {
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (pubkey) pubkey, data, lamports
            FROM (
                SELECT pubkey, slot, data, lamports FROM accounts
                    WHERE owner = $1 AND pubkey = $2
                UNION ALL
                SELECT pubkey, slot, data, lamports FROM snapshot_accounts
                    WHERE owner = $1 AND pubkey = $2
            ) AS u
            ORDER BY pubkey ASC, slot DESC
        )
        SELECT pubkey, data FROM latest WHERE lamports > 0
        "#
    } else {
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (pubkey) pubkey, data, lamports
            FROM (
                SELECT pubkey, slot, data, lamports FROM accounts WHERE owner = $1
                UNION ALL
                SELECT pubkey, slot, data, lamports FROM snapshot_accounts WHERE owner = $1
            ) AS u
            ORDER BY pubkey ASC, slot DESC
        )
        SELECT pubkey, data FROM latest WHERE lamports > 0
        "#
    };

    let stmt = if let Some(filter) = optional_filter {
        Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            sql,
            [owner_bytes.into(), filter.to_bytes().to_vec().into()],
        )
    } else {
        Statement::from_sql_and_values(DatabaseBackend::Postgres, sql, [owner_bytes.into()])
    };

    let rows = state.database.query_all(stmt).await.map_err(|e| {
        tracing::error!(target: "get_vote_accounts", "vote account query failed: {e}");
        RpcError::InternalError
    })?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let pubkey_bytes: Vec<u8> = row.try_get("", "pubkey").map_err(|e| {
            tracing::error!(target: "get_vote_accounts", "pubkey decode failed: {e}");
            RpcError::InternalError
        })?;
        let data: Vec<u8> = row.try_get("", "data").map_err(|e| {
            tracing::error!(target: "get_vote_accounts", "data decode failed: {e}");
            RpcError::InternalError
        })?;
        let Ok(pubkey) = Pubkey::try_from(pubkey_bytes.as_slice()) else {
            continue;
        };
        out.push(VoteAccountRow { pubkey, data });
    }
    Ok(out)
}

fn build_status(
    rows: &[VoteAccountRow],
    stakes: &StakesSnapshot,
    finalized_slot: u64,
    delinquent_slot_distance: u64,
    keep_unstaked_delinquents: bool,
) -> (Vec<RpcVoteAccountInfo>, Vec<RpcVoteAccountInfo>) {
    let mut current = Vec::new();
    let mut delinquent = Vec::new();

    for row in rows {
        let Ok(vote_state) = VoteStateV4::deserialize(&row.data, &row.pubkey) else {
            tracing::debug!(
                target: "get_vote_accounts",
                "failed to deserialize vote state for {}",
                row.pubkey
            );
            continue;
        };

        let stake_entry = stakes.voters.get(&row.pubkey);
        let activated_stake = stake_entry.map(|e| e.activated_stake).unwrap_or(0);
        let in_epoch_set = stake_entry.map(|e| e.in_epoch_set).unwrap_or(false);
        let node_pubkey = stake_entry
            .map(|e| e.node_pubkey)
            .unwrap_or(vote_state.node_pubkey);

        let last_vote = vote_state
            .votes
            .back()
            .map(|v| v.lockout.slot())
            .unwrap_or(0);
        let root_slot = vote_state.root_slot.unwrap_or(0);
        let commission = (vote_state.inflation_rewards_commission_bps / 100) as u8;

        let epoch_credits = vote_state
            .epoch_credits
            .iter()
            .rev()
            .take(MAX_EPOCH_CREDITS_HISTORY)
            .rev()
            .copied()
            .collect();

        let info = RpcVoteAccountInfo {
            vote_pubkey: row.pubkey.to_string(),
            node_pubkey: node_pubkey.to_string(),
            activated_stake,
            commission,
            inflation_rewards_commission_bps: Some(vote_state.inflation_rewards_commission_bps),
            epoch_vote_account: in_epoch_set,
            epoch_credits,
            last_vote,
            root_slot,
        };

        // Mirror Agave: a validator is delinquent when its last vote is at least
        // `delinquent_slot_distance` slots behind the reference slot
        let is_delinquent = finalized_slot.saturating_sub(last_vote) >= delinquent_slot_distance;
        if is_delinquent {
            if keep_unstaked_delinquents || activated_stake > 0 {
                delinquent.push(info);
            }
        } else {
            current.push(info);
        }
    }

    (current, delinquent)
}
