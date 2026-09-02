use solana_commitment_config::CommitmentLevel;
use solana_rpc_client_api::{
    config::RpcSupplyConfig,
    response::{Response as RpcResponse, RpcResponseContext, RpcSupply},
};

use crate::{error::RpcError, http::CloudbreakRpcState, methods::resolve_commitment};

const MAX_SUPPLY_STALENESS_SLOTS: u64 = 150;

pub async fn get_supply(
    state: &CloudbreakRpcState,
    config: Option<RpcSupplyConfig>,
) -> Result<RpcResponse<RpcSupply>, RpcError> {
    let config = config.unwrap_or_default();
    let commitment = resolve_commitment(
        config
            .commitment
            .map(|c| c.commitment)
            .unwrap_or(CommitmentLevel::Finalized),
        state.processed_commitment,
    )?;

    let snapshot = state.supply_cache.read().unwrap().clone();
    let Some(accounts_list) = snapshot.non_circulating_accounts.as_ref() else {
        tracing::warn!(
            target: "get_supply",
            "non-circulating accounts not computed yet; returning node unhealthy"
        );
        return Err(state.node_unhealthy());
    };

    let finalized_slot = match state
        .slot_syncronizer_data
        .as_ref()
        .map(|d| d.read().unwrap().finalized_slot.slot)
    {
        Some(finalized) if finalized > 0 => finalized,
        _ => crate::db_query::get_slot_data(&state.database)
            .await
            .map(|d| d.finalized_slot.slot)
            .unwrap_or(0),
    };

    let row = match commitment {
        CommitmentLevel::Finalized => snapshot
            .rows
            .iter()
            .rev()
            .find(|row| row.slot <= finalized_slot),
        _ => snapshot.rows.last(),
    };
    let Some(row) = row.copied() else {
        tracing::warn!(
            target: "get_supply",
            "no cached supply row for {:?} commitment; returning node unhealthy",
            commitment
        );
        return Err(state.node_unhealthy());
    };
    let Some(non_circulating) = row.non_circulating else {
        tracing::warn!(
            target: "get_supply",
            "supply row at slot {} has no non-circulating lamports yet; returning node unhealthy",
            row.slot
        );
        return Err(state.node_unhealthy());
    };
    if finalized_slot.saturating_sub(row.slot) > MAX_SUPPLY_STALENESS_SLOTS {
        tracing::warn!(
            target: "get_supply",
            "supply slot {} is more than {} slots behind finalized slot {}; returning node unhealthy",
            row.slot,
            MAX_SUPPLY_STALENESS_SLOTS,
            finalized_slot
        );
        return Err(state.node_unhealthy());
    }

    let non_circulating_accounts = if config.exclude_non_circulating_accounts_list {
        Vec::new()
    } else {
        accounts_list.clone()
    };

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: row.slot,
            api_version: None,
        },
        value: RpcSupply {
            total: row.total,
            circulating: row.total.saturating_sub(non_circulating),
            non_circulating,
            non_circulating_accounts,
        },
    })
}
