use cloudbreak_core::modules::largest_accounts::{
    CIRCULATING_SENTINEL_MINT, NON_CIRCULATING_SENTINEL_MINT, SOL_SENTINEL_MINT, fetch_record,
};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::{RpcLargestAccountsConfig, RpcLargestAccountsFilter};
use solana_rpc_client_api::response::{
    Response as RpcResponse, RpcAccountBalance, RpcResponseContext,
};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::resolve_commitment;
use crate::metrics;

/// Reads the persisted top-N record for `mint` through the core read path,
/// under the API query timeout. `Ok(None)` when no record exists.
pub async fn fetch_largest_record(
    state: &CloudbreakRpcState,
    mint: &Pubkey,
    latest_slot: u64,
) -> Result<Option<Vec<(Pubkey, u64)>>, RpcError> {
    let span = tracing::info_span!("get_largest_accounts_record_db");
    timeout(
        state.queries_timeout,
        fetch_record(&state.database, mint, latest_slot).instrument(span),
    )
    .await
    .map_err(|_elapsed| {
        tracing::error!("largest accounts record query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })
}

#[tracing::instrument(name = "get_largest_accounts_rpc", skip_all)]
pub async fn get_largest_accounts(
    state: &CloudbreakRpcState,
    config: Option<RpcLargestAccountsConfig>,
) -> Result<RpcResponse<Vec<RpcAccountBalance>>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getLargestAccounts");

    let config = config.unwrap_or_default();
    let sentinel = match &config.filter {
        None => SOL_SENTINEL_MINT,
        Some(RpcLargestAccountsFilter::Circulating) => CIRCULATING_SENTINEL_MINT,
        Some(RpcLargestAccountsFilter::NonCirculating) => NON_CIRCULATING_SENTINEL_MINT,
    };

    let commitment = config
        .commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, _block_time) = state.latest_slot_and_block_time(commitment).await?;

    let Some(rows) = fetch_largest_record(state, &sentinel, latest_slot).await? else {
        tracing::warn!("getLargestAccounts has no record at slot {}", latest_slot);
        return Err(state.node_unhealthy());
    };

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: rows
            .into_iter()
            .map(|(pubkey, lamports)| RpcAccountBalance {
                address: pubkey.to_string(),
                lamports,
            })
            .collect(),
    })
}
