use cloudbreak_core::modules::largest_accounts::{
    decode_record, CIRCULATING_SENTINEL_MINT, NON_CIRCULATING_SENTINEL_MINT, SOL_SENTINEL_MINT,
};
use sea_orm::sqlx::{self, Row};
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
use crate::{db_query, metrics};

pub async fn fetch_largest_record(
    state: &CloudbreakRpcState,
    mint: &Pubkey,
    latest_slot: u64,
) -> Result<Vec<(Pubkey, u64)>, RpcError> {
    let sql_template = include_str!("../db/getLargestAccountsRecord.sql");
    let mint_hex = format!("'\\x{}'::bytea", hex::encode(mint.as_ref()));
    let sql = sql_template.replace("$1", &mint_hex);
    let sql = sql.replace("$2", &latest_slot.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_largest_accounts_record_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("largest accounts record query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let Some(row) = rows.first() else {
        return Ok(Vec::new());
    };
    let record_bytes: Vec<u8> = row.get("record");
    decode_record(&record_bytes).ok_or(RpcError::InternalError)
}

#[tracing::instrument(name = "get_largest_accounts_rpc", skip_all)]
pub async fn get_largest_accounts(
    state: &CloudbreakRpcState,
    config: Option<RpcLargestAccountsConfig>,
) -> Result<RpcResponse<Vec<RpcAccountBalance>>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getLargestAccounts");

    if state.largest_accounts_mints.is_none() {
        return Err(RpcError::InvalidParamsWithMessage(
            "getLargestAccounts is not supported on this node".to_string(),
        ));
    }

    let config = config.unwrap_or_default();
    let sentinel = match &config.filter {
        None => SOL_SENTINEL_MINT,
        Some(filter) => {
            if !state.supply_supported {
                return Err(RpcError::InvalidParamsWithMessage(
                    "getLargestAccounts filter is not supported on this node".to_string(),
                ));
            }
            match filter {
                RpcLargestAccountsFilter::Circulating => CIRCULATING_SENTINEL_MINT,
                RpcLargestAccountsFilter::NonCirculating => NON_CIRCULATING_SENTINEL_MINT,
            }
        }
    };

    let commitment = config
        .commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, _block_time) = state.latest_slot_and_block_time(commitment).await?;

    let rows = fetch_largest_record(state, &sentinel, latest_slot).await?;
    if rows.is_empty() {
        tracing::error!("getLargestAccounts has no record at slot {}", latest_slot);
        return Err(RpcError::InternalError);
    }

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
