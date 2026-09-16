// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    http::{CloudbreakApiResponse, CloudbreakRpcState},
    methods::resolve_commitment,
};
use sea_orm::EntityTrait;
use serde::{Deserialize, Serialize};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use tokio::time::Instant;
use cloudbreak_entity::slots;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcGetSlotConfig {
    #[serde(flatten)]
    pub commitment: Option<CommitmentConfig>,
    pub min_context_slot: Option<u64>,
}

#[tracing::instrument(name = "getSlot", skip_all)]
pub async fn get_slot(
    state: &CloudbreakRpcState,
    config: Option<RpcGetSlotConfig>,
) -> Result<CloudbreakApiResponse<u64>, RpcError> {
    let start_time = Instant::now();

    let commitment = if let Some(commitment) = config.as_ref().and_then(|c| c.commitment) {
        resolve_commitment(commitment.commitment, state.processed_commitment)?
    } else {
        CommitmentLevel::Finalized
    };

    let slot_model = slots::Entity::find_by_id(commitment as i32)
        .one(&state.database)
        .await?;

    let rpc_latest_slot = slot_model.ok_or(RpcError::InternalError)?.slot as u64;

    let cached_slot_data = {
        state.slot_syncronizer_data.as_ref().and_then(|data| {
            let slot_syncronizer_data = data.read().ok()?;

            Some(slot_syncronizer_data.get_slot_for_commitment(commitment))
        })
    };

    if let Some(cached_slot_data) = cached_slot_data {
        if rpc_latest_slot - cached_slot_data == 1 {
            tracing::warn!(target: "slot_mismatch", "Slot mismatch: cached slot: {} - rpc latest slot: {} - commitment: {}", cached_slot_data, rpc_latest_slot, commitment);
        } else if rpc_latest_slot - cached_slot_data > 1 {
            tracing::error!(target: "slot_mismatch", "Slot mismatch: cached slot: {} - rpc latest slot: {} - commitment: {}", cached_slot_data, rpc_latest_slot, commitment);
        }
    }

    if let Some(min_slot) = config.as_ref().and_then(|c| c.min_context_slot)
        && rpc_latest_slot < min_slot
    {
        return Err(RpcError::MinContextSlotNotReached { context_slot: rpc_latest_slot });
    }

    tracing::debug!("get_slot: {}µs", start_time.elapsed().as_micros());

    Ok(CloudbreakApiResponse::Response(rpc_latest_slot))
}
