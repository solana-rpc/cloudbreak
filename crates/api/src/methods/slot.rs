// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    http::{CloudbreakApiResponse, CloudbreakRpcState},
    methods::processed,
};
use sea_orm::EntityTrait;
use serde::{Deserialize, Serialize};
use solana_commitment_config::CommitmentConfig;
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

    let min_context_slot = config.as_ref().and_then(|c| c.min_context_slot);
    let read = processed::read(state, config.as_ref().and_then(|c| c.commitment), "getSlot")?;

    if let Some(blocks) = &read.blocks {
        if let Some(min_slot) = min_context_slot
            && blocks.slot < min_slot
        {
            return Err(RpcError::MinContextSlotNotReached { context_slot: blocks.slot });
        }
        return Ok(CloudbreakApiResponse::Response(blocks.slot));
    }
    let commitment = read.commitment;

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
        if rpc_latest_slot.saturating_sub(cached_slot_data) == 1 {
            tracing::warn!(target: "slot_mismatch", "Slot mismatch: cached slot: {} - rpc latest slot: {} - commitment: {}", cached_slot_data, rpc_latest_slot, commitment);
        } else if rpc_latest_slot.saturating_sub(cached_slot_data) > 1 {
            tracing::error!(target: "slot_mismatch", "Slot mismatch: cached slot: {} - rpc latest slot: {} - commitment: {}", cached_slot_data, rpc_latest_slot, commitment);
        }
    }

    if let Some(min_slot) = min_context_slot
        && rpc_latest_slot < min_slot
    {
        return Err(RpcError::MinContextSlotNotReached { context_slot: rpc_latest_slot });
    }

    tracing::debug!("get_slot: {}µs", start_time.elapsed().as_micros());

    Ok(CloudbreakApiResponse::Response(rpc_latest_slot))
}
