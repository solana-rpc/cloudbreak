// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    methods::CloudbreakDbResult,
    slot_syncronizer::{SlotData, SlotSyncronizerData},
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, QueryOrder, Statement};
use solana_pubkey::Pubkey;
use cloudbreak_core::QueryTrackerConfig;
use cloudbreak_entity::{service_health, slots};

use opentelemetry::trace::TraceContextExt;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// It will only check for [`QueryTrackerConfig::excluded_programs`] if [`QueryTrackerConfig::included_programs`]
/// is empty, if it is not empty, it will only check for `included_programs`.
/// (true if it's included or not excluded, false otherwise)
pub fn check_program_in_index_list(program: Pubkey, config: &QueryTrackerConfig) -> bool {
    if config.included_programs.is_empty() {
        !config.excluded_programs.iter().any(|p| p.0 == program)
    } else {
        config.included_programs.iter().any(|p| p.0 == program)
    }
}

pub async fn get_database_indexes(
    database_connection: DatabaseConnection,
) -> CloudbreakDbResult<Vec<String>> {
    let res = database_connection
        .query_all(Statement::from_string(
            database_connection.get_database_backend(),
            "SELECT indexname FROM pg_indexes WHERE tablename = 'accounts';",
        ))
        .await
        .map_err(RpcError::from)?;

    res.iter()
        .map(|row| row.try_get("", "indexname").map_err(RpcError::from))
        .collect::<Result<Vec<String>, RpcError>>()
        .map(|indexes| {
            indexes
                .into_iter()
                .filter(|name| name.starts_with("idx"))
                .collect()
        })
}

/// Gets the service health from the database
pub async fn get_service_health(db: &DatabaseConnection) -> bool {
    let res = service_health::Entity::find_by_id(1)
        .one(db)
        .await
        .unwrap_or(None);

    match res {
        Some(res) => res.healthy,
        None => false,
    }
}

pub async fn get_slot_data(db: &DatabaseConnection) -> Option<SlotSyncronizerData> {
    let res = slots::Entity::find()
        .order_by_asc(slots::Column::Commitment)
        .all(db)
        .await
        .unwrap_or_default();
    let mut res_iter = res.into_iter();

    let confirmed = res_iter.next()?;
    let finalized = res_iter.next()?;

    // Health is denormalised identically onto every slot row; read it from either.
    let healthy = confirmed.health;

    Some(SlotSyncronizerData {
        confirmed_slot: SlotData {
            slot: confirmed.slot as u64,
            block_time: confirmed.block_time,
        },
        finalized_slot: SlotData {
            slot: finalized.slot as u64,
            block_time: finalized.block_time,
        },
        healthy,
    })
}

/// # W3C traceparent format:
/// 00-00000000000000000000000000000123-0000000000000123-01
/// ^^                                                   ^^
/// version                                               trace-flags (sampled or not)
///    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^
///     trace-id (32 hex chars)         parent-id (16 hex chars)
///
pub fn add_trace_traceparent_to_query(sql: &str) -> String {
    let cx = tracing::Span::current().context();
    let sc = cx.span().span_context().clone();

    if sc.is_valid() && sc.is_sampled() {
        format!(
            "/*traceparent='00-{}-{}-01'*/ {sql}",
            sc.trace_id(),
            sc.span_id()
        )
    } else {
        sql.to_string()
    }
}
