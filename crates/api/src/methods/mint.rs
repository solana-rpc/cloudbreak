// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::error::RpcError;
use crate::methods::is_token_program;
use sea_orm::sqlx::Row;
use sea_orm::{DatabaseConnection, sqlx};
use cloudbreak_core::modules::rpc_filter_type::{RpcFilterType, RpcProgramAccountsConfig};
use solana_pubkey::Pubkey;
use std::time::Duration;
use tokio::time::{Instant, timeout};

/// Check if there is at least one memcmp that allows to use tokenowner or tokenmint DB indexes or Error otherwise
/// Returns a boolean indicating if the filters match for a mint filter
pub fn check_filters_are_valid_for_token_query(
    program: Pubkey,
    config: RpcProgramAccountsConfig,
) -> Result<Option<Vec<u8>>, RpcError> {
    if !is_token_program(&program) {
        return Err(RpcError::InternalError);
    }

    let filters = config.filters.as_ref().ok_or(RpcError::InvalidParams)?;

    let mut valid_filters = false;
    let mut mint_pubkey = None;

    for filter in filters {
        if let RpcFilterType::Memcmp(memcmp) = filter {
            let offset = memcmp.offset();
            let bytes = memcmp.bytes().ok_or(RpcError::InvalidParams)?;

            let is_token_filter = offset == 32 && bytes.len() == 32;
            let is_mint_filter = offset == 0 && bytes.len() == 32;

            if is_mint_filter {
                mint_pubkey = Some(bytes.to_vec());
            }

            if is_token_filter || is_mint_filter {
                valid_filters = true;
                break;
            }
        }
    }

    if !valid_filters {
        return Err(RpcError::InvalidParams);
    }

    Ok(mint_pubkey)
}

/// gets mint data from the database (it adds the ability to merge mint data for jsonParsed
///  encoding for gPA tokenmint queries)
#[tracing::instrument(name = "mint_data", skip_all)]
pub async fn get_mint(
    token_program: Pubkey,
    mint: Vec<u8>,
    slot: u64,
    db: &DatabaseConnection,
    queries_timeout: Duration,
) -> Option<Vec<u8>> {
    let start_time = Instant::now();
    let mint_hex = format!("'\\x{}'::bytea", hex::encode(&mint));
    let token_program_hex = format!("'\\x{}'::bytea", hex::encode(token_program.to_bytes()));

    let pool = db.get_postgres_connection_pool();

    let sql = include_str!("../db/getMintDataWithProgram.sql").replace("$1", &mint_hex);
    let sql = sql.replace("$2", slot.to_string().as_str());
    let sql = sql.replace("$3", &token_program_hex);

    let rows = timeout(queries_timeout, async {
        sqlx::raw_sql(&sql)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                tracing::error!("Database query error: {}", e);
                RpcError::InternalError
            })
            .ok()
    })
    .await
    .unwrap_or_else(|elapsed| {
        tracing::error!("Database query error: {}", elapsed);
        None
    })?;

    let row = rows.first()?;

    let mint_data: Vec<u8> = row.get::<Vec<u8>, _>("data");

    tracing::debug!(
        target: "gpa_mint_data",
        "Mint filter query duration: {:?} microseconds - mint data length: {}",
        start_time.elapsed().as_micros(),
        mint_data.len()
    );

    Some(mint_data)
}
