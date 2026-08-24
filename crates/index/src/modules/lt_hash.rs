// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result};
use cloudbreak_core::AccountSelectorConfig;
use cloudbreak_snapshot::lt_hash::lt_hash_account;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use solana_lattice_hash::lt_hash::LtHash;

fn build_owner_filter(programs: &AccountSelectorConfig) -> String {
    let program_owners: Vec<Vec<u8>> = programs
        .include
        .iter()
        .map(|p| p.0.to_bytes().to_vec())
        .collect();

    if !program_owners.is_empty() {
        let owner_literals: Vec<String> = program_owners
            .iter()
            .map(|bytes| format!("'\\x{}'::bytea", hex::encode(bytes)))
            .collect();
        format!("AND owner IN ({})", owner_literals.join(", "))
    } else {
        let exclude_owners: Vec<String> = programs
            .exclude
            .iter()
            .map(|p| format!("'\\x{}'::bytea", hex::encode(p.0.to_bytes())))
            .collect();
        if exclude_owners.is_empty() {
            String::new()
        } else {
            format!("AND owner NOT IN ({})", exclude_owners.join(", "))
        }
    }
}

async fn query_accounts_batch(
    db: &DatabaseConnection,
    target_slot: u64,
    owner_filter: &str,
    prefix: u16,
) -> Result<Vec<sea_orm::QueryResult>> {
    let lower = format!("'\\x{:04x}'::bytea", prefix);
    let upper = if prefix == 0xFFFF {
        String::new()
    } else {
        format!("AND pubkey < '\\x{:04x}'::bytea", prefix + 1)
    };

    let sql = format!(
        "SELECT DISTINCT ON (pubkey) pubkey, lamports, owner, executable, data
         FROM (
             SELECT pubkey, lamports, owner, executable, data, slot, write_version
             FROM accounts WHERE slot <= {slot} AND pubkey >= {lower} {upper} {owner_filter}
             UNION ALL
             SELECT pubkey, lamports, owner, executable, data, slot, write_version
             FROM snapshot_accounts WHERE slot <= {slot} AND pubkey >= {lower} {upper} {owner_filter}
         ) combined
         ORDER BY pubkey, slot DESC, write_version DESC",
        slot = target_slot,
        lower = lower,
        upper = upper,
        owner_filter = owner_filter,
    );

    // Use a transaction so SET LOCAL applies to the query
    let txn = sea_orm::TransactionTrait::begin(db)
        .await
        .context("Failed to begin transaction")?;
    txn.execute(Statement::from_string(
        DbBackend::Postgres,
        "SET LOCAL statement_timeout = '0'".to_string(),
    ))
    .await
    .context("Failed to set statement timeout")?;
    let result = txn
        .query_all(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .with_context(|| format!("Failed to query accounts batch prefix=0x{:04x}", prefix));
    txn.commit().await.context("Failed to commit transaction")?;
    result
}

pub async fn compute_db_lt_hash(
    db: &DatabaseConnection,
    target_slot: u64,
    programs: &AccountSelectorConfig,
) -> Result<(LtHash, usize)> {
    let start = std::time::Instant::now();
    let owner_filter = build_owner_filter(programs);
    let mut aggregate = LtHash::identity();
    let mut count = 0usize;

    for prefix in 0u32..=0xFFFF {
        let rows = query_accounts_batch(db, target_slot, &owner_filter, prefix as u16).await?;

        for row in &rows {
            let pubkey: Vec<u8> = row.try_get_by_index(0).context("Failed to read pubkey")?;
            let lamports: i64 = row.try_get_by_index(1).context("Failed to read lamports")?;
            let owner: Vec<u8> = row.try_get_by_index(2).context("Failed to read owner")?;
            let executable: bool = row
                .try_get_by_index(3)
                .context("Failed to read executable")?;
            let data: Vec<u8> = row.try_get_by_index(4).context("Failed to read data")?;

            if lamports <= 0 {
                continue;
            }

            let hash = lt_hash_account(lamports as u64, &data, executable, &owner, &pubkey);
            aggregate.mix_in(&hash);
            count += 1;
        }

        if (prefix + 1) % 4096 == 0 || prefix == 0xFFFF {
            tracing::info!(
                "LtHash progress: {}/65536 batches, {} accounts, {:.1}s elapsed",
                prefix + 1,
                count,
                start.elapsed().as_secs_f64()
            );
        }
    }

    Ok((aggregate, count))
}
