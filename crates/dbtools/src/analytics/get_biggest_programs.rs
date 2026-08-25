// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
use solana_pubkey::Pubkey;

const PROGRAMS_COUNT: usize = 3;
const ORDER: &str = "DESC";

pub async fn get_biggest_programs(database_url: &str) {
    let db = Database::connect(database_url)
        .await
        .expect("Failed to connect to database");

    get_biggest_programs_from_table(&db, "accounts").await;
    // get_biggest_programs_from_table(&db, "snapshot_accounts").await;

    println!("########################################################");
    println!("Getting biggest programs from snapshot accounts partitions");
    println!("########################################################");

    let partition_rows = db
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'snapshot_accounts_p%' ORDER BY tablename",
        ))
        .await
        .expect("Failed to query pg_tables for snapshot_accounts partitions");

    let mut owners = Vec::new();
    for row in partition_rows {
        let table: String = row.try_get("", "tablename").unwrap();
        let partition_owners = get_biggest_programs_from_table(&db, &table).await;
        if let Some(biggest_owner) = partition_owners.first() {
            owners.push(biggest_owner.to_string());
        }
    }

    // Save biggest owner by partition to json file
    let json = serde_json::to_string(&owners).unwrap();
    std::fs::write("biggest_owner_by_partition.json", json).unwrap();
}

async fn get_biggest_programs_from_table(db: &DatabaseConnection, table: &str) -> Vec<Pubkey> {
    let start_time = tokio::time::Instant::now();

    let rows = db
  .query_all(Statement::from_string(
      DbBackend::Postgres,
      format!("SELECT owner, COUNT(*) as account_count FROM {table} GROUP BY owner ORDER BY account_count {ORDER} LIMIT {PROGRAMS_COUNT}"),
  ))
  .await
  .expect("Failed to get programs");

    let end_time = start_time.elapsed().as_secs_f64();

    println!("########################################################");
    println!("Table: {table} (Time taken: {end_time} seconds)");
    println!("########################################################");
    let mut owners = Vec::new();
    for row in rows {
        let owner: Vec<u8> = row.try_get("", "owner").unwrap();
        let owner = Pubkey::try_from(owner.as_slice()).unwrap();
        let account_count: i64 = row.try_get("", "account_count").unwrap();

        println!(
            "owner: {}, account_count: {}",
            owner,
            colored_count(account_count)
        );
        owners.push(owner);
    }
    println!("########################################################\n\n");

    owners
}

fn colored_count(account_count: i64) -> String {
    let color = match account_count {
        c if c >= 100_000_000 => "\x1b[38;5;196m",
        c if c >= 30_000_000 => "\x1b[38;5;202m",
        c if c >= 10_000_000 => "\x1b[38;5;208m",
        c if c >= 5_000_000 => "\x1b[38;5;214m",
        _ => "",
    };
    format!("{}{}\x1b[0m", color, account_count)
}
