// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::config::RpcEndpoint;
use anyhow::{Context, Result};
use clap::Parser;
use futures::future::join_all;
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Parser, Debug)]
#[command(name = "compare_accounts_by_mint")]
#[command(about = "Compare Solana accounts-by-mint from two RPC endpoints")]
pub struct Args {
    /// Path to the TOML configuration file
    #[arg(short, long, default_value = "cloudbreak.integration_tests.toml")]
    pub config: String,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// First (tested) RPC endpoint — called with `getTokenAccountsByMint`
    pub rpc1: RpcEndpoint,

    /// Second (healthy) RPC endpoint — called with `getProgramAccounts` + mint memcmp
    pub rpc2: RpcEndpoint,

    /// The mints to fetch accounts for
    pub mints: Vec<String>,

    /// Token program for `getProgramAccounts` on rpc2 and for `programId` on rpc1
    #[serde(default = "default_program_id")]
    pub program_id: String,

    /// RPC URL for the 'getSignaturesForAddress' (gsfa) check
    pub gsfa_rpc_url: String,

    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Number of parallel workers for checking transaction history
    #[serde(default = "default_workers")]
    pub workers: usize,

    pub slot: u64,

    pub db_url: String,
}

fn default_timeout() -> u64 {
    300
}

fn default_workers() -> usize {
    4
}

fn default_program_id() -> String {
    DEFAULT_TOKEN_PROGRAM.to_string()
}

#[derive(Serialize)]
struct RpcRequest<T> {
    jsonrpc: &'static str,
    id: &'static str,
    method: &'static str,
    params: T,
}

#[derive(Deserialize, Debug)]
struct RpcResponse<T> {
    result: Option<T>,
}

#[derive(Deserialize, Debug)]
struct AccountInfo {
    pubkey: String,
}

#[derive(Serialize)]
struct DataSlice {
    offset: u64,
    length: u64,
}

#[derive(Serialize)]
struct AccountsByMintParams {
    encoding: &'static str,
    commitment: &'static str,
    #[serde(rename = "dataSlice")]
    data_slice: DataSlice,
    #[serde(rename = "programId")]
    program_id: String,
}

#[derive(Serialize)]
struct Memcmp {
    offset: u64,
    bytes: String,
}

#[derive(Serialize)]
struct MemcmpFilter {
    memcmp: Memcmp,
}

#[derive(Serialize)]
struct GpaParams {
    encoding: &'static str,
    commitment: &'static str,
    #[serde(rename = "dataSlice")]
    data_slice: DataSlice,
    filters: Vec<MemcmpFilter>,
}

#[derive(Deserialize, Debug)]
struct SignatureInfo {
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
    slot: Option<u64>,
}

#[derive(Serialize)]
struct GsfaParams {
    limit: u64,
}

async fn get_pubkeys_from_cloudbreak(
    client: &reqwest::Client,
    rpc_url: &str,
    mint: &str,
    program_id: &str,
) -> Result<Vec<String>> {
    println!(
        "   -> Connecting to {} for mint {} (getTokenAccountsByMint)...",
        rpc_url, mint
    );

    let params = (
        mint,
        AccountsByMintParams {
            encoding: "base64",
            commitment: "confirmed",
            data_slice: DataSlice {
                offset: 0,
                length: 0,
            },
            program_id: program_id.to_string(),
        },
    );

    let request = RpcRequest {
        jsonrpc: "2.0",
        id: "test",
        method: "getTokenAccountsByMint",
        params,
    };

    fetch_pubkeys(client, rpc_url, &request).await
}

async fn get_pubkeys_from_source(
    client: &reqwest::Client,
    rpc_url: &str,
    mint: &str,
    program_id: &str,
) -> Result<Vec<String>> {
    println!(
        "   -> Connecting to {} for mint {} (getProgramAccounts + memcmp)...",
        rpc_url, mint
    );

    let params = (
        program_id,
        GpaParams {
            encoding: "base64",
            commitment: "confirmed",
            data_slice: DataSlice {
                offset: 0,
                length: 0,
            },
            filters: vec![MemcmpFilter {
                memcmp: Memcmp {
                    offset: 0,
                    bytes: mint.to_string(),
                },
            }],
        },
    );

    let request = RpcRequest {
        jsonrpc: "2.0",
        id: "test",
        method: "getProgramAccounts",
        params,
    };

    fetch_pubkeys(client, rpc_url, &request).await
}

async fn fetch_pubkeys<T: Serialize>(
    client: &reqwest::Client,
    rpc_url: &str,
    request: &RpcRequest<T>,
) -> Result<Vec<String>> {
    let response = client
        .post(rpc_url)
        .json(request)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", rpc_url))?;

    println!(
        "   -> ✅ Connected. Downloading response from {}...",
        rpc_url
    );

    let bytes = response.bytes().await?;
    println!(
        "   -> ⚙️ Download complete ({:.2} MB). Parsing JSON...",
        bytes.len() as f64 / 1e6
    );

    let rpc_response: RpcResponse<Vec<AccountInfo>> = serde_json::from_slice(&bytes)
        .with_context(|| format!("Failed to parse response from {}", rpc_url))?;

    let accounts = rpc_response.result.unwrap_or_default();
    let mut pubkeys: Vec<String> = accounts.into_iter().map(|a| a.pubkey).collect();
    pubkeys.sort();

    println!(
        "   -> ✨ Finished. Found {} pubkeys from {}.",
        pubkeys.len(),
        rpc_url
    );

    Ok(pubkeys)
}

async fn get_last_tx_time(
    client: &reqwest::Client,
    gsfa_rpc_url: &str,
    pubkey: &str,
) -> Result<(String, i64, u64)> {
    let params = (pubkey, GsfaParams { limit: 1 });

    let request = RpcRequest {
        jsonrpc: "2.0",
        id: "1",
        method: "getSignaturesForAddress",
        params,
    };

    let result = async {
        let response = client.post(gsfa_rpc_url).json(&request).send().await?;
        let rpc_response: RpcResponse<Vec<SignatureInfo>> = response.json().await?;
        Ok::<_, anyhow::Error>(rpc_response.result)
    }
    .await;

    match result {
        Ok(Some(signatures)) if !signatures.is_empty() => {
            let sig = &signatures[0];
            match (sig.block_time, sig.slot) {
                (Some(block_time), Some(slot)) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    let minutes = (now - block_time) / 60;

                    Ok((pubkey.to_string(), minutes, slot))
                }
                _ => Err(anyhow::anyhow!(
                    "No blockTime or slot found for pubkey: {}",
                    pubkey
                )),
            }
        }
        Ok(_) => Err(anyhow::anyhow!(
            "No transactions found in history for pubkey: {}",
            pubkey
        )),
        Err(_) => Err(anyhow::anyhow!(
            "Error checking transaction history for pubkey: {}",
            pubkey
        )),
    }
}

async fn check_activity_batch(
    client: &reqwest::Client,
    gsfa_rpc_url: &str,
    pubkeys: &[String],
    workers: usize,
) -> Vec<(String, i64, u64)> {
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(workers));

    let futures: Vec<_> = pubkeys
        .iter()
        .map(|pubkey| {
            let client = client.clone();
            let gsfa_rpc_url = gsfa_rpc_url.to_string();
            let pubkey = pubkey.clone();
            let semaphore = semaphore.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();
                get_last_tx_time(&client, &gsfa_rpc_url, &pubkey).await
            }
        })
        .collect();

    join_all(futures)
        .await
        .into_iter()
        .map(|result| result.unwrap())
        .collect()
}

pub async fn run(args: &Args) -> Result<()> {
    let config_content = std::fs::read_to_string(&args.config)
        .with_context(|| format!("Failed to read config file: {}", args.config))?;

    let config: Config = toml::from_str(&config_content)
        .with_context(|| format!("Failed to parse config file: {}", args.config))?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.timeout))
        .build()?;

    for mint in &config.mints {
        println!("--- 📂 Step 1: Downloading Public Keys in Parallel ---");

        let (pubkeys1, pubkeys2) = tokio::join!(
            get_pubkeys_from_cloudbreak(&client, &config.rpc1.url, mint, &config.program_id),
            get_pubkeys_from_source(&client, &config.rpc2.url, mint, &config.program_id),
        );

        let pubkeys1 = pubkeys1.context("Failed to get pubkeys from RPC 1")?;
        let pubkeys2 = pubkeys2.context("Failed to get pubkeys from RPC 2")?;

        println!("\n--- 📊 Step 2: Comparing Results ---");

        let set1: HashSet<_> = pubkeys1.iter().collect();
        let set2: HashSet<_> = pubkeys2.iter().collect();

        let mut only_in_rpc1: Vec<String> = set1.difference(&set2).map(|s| (*s).clone()).collect();
        only_in_rpc1.sort();

        let previous_only_in_rpc1: Vec<String> = std::fs::read_to_string(
            "crates/integration_tests/compare_responses_results/only_in_rpc1_mint.json",
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

        let current_set: HashSet<_> = only_in_rpc1.iter().collect();
        let previous_set: HashSet<_> = previous_only_in_rpc1.iter().collect();

        let only_new: Vec<_> = current_set.difference(&previous_set).collect();
        let only_old: Vec<_> = previous_set.difference(&current_set).collect();

        println!("Found {} keys only in current run (new).", only_new.len());
        println!("{}", "-".repeat(20));
        println!("{:?}", only_new);
        println!(
            "Found {} keys only in previous run (disappeared).",
            only_old.len()
        );
        println!("{}", "-".repeat(20));
        println!("{:?}", only_old);

        std::fs::create_dir_all("crates/integration_tests/compare_responses_results")?;
        std::fs::write(
            "crates/integration_tests/compare_responses_results/only_in_rpc1_mint.json",
            serde_json::to_string_pretty(&only_in_rpc1)?,
        )?;

        let mut only_in_rpc2: Vec<String> = set2.difference(&set1).map(|s| (*s).clone()).collect();
        only_in_rpc2.sort();

        println!("RPC 1 ({}):\t{} pubkeys", config.rpc1.name, pubkeys1.len());
        println!("RPC 2 ({}):\t{} pubkeys", config.rpc2.name, pubkeys2.len());
        println!("{}", "-".repeat(20));
        println!("Found {} keys only in RPC 1.", only_in_rpc1.len());
        println!("Found {} keys only in RPC 2.", only_in_rpc2.len());

        if only_in_rpc1.is_empty() && only_in_rpc2.is_empty() {
            println!("\n✅ All pubkeys match. No differences found!");
            continue;
        }

        if !only_in_rpc1.is_empty() {
            println!(
                "\n--- ⏱️ Step 3: Checking Activity for {} Keys ONLY in RPC 1 (using {} workers) ---",
                only_in_rpc1.len(),
                config.workers
            );

            let results =
                check_activity_batch(&client, &config.gsfa_rpc_url, &only_in_rpc1, config.workers)
                    .await;

            let db_results = find_keys_in_db(&config.db_url, &only_in_rpc1, "accounts").await?;
            let db_snapshot_results =
                find_keys_in_db(&config.db_url, &only_in_rpc1, "snapshot_accounts").await?;

            for (pubkey, minutes, slot) in results {
                let is_grpc = slot > config.slot;

                let db_data = db_results.iter().find(|(pk, _, _)| pk == &pubkey);
                let db_snapshot_data = db_snapshot_results.iter().find(|(pk, _, _)| pk == &pubkey);

                let mut log_message = format!(
                    "{} -> Last activity: {} minutes ago. Slot: {} - GRPC covers it: {}",
                    pubkey, minutes, slot, is_grpc
                );

                if let Some(db_data) = db_data {
                    log_message += &format!(" - DB: Slot: {} - Lamports: {}", db_data.1, db_data.2);
                }
                if let Some(db_snapshot_data) = db_snapshot_data {
                    log_message += &format!(
                        " - Snapshot DB: Slot: {} - Lamports: {}",
                        db_snapshot_data.1, db_snapshot_data.2
                    );
                }

                println!("{}", log_message);
            }
        }

        if !only_in_rpc2.is_empty() {
            println!(
                "\n--- ⏱️ Step 4: Checking Activity for {} Keys ONLY in RPC 2 (using {} workers) ---",
                only_in_rpc2.len(),
                config.workers
            );

            let results =
                check_activity_batch(&client, &config.gsfa_rpc_url, &only_in_rpc2, config.workers)
                    .await;
            for result in results {
                println!("{:?}", result);
            }
        }
    }

    Ok(())
}

async fn find_keys_in_db(
    db_url: &str,
    pubkeys: &[String],
    table_name: &str,
) -> Result<Vec<(String, u64, i64)>> {
    if pubkeys.is_empty() {
        return Ok(vec![]);
    }

    let db = Database::connect(db_url)
        .await
        .context("Failed to connect to database")?;

    let pubkey_literals: Vec<String> = pubkeys
        .iter()
        .filter_map(|pk| bs58::decode(pk).into_vec().ok())
        .map(|bytes| format!("'\\x{}'::bytea", hex::encode(&bytes)))
        .collect();

    let sql = format!(
        "SELECT DISTINCT pubkey, slot, lamports FROM {} WHERE pubkey IN ({})",
        table_name,
        pubkey_literals.join(", ")
    );

    let stmt = Statement::from_string(DbBackend::Postgres, sql);
    let rows = db
        .query_all(stmt)
        .await
        .context("Failed to query accounts")?;

    let found: Vec<(String, u64, i64)> = rows
        .iter()
        .filter_map(|row| {
            let pubkey: Vec<u8> = row.try_get_by_index(0).ok()?;
            let slot: i64 = row.try_get_by_index(1).ok()?;
            let lamports: i64 = row.try_get_by_index(2).ok()?;

            Some((bs58::encode(&pubkey).into_string(), slot as u64, lamports))
        })
        .collect();

    Ok(found)
}
