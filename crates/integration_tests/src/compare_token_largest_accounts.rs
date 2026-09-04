// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use serde_json::{Value as JsonValue, json};
use solana_pubkey::Pubkey;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::{Duration, Instant};

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
/// USDC mint — high-activity, always populated.
const DEFAULT_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Parser, Debug)]
#[command(name = "compare-token-largest-accounts")]
#[command(about = "\
Validate getTokenLargestAccounts against the same node's per-account getAccountInfo. Public \
cluster RPCs disable token secondary indexes, so there is no external oracle; instead we check \
structural invariants (count, descending order, distinct valid addresses, consistent decimals) \
and confirm every reported holder is a token account of the requested mint whose parsed amount \
matches (an independent read path).")]
pub struct Args {
    /// Cloudbreak RPC endpoint URL
    #[arg(long, default_value = "http://10.43.10.2:26722")]
    pub rpc: String,

    #[arg(long, default_value = "cloudbreak")]
    pub rpc_name: String,

    /// Mint to query (default: USDC)
    #[arg(long, default_value = DEFAULT_MINT)]
    pub mint: String,

    /// Commitment used for both getTokenLargestAccounts and the getAccountInfo oracle
    #[arg(long, default_value = "confirmed")]
    pub commitment: String,

    /// Relative tolerance for benign amount drift between the two reads
    #[arg(long, default_value_t = 0.0)]
    pub tolerance: f64,

    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

struct Row {
    address: String,
    amount: u64,
    decimals: u64,
}

pub async fn run(args: &Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let gtla_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [args.mint, {"commitment": args.commitment}],
    });

    let (gtla_json, dur, slot) = call(&client, &args.rpc, &args.rpc_name, &gtla_req).await?;
    let rows = parse_rows(&gtla_json)?;
    println!(
        "{:<12} {:>5}ms  {} holders of {} at slot {}",
        args.rpc_name,
        dur,
        rows.len(),
        args.mint,
        slot
    );

    let mut failures: Vec<String> = Vec::new();

    if rows.is_empty() {
        failures.push("no holders returned".to_string());
    }
    if rows.len() > 20 {
        failures.push(format!(
            "returned {} holders, expected at most 20",
            rows.len()
        ));
    }

    let mut seen = HashSet::new();
    for row in &rows {
        if Pubkey::from_str(&row.address).is_err() {
            failures.push(format!("invalid address: {}", row.address));
        }
        if !seen.insert(row.address.clone()) {
            failures.push(format!("duplicate address: {}", row.address));
        }
        if row.decimals != rows[0].decimals {
            failures.push(format!(
                "inconsistent decimals: {} has {}, first has {}",
                row.address, row.decimals, rows[0].decimals
            ));
        }
    }

    for pair in rows.windows(2) {
        if pair[0].amount < pair[1].amount {
            failures.push(format!(
                "not sorted descending: {} ({}) < {} ({})",
                pair[0].address, pair[0].amount, pair[1].address, pair[1].amount
            ));
        }
    }

    let mut exact = 0usize;
    let mut drifted = 0usize;
    for row in &rows {
        let info = get_token_account(
            &client,
            &args.rpc,
            &args.rpc_name,
            &row.address,
            &args.commitment,
        )
        .await
        .with_context(|| format!("getAccountInfo {}", row.address))?;
        match info {
            None => failures.push(format!("{} not found by getAccountInfo", row.address)),
            Some(info) => {
                if info.mint != args.mint {
                    failures.push(format!(
                        "{} wrong mint: expected {} got {}",
                        row.address, args.mint, info.mint
                    ));
                }
                if info.owner != TOKEN_PROGRAM && info.owner != TOKEN_2022_PROGRAM {
                    failures.push(format!(
                        "{} not owned by a token program: {}",
                        row.address, info.owner
                    ));
                }
                if info.amount == row.amount {
                    exact += 1;
                } else {
                    let rel = (info.amount.abs_diff(row.amount)) as f64 / row.amount.max(1) as f64;
                    if rel <= args.tolerance {
                        drifted += 1;
                    } else {
                        failures.push(format!(
                            "{} amount mismatch: GTLA={} getAccountInfo={} (rel {:.4}%)",
                            row.address,
                            row.amount,
                            info.amount,
                            rel * 100.0
                        ));
                    }
                }
            }
        }
    }

    println!(
        "oracle: {} exact, {} within tolerance, {} failures",
        exact,
        drifted,
        failures.len()
    );

    if failures.is_empty() {
        println!("PASS: getTokenLargestAccounts consistent with getAccountInfo");
        Ok(())
    } else {
        for f in failures.iter().take(40) {
            println!("  FAIL {f}");
        }
        Err(anyhow!("{} check(s) failed", failures.len()))
    }
}

struct TokenAccount {
    mint: String,
    owner: String,
    amount: u64,
}

fn parse_rows(response: &JsonValue) -> Result<Vec<Row>> {
    if let Some(err) = response.get("error") {
        return Err(anyhow!(
            "RPC error: {}",
            serde_json::to_string(err).unwrap_or_default()
        ));
    }
    let array = response
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("result.value is not an array: {}", response))?;

    array
        .iter()
        .map(|item| {
            let address = item
                .get("address")
                .and_then(|a| a.as_str())
                .ok_or_else(|| anyhow!("missing address"))?
                .to_string();
            let amount = item
                .get("amount")
                .and_then(|a| a.as_str())
                .and_then(|a| a.parse::<u64>().ok())
                .ok_or_else(|| anyhow!("missing amount"))?;
            let decimals = item
                .get("decimals")
                .and_then(|d| d.as_u64())
                .ok_or_else(|| anyhow!("missing decimals"))?;
            Ok(Row {
                address,
                amount,
                decimals,
            })
        })
        .collect()
}

async fn get_token_account(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    address: &str,
    commitment: &str,
) -> Result<Option<TokenAccount>> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [address, {"encoding": "jsonParsed", "commitment": commitment}],
    });
    let (json, _, _) = call(client, url, name, &req).await?;
    let value = json.get("result").and_then(|r| r.get("value"));
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let owner = value
        .get("owner")
        .and_then(|o| o.as_str())
        .unwrap_or_default()
        .to_string();
    let info = value
        .get("data")
        .and_then(|d| d.get("parsed"))
        .and_then(|p| p.get("info"))
        .ok_or_else(|| anyhow!("account {} is not a parsed token account", address))?;
    let mint = info
        .get("mint")
        .and_then(|m| m.as_str())
        .ok_or_else(|| anyhow!("missing mint"))?
        .to_string();
    let amount = info
        .get("tokenAmount")
        .and_then(|t| t.get("amount"))
        .and_then(|a| a.as_str())
        .and_then(|a| a.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("missing tokenAmount.amount"))?;
    Ok(Some(TokenAccount {
        mint,
        owner,
        amount,
    }))
}

async fn call(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    request: &JsonValue,
) -> Result<(JsonValue, u128, u64)> {
    let start = Instant::now();
    let response: JsonValue = client
        .post(url)
        .header("x-subscription-id", "test-value")
        .json(request)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {} ({})", name, url))?
        .json()
        .await
        .with_context(|| format!("Failed to parse JSON from {} ({})", name, url))?;
    let slot = response
        .get("result")
        .and_then(|r| r.get("context"))
        .and_then(|c| c.get("slot"))
        .and_then(|s| s.as_u64())
        .unwrap_or_default();
    Ok((response, start.elapsed().as_millis(), slot))
}
