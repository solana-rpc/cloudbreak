// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::time::{Duration, Instant};

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
/// USDC mint — high-activity, always populated.
const DEFAULT_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Parser, Debug)]
#[command(name = "compare-accounts-by-mint-vs-cluster")]
#[command(about = "\
Smoke-check getTokenAccountsByMint against a cluster RPC. Public cluster RPCs disable \
getProgramAccounts on token programs, so a full comparison is impossible; instead \
we use getTokenLargestAccounts on the cluster as a small oracle and verify that \
every top holder it returns is also present in cloudbreak's getTokenAccountsByMint result.")]
pub struct Args {
    /// Cloudbreak RPC endpoint URL
    #[arg(long)]
    pub rpc1: String,

    /// Cluster RPC endpoint URL (used as oracle via getTokenLargestAccounts)
    #[arg(long)]
    pub rpc2: String,

    #[arg(long, default_value = "cloudbreak")]
    pub rpc1_name: String,

    #[arg(long, default_value = "cluster")]
    pub rpc2_name: String,

    /// Mint to query (default: USDC)
    #[arg(long, default_value = DEFAULT_MINT)]
    pub mint: String,

    /// Token program id (passed to getTokenAccountsByMint as programId)
    #[arg(long, default_value = TOKEN_PROGRAM)]
    pub program_id: String,

    /// Request timeout in seconds
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
}

pub async fn run(args: &Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let cloudbreak_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByMint",
        "params": [
            args.mint,
            {
                "encoding": "base64",
                "commitment": "confirmed",
                "dataSlice": {"offset": 0, "length": 0},
                "programId": args.program_id,
            }
        ],
    });

    let cluster_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [args.mint, {"commitment": "confirmed"}],
    });

    let (r1, r2) = tokio::join!(
        call(&client, &args.rpc1, &args.rpc1_name, &cloudbreak_req),
        call(&client, &args.rpc2, &args.rpc2_name, &cluster_req),
    );
    let (cloudbreak_json, dur1) = r1?;
    let (cluster_json, dur2) = r2?;

    let cloudbreak_pubkeys = extract_accounts_by_mint_pubkeys(&cloudbreak_json)
        .with_context(|| format!("Parsing {} getTokenAccountsByMint response", args.rpc1_name))?;
    let cluster_top: Vec<String> =
        extract_token_largest_addresses(&cluster_json).with_context(|| {
            format!(
                "Parsing {} getTokenLargestAccounts response",
                args.rpc2_name
            )
        })?;

    println!(
        "{:<12} {:>5}ms  {} pubkeys from getTokenAccountsByMint",
        args.rpc1_name,
        dur1,
        cloudbreak_pubkeys.len()
    );
    println!(
        "{:<12} {:>5}ms  {} top addresses from getTokenLargestAccounts",
        args.rpc2_name,
        dur2,
        cluster_top.len()
    );

    let missing: Vec<&String> = cluster_top
        .iter()
        .filter(|pk| !cloudbreak_pubkeys.contains(*pk))
        .collect();

    if missing.is_empty() {
        println!(
            "\nMATCH all {} cluster top-holder addresses are present in {} result",
            cluster_top.len(),
            args.rpc1_name
        );
        Ok(())
    } else {
        Err(anyhow!(
            "MISMATCH: {} of {} cluster top-holder addresses missing from {} result: {:?}",
            missing.len(),
            cluster_top.len(),
            args.rpc1_name,
            missing
        ))
    }
}

fn extract_accounts_by_mint_pubkeys(response: &JsonValue) -> Result<HashSet<String>> {
    if let Some(err) = response.get("error") {
        return Err(anyhow!(
            "RPC error: {}",
            serde_json::to_string(err).unwrap_or_default()
        ));
    }
    let array = response
        .get("result")
        .and_then(|r| r.get("value").or(Some(r)))
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("result is not an array or {{value: array}}: {}", response))?;

    Ok(array
        .iter()
        .filter_map(|item| {
            item.get("pubkey")
                .and_then(|p| p.as_str())
                .map(String::from)
        })
        .collect())
}

fn extract_token_largest_addresses(response: &JsonValue) -> Result<Vec<String>> {
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

    Ok(array
        .iter()
        .filter_map(|item| {
            item.get("address")
                .and_then(|a| a.as_str())
                .map(String::from)
        })
        .collect())
}

async fn call(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    request: &JsonValue,
) -> Result<(JsonValue, u128)> {
    let start = Instant::now();
    let response: JsonValue = client
        .post(url)
        .json(request)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {} ({})", name, url))?
        .json()
        .await
        .with_context(|| format!("Failed to parse JSON from {} ({})", name, url))?;
    Ok((response, start.elapsed().as_millis()))
}
