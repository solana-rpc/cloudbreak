// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use serde_json::Value as JsonValue;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "compare-genesis-hash")]
#[command(about = "Compare getGenesisHash between two RPC endpoints (expects exact equality)")]
pub struct Args {
    /// First RPC endpoint URL (typically the cloudbreak API)
    #[arg(long)]
    pub rpc1: String,

    /// Second RPC endpoint URL (typically the cluster RPC, source of truth)
    #[arg(long)]
    pub rpc2: String,

    /// Display name for rpc1
    #[arg(long, default_value = "rpc1")]
    pub rpc1_name: String,

    /// Display name for rpc2
    #[arg(long, default_value = "rpc2")]
    pub rpc2_name: String,

    /// Request timeout in seconds
    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

pub async fn run(args: &Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getGenesisHash",
    });

    let (r1, r2) = tokio::join!(
        call(&client, &args.rpc1, &args.rpc1_name, &request),
        call(&client, &args.rpc2, &args.rpc2_name, &request),
    );
    let (hash1, dur1) = r1?;
    let (hash2, dur2) = r2?;

    println!("{:<12} {:>5}ms  {}", args.rpc1_name, dur1, hash1);
    println!("{:<12} {:>5}ms  {}", args.rpc2_name, dur2, hash2);

    if hash1 == hash2 {
        println!("\nMATCH genesis hash is identical");
        Ok(())
    } else {
        Err(anyhow!(
            "MISMATCH: {} returned {:?}, {} returned {:?}",
            args.rpc1_name,
            hash1,
            args.rpc2_name,
            hash2
        ))
    }
}

async fn call(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    request: &JsonValue,
) -> Result<(String, u128)> {
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
    let duration = start.elapsed().as_millis();

    if let Some(err) = response.get("error") {
        return Err(anyhow!(
            "{} returned RPC error: {}",
            name,
            serde_json::to_string(err).unwrap_or_default()
        ));
    }

    let hash = response
        .get("result")
        .and_then(|r| r.as_str())
        .ok_or_else(|| anyhow!("{} response had no string `result`: {}", name, response))?
        .to_string();
    Ok((hash, duration))
}
