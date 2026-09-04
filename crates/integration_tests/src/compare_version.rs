// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use serde_json::Value as JsonValue;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "compare-version")]
#[command(about = "\
Compare getVersion between cloudbreak and a cluster RPC. Cloudbreak returns a \
composite `solana-core` string of the form `solana<X>-grpc<Y>-cloudbreak<Z>`, \
where <X> is the cluster's solana version. The cluster RPC returns just <X>. \
This test verifies that rpc1's `solana-core` contains `solana<rpc2_solana_core>` \
as a substring.")]
pub struct Args {
    /// Cloudbreak RPC endpoint URL (returns composite version)
    #[arg(long)]
    pub rpc1: String,

    /// Cluster RPC endpoint URL (returns plain solana version)
    #[arg(long)]
    pub rpc2: String,

    #[arg(long, default_value = "cloudbreak")]
    pub rpc1_name: String,

    #[arg(long, default_value = "cluster")]
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
        "method": "getVersion",
    });

    let (r1, r2) = tokio::join!(
        call(&client, &args.rpc1, &args.rpc1_name, &request),
        call(&client, &args.rpc2, &args.rpc2_name, &request),
    );
    let (v1, dur1) = r1?;
    let (v2, dur2) = r2?;

    println!("{:<12} {:>5}ms  {}", args.rpc1_name, dur1, v1);
    println!("{:<12} {:>5}ms  {}", args.rpc2_name, dur2, v2);

    let expected_substring = format!("solana{}", v2);
    if v1.contains(&expected_substring) {
        println!(
            "\nMATCH {} embeds cluster solana version {:?}",
            args.rpc1_name, v2
        );
        Ok(())
    } else {
        Err(anyhow!(
            "MISMATCH: {} returned {:?} which does not contain {:?}",
            args.rpc1_name,
            v1,
            expected_substring
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

    let solana_core = response
        .get("result")
        .and_then(|r| r.get("solana-core"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            anyhow!(
                "{} response had no string `result.solana-core`: {}",
                name,
                response
            )
        })?
        .to_string();
    Ok((solana_core, duration))
}
