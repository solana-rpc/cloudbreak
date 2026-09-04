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

#[derive(Parser, Debug)]
#[command(name = "compare-largest-accounts")]
#[command(about = "\
Validate getLargestAccounts against the same node's per-account getBalance. Public cluster \
RPCs disable getLargestAccounts, so there is no external oracle; instead we check structural \
invariants (count, descending order, distinct valid addresses) and confirm every reported \
lamport balance matches getBalance for that address (an independent read path).")]
pub struct Args {
    /// Cloudbreak RPC endpoint URL
    #[arg(long, default_value = "http://10.43.10.2:26722")]
    pub rpc: String,

    #[arg(long, default_value = "cloudbreak")]
    pub rpc_name: String,

    /// Commitment used for both getLargestAccounts and the getBalance oracle
    #[arg(long, default_value = "confirmed")]
    pub commitment: String,

    /// Expected number of returned accounts
    #[arg(long, default_value_t = 20)]
    pub expected: usize,

    /// Relative tolerance for benign lamport drift between the two reads
    #[arg(long, default_value_t = 0.0)]
    pub tolerance: f64,

    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

struct Row {
    address: String,
    lamports: u64,
}

pub async fn run(args: &Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let gla_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLargestAccounts",
        "params": [{"commitment": args.commitment}],
    });

    let (gla_json, dur, slot) = call(&client, &args.rpc, &args.rpc_name, &gla_req).await?;
    let rows = parse_rows(&gla_json)?;
    println!(
        "{:<12} {:>5}ms  {} accounts at slot {}",
        args.rpc_name,
        dur,
        rows.len(),
        slot
    );

    let mut failures: Vec<String> = Vec::new();

    if rows.len() != args.expected {
        failures.push(format!(
            "expected {} accounts, got {}",
            args.expected,
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
    }

    for pair in rows.windows(2) {
        if pair[0].lamports < pair[1].lamports {
            failures.push(format!(
                "not sorted descending: {} ({}) < {} ({})",
                pair[0].address, pair[0].lamports, pair[1].address, pair[1].lamports
            ));
        }
    }

    let mut exact = 0usize;
    let mut drifted = 0usize;
    for row in &rows {
        let balance = get_balance(
            &client,
            &args.rpc,
            &args.rpc_name,
            &row.address,
            &args.commitment,
        )
        .await
        .with_context(|| format!("getBalance {}", row.address))?;
        match balance {
            None => failures.push(format!("{} not found by getBalance", row.address)),
            Some(0) if row.lamports > 0 => failures.push(format!(
                "{} phantom: GLA={} balance=0",
                row.address, row.lamports
            )),
            Some(b) if b == row.lamports => exact += 1,
            Some(b) => {
                let rel = (b.abs_diff(row.lamports)) as f64 / row.lamports.max(1) as f64;
                if rel <= args.tolerance {
                    drifted += 1;
                } else {
                    failures.push(format!(
                        "{} mismatch: GLA={} getBalance={} (rel {:.4}%)",
                        row.address,
                        row.lamports,
                        b,
                        rel * 100.0
                    ));
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
        println!("PASS: getLargestAccounts consistent with getBalance");
        Ok(())
    } else {
        for f in failures.iter().take(40) {
            println!("  FAIL {f}");
        }
        Err(anyhow!("{} check(s) failed", failures.len()))
    }
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
            let lamports = item
                .get("lamports")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| anyhow!("missing lamports"))?;
            Ok(Row { address, lamports })
        })
        .collect()
}

async fn get_balance(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    address: &str,
    commitment: &str,
) -> Result<Option<u64>> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [address, {"commitment": commitment}],
    });
    let (json, _, _) = call(client, url, name, &req).await?;
    Ok(json
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.as_u64()))
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
