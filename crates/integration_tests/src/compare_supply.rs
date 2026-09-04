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
#[command(name = "compare-supply")]
#[command(about = "\
Validate getSupply against the same node's getMultipleAccounts (an independent read path). \
Checks the structural invariants (circulating == total - nonCirculating; the account list is \
distinct and valid; excludeNonCirculatingAccountsList returns an empty list with the same \
numbers; context.slot is within the staleness bound of getSlot) and confirms the summed \
balances of the non-circulating accounts equal nonCirculating. A same-slot mismatch is a bug; a \
cross-slot drift is benign and reported with the slot gap.")]
pub struct Args {
    /// Cloudbreak RPC endpoint URL
    #[arg(long, default_value = "http://10.43.10.2:26722")]
    pub rpc: String,

    #[arg(long, default_value = "cloudbreak")]
    pub rpc_name: String,

    /// Commitment used for getSupply and the getMultipleAccounts oracle
    #[arg(long, default_value = "confirmed")]
    pub commitment: String,

    /// Max slots getSupply may lag getSlot before the staleness check fails
    #[arg(long, default_value_t = 150)]
    pub max_staleness: u64,

    /// Optional Agave upstream to compare `total` against at the same slot
    #[arg(long)]
    pub upstream: Option<String>,

    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

struct Supply {
    total: u64,
    circulating: u64,
    non_circulating: u64,
    accounts: Vec<String>,
    slot: u64,
}

pub async fn run(args: &Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let supply = get_supply(&client, args, false).await?;
    println!(
        "{:<12} slot {}  total {}  circulating {}  nonCirculating {}  {} accounts",
        args.rpc_name,
        supply.slot,
        supply.total,
        supply.circulating,
        supply.non_circulating,
        supply.accounts.len()
    );

    let mut failures: Vec<String> = Vec::new();

    // Structural: circulating == total - nonCirculating.
    if supply.circulating != supply.total.saturating_sub(supply.non_circulating) {
        failures.push(format!(
            "circulating {} != total {} - nonCirculating {}",
            supply.circulating, supply.total, supply.non_circulating
        ));
    }

    // The account list is distinct and every address is valid.
    let mut seen = HashSet::new();
    for address in &supply.accounts {
        if Pubkey::from_str(address).is_err() {
            failures.push(format!("invalid address: {address}"));
        }
        if !seen.insert(address.clone()) {
            failures.push(format!("duplicate address: {address}"));
        }
    }

    // excludeNonCirculatingAccountsList returns an empty list, same numbers.
    let excluded = get_supply(&client, args, true).await?;
    if !excluded.accounts.is_empty() {
        failures.push(format!(
            "excludeNonCirculatingAccountsList returned {} accounts, expected 0",
            excluded.accounts.len()
        ));
    }
    if excluded.slot == supply.slot
        && (excluded.total != supply.total || excluded.non_circulating != supply.non_circulating)
    {
        failures.push("excludeNonCirculatingAccountsList changed the numbers".to_string());
    }

    // Staleness: context.slot within the bound of getSlot.
    let head = get_slot(&client, args).await?;
    if head.saturating_sub(supply.slot) > args.max_staleness {
        failures.push(format!(
            "supply slot {} is {} slots behind getSlot {}",
            supply.slot,
            head.saturating_sub(supply.slot),
            head
        ));
    }

    // Independent read path: sum the non-circulating balances via
    // getMultipleAccounts and compare to nonCirculating.
    let (summed, summed_slot, missing) =
        sum_balances(&client, args, &supply.accounts).await?;
    if missing > 0 {
        failures.push(format!("{missing} non-circulating accounts not found by getMultipleAccounts"));
    }
    if summed == supply.non_circulating {
        println!("oracle: getMultipleAccounts sum matches nonCirculating exactly");
    } else if summed_slot == supply.slot {
        failures.push(format!(
            "same-slot mismatch: getMultipleAccounts sum {} != nonCirculating {}",
            summed, supply.non_circulating
        ));
    } else {
        println!(
            "oracle: cross-slot drift (benign): sum {} at slot {} vs nonCirculating {} at slot {} (gap {})",
            summed,
            summed_slot,
            supply.non_circulating,
            supply.slot,
            summed_slot.abs_diff(supply.slot)
        );
    }

    // Optional upstream: compare total at the same slot only.
    if let Some(upstream) = &args.upstream {
        let up = get_supply_from(&client, upstream, "upstream", &args.commitment, false, args).await?;
        if up.slot == supply.slot && up.total != supply.total {
            failures.push(format!(
                "same-slot total mismatch: cloudbreak {} vs upstream {}",
                supply.total, up.total
            ));
        } else {
            println!(
                "upstream: total {} at slot {} (cloudbreak {} at slot {})",
                up.total, up.slot, supply.total, supply.slot
            );
        }
    }

    if failures.is_empty() {
        println!("PASS: getSupply consistent and self-validating");
        Ok(())
    } else {
        for f in failures.iter().take(40) {
            println!("  FAIL {f}");
        }
        Err(anyhow!("{} check(s) failed", failures.len()))
    }
}

async fn get_supply(client: &reqwest::Client, args: &Args, exclude_list: bool) -> Result<Supply> {
    get_supply_from(client, &args.rpc, &args.rpc_name, &args.commitment, exclude_list, args).await
}

async fn get_supply_from(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    commitment: &str,
    exclude_list: bool,
    _args: &Args,
) -> Result<Supply> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSupply",
        "params": [{
            "commitment": commitment,
            "excludeNonCirculatingAccountsList": exclude_list,
        }],
    });
    let (json, _, slot) = call(client, url, name, &req).await?;
    if let Some(err) = json.get("error") {
        return Err(anyhow!("getSupply error from {name}: {err}"));
    }
    let value = json
        .get("result")
        .and_then(|r| r.get("value"))
        .ok_or_else(|| anyhow!("getSupply result.value missing: {json}"))?;
    let total = field_u64(value, "total")?;
    let circulating = field_u64(value, "circulating")?;
    let non_circulating = field_u64(value, "nonCirculating")?;
    let accounts = value
        .get("nonCirculatingAccounts")
        .and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Ok(Supply {
        total,
        circulating,
        non_circulating,
        accounts,
        slot,
    })
}

async fn get_slot(client: &reqwest::Client, args: &Args) -> Result<u64> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": args.commitment}],
    });
    let (json, _, _) = call(client, &args.rpc, &args.rpc_name, &req).await?;
    json.get("result")
        .and_then(|r| r.as_u64())
        .ok_or_else(|| anyhow!("getSlot returned no result: {json}"))
}

/// Sums balances of the non-circulating accounts via getMultipleAccounts in
/// chunks of 100. Returns the sum, the last observed context slot, and the count
/// of accounts the node did not find.
async fn sum_balances(
    client: &reqwest::Client,
    args: &Args,
    accounts: &[String],
) -> Result<(u64, u64, usize)> {
    let mut sum: u128 = 0;
    let mut last_slot = 0u64;
    let mut missing = 0usize;
    for chunk in accounts.chunks(100) {
        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [chunk, {"commitment": args.commitment, "encoding": "base64"}],
        });
        let (json, _, slot) = call(client, &args.rpc, &args.rpc_name, &req).await?;
        last_slot = slot;
        let value = json
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("getMultipleAccounts result.value missing: {json}"))?;
        for account in value {
            if account.is_null() {
                missing += 1;
                continue;
            }
            let lamports = account
                .get("lamports")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| anyhow!("account missing lamports: {account}"))?;
            sum += lamports as u128;
        }
    }
    Ok((sum as u64, last_slot, missing))
}

fn field_u64(value: &JsonValue, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing {key} in getSupply value"))
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
        .with_context(|| format!("Failed to connect to {name} ({url})"))?
        .json()
        .await
        .with_context(|| format!("Failed to parse JSON from {name} ({url})"))?;
    let slot = response
        .get("result")
        .and_then(|r| r.get("context"))
        .and_then(|c| c.get("slot"))
        .and_then(|s| s.as_u64())
        .unwrap_or_default();
    Ok((response, start.elapsed().as_millis(), slot))
}
