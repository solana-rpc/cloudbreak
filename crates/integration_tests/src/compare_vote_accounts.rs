use clap::Parser;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

use crate::config::RpcEndpoint;

#[derive(Parser, Debug)]
#[command(name = "compare_vote_accounts")]
#[command(
    about = "Compare getVoteAccounts activatedStake between cloudbreak (rpc1) and an upstream RPC (rpc2)"
)]
pub struct Args {
    /// Path to the config file
    #[arg(short = 'c', long, default_value = "cloudbreak.integration_tests.toml")]
    pub config: String,
    /// Max per-voter mismatches to print
    #[arg(long, default_value_t = 20)]
    pub max_print: usize,
    /// Total-stake difference tolerance (fraction) before the comparison is reported as FAIL
    #[arg(long, default_value_t = 0.001)]
    pub tolerance: f64,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub rpc1: RpcEndpoint,
    pub rpc2: RpcEndpoint,
}

#[derive(Deserialize)]
struct VoteAccountInfo {
    #[serde(rename = "votePubkey")]
    vote_pubkey: String,
    #[serde(rename = "activatedStake")]
    activated_stake: u64,
}

#[derive(Deserialize)]
struct VoteAccountStatus {
    current: Vec<VoteAccountInfo>,
    delinquent: Vec<VoteAccountInfo>,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: VoteAccountStatus,
}

pub async fn run(args: &Args) -> Result<(), anyhow::Error> {
    let config_content = std::fs::read_to_string(&args.config)?;
    let config: Config = toml::from_str(&config_content)?;

    let client = reqwest::Client::new();

    let stakes1 = fetch(&client, &config.rpc1).await?;
    let stakes2 = fetch(&client, &config.rpc2).await?;

    let total1: u128 = stakes1.values().map(|&v| v as u128).sum();
    let total2: u128 = stakes2.values().map(|&v| v as u128).sum();

    println!(
        "{}: {} voters, total activatedStake = {}",
        config.rpc1.name,
        stakes1.len(),
        total1
    );
    println!(
        "{}: {} voters, total activatedStake = {}",
        config.rpc2.name,
        stakes2.len(),
        total2
    );

    let diff = total1.abs_diff(total2);
    let frac = if total2 == 0 {
        0.0
    } else {
        diff as f64 / total2 as f64
    };
    println!("total diff = {diff} ({:.4}%)", frac * 100.0);

    let mut keys: Vec<&String> = stakes1
        .keys()
        .chain(stakes2.keys())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    keys.sort();

    let mut mismatches: Vec<(&String, u64, u64)> = keys
        .iter()
        .filter_map(|k| {
            let a = stakes1.get(*k).copied().unwrap_or(0);
            let b = stakes2.get(*k).copied().unwrap_or(0);
            (a != b).then_some((*k, a, b))
        })
        .collect();
    mismatches.sort_by_key(|(_, a, b)| std::cmp::Reverse(a.abs_diff(*b)));

    println!("per-voter mismatches: {}", mismatches.len());
    for (vote_pubkey, a, b) in mismatches.iter().take(args.max_print) {
        println!(
            "  {vote_pubkey}: {} = {a}, {} = {b} (diff {})",
            config.rpc1.name,
            config.rpc2.name,
            a.abs_diff(*b)
        );
    }

    if frac > args.tolerance {
        anyhow::bail!(
            "FAIL: total activatedStake differs by {:.4}% (tolerance {:.4}%)",
            frac * 100.0,
            args.tolerance * 100.0
        );
    }
    println!(
        "PASS: total activatedStake within {:.4}% tolerance",
        args.tolerance * 100.0
    );
    Ok(())
}

async fn fetch(
    client: &reqwest::Client,
    endpoint: &RpcEndpoint,
) -> Result<HashMap<String, u64>, anyhow::Error> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getVoteAccounts",
    });

    let response: RpcResponse = client
        .post(&endpoint.url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    let status = response.result;
    let mut map = HashMap::with_capacity(status.current.len() + status.delinquent.len());
    for info in status.current.into_iter().chain(status.delinquent) {
        map.insert(info.vote_pubkey, info.activated_stake);
    }
    Ok(map)
}
