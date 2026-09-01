// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use clap::{Parser, Subcommand};

mod bandwidth;
mod benchmark;
mod compare_accounts_by_mint;
mod compare_accounts_by_mint_vs_cluster;
mod compare_genesis_hash;
mod compare_largest_accounts;
mod compare_program_accounts;
mod compare_token_largest_accounts;
mod compare_version;
mod compare_vote_accounts;
mod config;
mod db_check;
mod get_slot;
mod logging;
mod response_comparison;
mod sources;
mod utils;

#[derive(Parser)]
#[command(name = "integration_tests")]
#[command(about = "Integration tests for Solana RPC endpoints")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run benchmark
    Benchmark(config::BenchmarkArgs),
    /// Compare getProgramAccounts responses (legacy)
    Compare(compare_program_accounts::Args),
    /// Compare getVoteAccounts activatedStake against an upstream RPC
    CompareVoteAccounts(compare_vote_accounts::Args),
    /// Compare getTokenAccountsByMint (cloudbreak) against getProgramAccounts + mint memcmp (source of truth)
    CompareAccountsByMint(compare_accounts_by_mint::Args),
    /// Smoke-check getTokenAccountsByMint against a cluster RPC using getTokenLargestAccounts as oracle
    CompareAccountsByMintVsCluster(compare_accounts_by_mint_vs_cluster::Args),
    /// Validate getLargestAccounts against per-account getBalance on the same node
    CompareLargestAccounts(compare_largest_accounts::Args),
    /// Validate getTokenLargestAccounts against per-account getAccountInfo on the same node
    CompareTokenLargestAccounts(compare_token_largest_accounts::Args),
    /// Compare getGenesisHash between two RPC endpoints (exact equality)
    CompareGenesisHash(compare_genesis_hash::Args),
    /// Compare getVersion: cloudbreak's composite string should embed the cluster's solana-core
    CompareVersion(compare_version::Args),
    /// Get slot (legacy)
    GetSlot(get_slot::Args),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Tracing is initialized per-subcommand so the benchmark command can
    // append per-target directives derived from its `[print_config]` flags
    // before the subscriber is installed (see `logging::init_tracing`).
    match cli.command {
        Commands::Compare(args) => compare_program_accounts::run(&args).await?,
        Commands::CompareVoteAccounts(args) => compare_vote_accounts::run(&args).await?,
        Commands::CompareAccountsByMint(args) => compare_accounts_by_mint::run(&args).await?,
        Commands::CompareAccountsByMintVsCluster(args) => {
            compare_accounts_by_mint_vs_cluster::run(&args).await?
        }
        Commands::CompareLargestAccounts(args) => compare_largest_accounts::run(&args).await?,
        Commands::CompareTokenLargestAccounts(args) => {
            compare_token_largest_accounts::run(&args).await?
        }
        Commands::CompareGenesisHash(args) => compare_genesis_hash::run(&args).await?,
        Commands::CompareVersion(args) => compare_version::run(&args).await?,
        Commands::Benchmark(args) => benchmark::run(&args).await?,
        Commands::GetSlot(args) => get_slot::run(&args).await?,
    }

    Ok(())
}
