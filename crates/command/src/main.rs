// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use clap::{Parser, Subcommand};
use cloudbreak_api::run as run_api;
use cloudbreak_core::{
    Result, SnapshotConfig, TryLoadConfig,
    modules::{account_owner_map::AccountOwnerMap, largest_accounts::LargestAccountsTracker},
};
use cloudbreak_index::indexer::run as run_index;
use cloudbreak_query_tracker::run as run_query_tracker;
use cloudbreak_snapshot::run as run_snapshot;

mod hash_check;
mod opentelemetry;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

/// Docs: https://jemalloc.net/jemalloc.3.html
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(
    name = "cloudbreak",
    version,
    about = "Faster, cheaper, and easier Solana RPC"
)]
pub struct Cli {
    /// Path to the configuration file
    #[arg(short, long, default_value = "cloudbreak.toml")]
    pub config: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Api,
    Index,
    Snapshot,
    QueryTracker,
    SnapshotDiff(hash_check::DiffArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    opentelemetry::init_tracer(&args.config);

    match args.command {
        Commands::Api => run_api(&args.config).await,
        Commands::Index => run_index(&args.config).await,
        Commands::Snapshot => {
            run_snapshot(
                SnapshotConfig::try_load(&args.config).expect("Failed to load config"),
                None,
                None,
                None,
                AccountOwnerMap::default(),
                LargestAccountsTracker::default(),
            )
            .await
        }
        Commands::QueryTracker => run_query_tracker(&args.config).await,
        Commands::SnapshotDiff(diff_args) => hash_check::run_diff(&diff_args).await,
    }
}
