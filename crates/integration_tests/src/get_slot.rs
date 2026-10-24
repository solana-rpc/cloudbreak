// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use clap::Parser;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

use crate::config::RpcEndpoint;

#[derive(Parser, Debug)]
#[command(name = "get_slot")]
#[command(about = "Get slot")]
pub struct Args {
    /// Path to the config file
    #[arg(short = 'c', long, default_value = "cloudbreak.integration_tests.toml")]
    pub config: String,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub rpc1: RpcEndpoint,
}

pub async fn run(args: &Args) -> Result<(), anyhow::Error> {
    crate::logging::init_tracing(Vec::new());

    let config_content = std::fs::read_to_string(&args.config)?;
    let config: Config = toml::from_str(&config_content)?;

    let client = crate::utils::http_client_builder().build()?;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "confirmed"}]
    });

    loop {
        let response = client
            .post(&config.rpc1.url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let json: Value = response.json().await?;

        if let Some(slot) = json.get("result") {
            println!("Slot: {}", slot);
        } else {
            println!("Response: {}", json);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
