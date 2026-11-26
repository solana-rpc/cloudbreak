// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    http::{CloudbreakApiResponse, CloudbreakRpcState},
};
use cloudbreak_core::EnvironmentInfo;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use solana_rpc_client_api::response::{RpcApiVersion, RpcResponseContext};
use std::sync::OnceLock;

const CLOUDBREAK_VERSION: &str = env!("CARGO_PKG_VERSION");

static REPORTED_VERSION: OnceLock<ReportedVersion> = OnceLock::new();

/// One version string for `getVersion` and every response's `context.apiVersion`.
#[derive(Debug, Clone)]
struct ReportedVersion {
    solana_core: String,
    api_version: RpcApiVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcVersionInfo {
    #[serde(rename = "solana-core")]
    pub solana_core: String,
}

/// Reads the indexer's gRPC version once at startup; later changes need an API restart.
pub async fn load(database: &DatabaseConnection) {
    let grpc_version = EnvironmentInfo::load_grpc_version(database)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("failed to load the gRPC version, reporting it as unknown: {e}");
            None
        });
    let version = compose_version(grpc_version.as_deref(), CLOUDBREAK_VERSION);
    tracing::info!("reporting version {}", version.solana_core);
    let _ = REPORTED_VERSION.set(version);
}

fn reported_version() -> &'static ReportedVersion {
    REPORTED_VERSION.get_or_init(|| compose_version(None, CLOUDBREAK_VERSION))
}

#[tracing::instrument(name = "getVersion", skip_all)]
pub async fn get_version(
    _state: &CloudbreakRpcState,
) -> Result<CloudbreakApiResponse<RpcVersionInfo>, RpcError> {
    Ok(CloudbreakApiResponse::Response(RpcVersionInfo {
        solana_core: reported_version().solana_core.clone(),
    }))
}

pub fn api_version() -> RpcApiVersion {
    reported_version().api_version.clone()
}

pub fn response_context(slot: u64) -> RpcResponseContext {
    RpcResponseContext {
        slot,
        api_version: Some(api_version()),
    }
}

/// `<agave>+cloudbreak<crate>.grpc<plugin>`, from a plugin version like `17.0.0-rc9+solana.4.4.0`.
fn compose_version(grpc_version: Option<&str>, cloudbreak_version: &str) -> ReportedVersion {
    let (plugin, agave) = match grpc_version {
        Some(version) => match version.split_once('+') {
            Some((plugin, metadata)) => (plugin, metadata.strip_prefix("solana.")),
            None => (version, None),
        },
        None => ("unknown", None),
    };
    let agave = agave
        .and_then(agave_semver)
        .unwrap_or_else(|| RpcApiVersion::default().to_string());
    let solana_core = format!(
        "{agave}+cloudbreak{}.grpc{}",
        metadata_safe(cloudbreak_version),
        metadata_safe(plugin)
    );
    let api_version = serde_json::from_value(serde_json::Value::String(solana_core.clone()))
        .unwrap_or_else(|e| {
            tracing::warn!("{solana_core} is not semver ({e}); apiVersion uses the crate version");
            RpcApiVersion::default()
        });
    ReportedVersion {
        solana_core,
        api_version,
    }
}

/// `4.4.0` stays as is; `4.5.0.beta.0` becomes `4.5.0-beta.0`.
fn agave_semver(version: &str) -> Option<String> {
    let mut parts = version.split('.');
    let core: Vec<&str> = parts.by_ref().take(3).collect();
    if core.len() != 3 || core.iter().any(|p| p.parse::<u64>().is_err()) {
        return None;
    }
    let pre: Vec<&str> = parts.collect();
    Some(if pre.is_empty() {
        core.join(".")
    } else {
        format!("{}-{}", core.join("."), pre.join("."))
    })
}

/// Semver build metadata allows only ASCII letters, digits, `-` and `.`.
fn metadata_safe(part: &str) -> String {
    part.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_plugin_version() {
        let v = compose_version(Some("17.0.0-rc9+solana.4.4.0"), "0.1.0");
        assert_eq!(v.solana_core, "4.4.0+cloudbreak0.1.0.grpc17.0.0-rc9");
        assert_eq!(v.api_version.to_string(), v.solana_core);
        assert_eq!(
            (
                v.api_version.major,
                v.api_version.minor,
                v.api_version.patch
            ),
            (4, 4, 0)
        );
    }

    #[test]
    fn testnet_beta_version() {
        let v = compose_version(Some("18.0.0-rc4+solana.4.5.0.beta.0"), "0.1.0");
        assert_eq!(v.solana_core, "4.5.0-beta.0+cloudbreak0.1.0.grpc18.0.0-rc4");
        assert_eq!(v.api_version.to_string(), v.solana_core);
    }

    #[test]
    fn unknown_plugin_falls_back_to_the_crate_version() {
        let v = compose_version(None, "0.1.0");
        let crate_version = RpcApiVersion::default().to_string();
        assert_eq!(
            v.solana_core,
            format!("{crate_version}+cloudbreak0.1.0.grpcunknown")
        );
        assert_eq!(v.api_version.to_string(), v.solana_core);
    }
}
