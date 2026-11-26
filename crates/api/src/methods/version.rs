// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    http::{CloudbreakApiResponse, CloudbreakRpcState},
};
use cloudbreak_core::EnvironmentInfo;
use serde::{Deserialize, Serialize};
use solana_rpc_client_api::response::{RpcApiVersion, RpcResponseContext};
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};

/// The tag the binary was built from, or the crate version for local builds.
const CLOUDBREAK_VERSION: &str = match option_env!("CLOUDBREAK_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};
const VERSION_CACHE_TTL: Duration = Duration::from_secs(600);

static VERSION_CACHE: LazyLock<RwLock<Option<(Instant, ReportedVersion)>>> =
    LazyLock::new(|| RwLock::new(None));

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

#[tracing::instrument(name = "getVersion", skip_all)]
pub async fn get_version(
    state: &CloudbreakRpcState,
) -> Result<CloudbreakApiResponse<RpcVersionInfo>, RpcError> {
    let version = reported_version(state).await?;
    Ok(CloudbreakApiResponse::Response(RpcVersionInfo {
        solana_core: version.solana_core,
    }))
}

/// `apiVersion` for a response; the Solana crate version if the indexer's is unknown.
pub async fn api_version(state: &CloudbreakRpcState) -> RpcApiVersion {
    reported_version(state)
        .await
        .map(|version| version.api_version)
        .unwrap_or_default()
}

pub async fn response_context(state: &CloudbreakRpcState, slot: u64) -> RpcResponseContext {
    RpcResponseContext {
        slot,
        api_version: Some(api_version(state).await),
    }
}

async fn reported_version(state: &CloudbreakRpcState) -> Result<ReportedVersion, RpcError> {
    if let Ok(cache) = VERSION_CACHE.read()
        && let Some((cached_at, version)) = cache.as_ref()
        && cached_at.elapsed() < VERSION_CACHE_TTL
    {
        return Ok(version.clone());
    }

    let grpc_version = EnvironmentInfo::load_grpc_version(&state.database)
        .await
        .map_err(|_| RpcError::InternalError)?;
    let version = build_version(grpc_version.as_deref(), CLOUDBREAK_VERSION);

    if let Ok(mut cache) = VERSION_CACHE.write() {
        *cache = Some((Instant::now(), version.clone()));
    }
    Ok(version)
}

/// `<agave>+cloudbreak<build>.grpc<plugin>`, from a plugin version like `17.0.0-rc9+solana.4.4.0`.
fn build_version(grpc_version: Option<&str>, cloudbreak_version: &str) -> ReportedVersion {
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
    let cloudbreak = cloudbreak_version.trim_start_matches('v');
    let cloudbreak = cloudbreak.split('+').next().unwrap_or(cloudbreak);

    let solana_core = format!(
        "{agave}+cloudbreak{}.grpc{}",
        metadata_safe(cloudbreak),
        metadata_safe(plugin)
    );
    let api_version =
        serde_json::from_value(serde_json::Value::String(solana_core.clone())).unwrap_or_default();
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
        let v = build_version(Some("17.0.0-rc9+solana.4.4.0"), "v0.1.0-22582ef");
        assert_eq!(
            v.solana_core,
            "4.4.0+cloudbreak0.1.0-22582ef.grpc17.0.0-rc9"
        );
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
    fn testnet_beta_and_release_tag_metadata() {
        let v = build_version(
            Some("18.0.0-rc4+solana.4.5.0.beta.0"),
            "0.1.0-staging.48+solana-rpc-cloudbreak",
        );
        assert_eq!(
            v.solana_core,
            "4.5.0-beta.0+cloudbreak0.1.0-staging.48.grpc18.0.0-rc4"
        );
        assert_eq!(v.api_version.to_string(), v.solana_core);
    }

    #[test]
    fn unknown_plugin_falls_back_to_the_crate_version() {
        let v = build_version(None, "0.1.0");
        let crate_version = RpcApiVersion::default().to_string();
        assert_eq!(
            v.solana_core,
            format!("{crate_version}+cloudbreak0.1.0.grpcunknown")
        );
        assert_eq!(v.api_version.to_string(), v.solana_core);
    }
}
