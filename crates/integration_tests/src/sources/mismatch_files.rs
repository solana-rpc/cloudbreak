// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result};
use serde_json::Value as JsonValue;
use tokio::sync::watch;

use crate::sources::BenchRequest;
use crate::utils;

/// Reads all `mismatch_*.json` files from a directory and extracts the `request`
/// field from each, producing a list of requests to re-benchmark.
///
/// If `inject_context` is true, ensures each request has `withContext: true` in
/// its params object (creates the object if needed). This is necessary for GPA
/// requests that were originally sent without context, so that slot compensation
/// can work on the re-run.
pub fn load_requests(
    dir: &str,
    inject_context: bool,
) -> Result<watch::Receiver<Vec<BenchRequest>>> {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("Failed to read mismatch directory: {dir}"))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("mismatch_") && name.ends_with(".json")
        })
        .collect();

    let mut requests = Vec::new();
    for entry in &entries {
        let path = entry.path();
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let parsed: JsonValue = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {}", path.display()))?;

        if let Some(mut request) = parsed.get("request").cloned() {
            if inject_context {
                utils::inject_with_context(&mut request);
            }
            // No size estimate available for this source → uncapped.
            requests.push(BenchRequest {
                body: request,
                est_bytes: None,
            });
        } else {
            tracing::warn!(
                target: "bench_source",
                "No 'request' field in {}, skipping",
                path.display()
            );
        }
    }

    tracing::info!(
        target: "bench_source",
        "Loaded {} requests from {} mismatch files in {} (inject_context: {})",
        requests.len(),
        entries.len(),
        dir,
        inject_context,
    );

    let (_tx, rx) = watch::channel(requests);
    Ok(rx)
}
