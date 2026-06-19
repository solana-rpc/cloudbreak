// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use serde::Serialize;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::filter::RpcFilterType;
use std::convert::Infallible;
use std::str::FromStr;
use tracing_subscriber::EnvFilter;

use crate::http::CloudbreakRpcState;
use crate::http::server::{HttpHandlerResponse, ResponseBody};
use crate::modules::cache::GpaProcessor;

/// Used to get or set the log filter at runtime
pub fn log_filter_handler(req: &Request<Incoming>) -> Result<HttpHandlerResponse, Infallible> {
    let Some(handle) = cloudbreak_core::LOG_FILTER_HANDLE.get() else {
        return Ok(HttpHandlerResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: ResponseBody::Buffered(b"Log filter handle not initialized".to_vec()),
        });
    };

    // Check for ?filter=... query param to SET the filter
    if let Some(query) = req.uri().query() {
        // Parse query string: "filter=debug,hyper=warn"
        for pair in query.split('&') {
            if let Some(value) = pair.strip_prefix("filter=") {
                let decoded = urlencoding::decode(value).unwrap_or_default();
                match EnvFilter::try_new(&*decoded) {
                    Ok(new_filter) => {
                        if handle.reload(new_filter).is_err() {
                            return Ok(HttpHandlerResponse {
                                status: StatusCode::INTERNAL_SERVER_ERROR,
                                body: ResponseBody::Buffered(b"Failed to reload".to_vec()),
                            });
                        }
                        tracing::info!(target: "operational_endpoints", "Log filter updated to: {}", decoded);
                        return Ok(HttpHandlerResponse {
                            status: StatusCode::OK,
                            body: ResponseBody::Buffered(b"Filter set to:".to_vec()),
                        });
                    }
                    Err(_) => {
                        return Ok(HttpHandlerResponse {
                            status: StatusCode::BAD_REQUEST,
                            body: ResponseBody::Buffered(b"Invalid filter".to_vec()),
                        });
                    }
                }
            }
        }
    }

    // GET: return current filter
    let current = handle
        .with_current(|f| f.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    Ok(HttpHandlerResponse {
        status: StatusCode::OK,
        body: ResponseBody::Buffered(format!("Current filter: {}", current).into_bytes()),
    })
}

// ============================================================================
// GPA cache debug endpoint
// ============================================================================

/// HTTP debug endpoint for inspecting the live state of the GPA cache
/// (`crate::modules::cache::GpaProcessor::Cached`).
///
/// Always read-only. Designed to be safe to hit at any time: it takes a
/// read lock on the cache, copies the requested metadata into owned
/// structures, then releases the lock before serializing JSON. It will not
/// block concurrent gPA reader requests but will queue behind any
/// in-progress `finalize_query` writer for as long as that writer holds
/// the write lock.
///
/// # Route
///
/// `GET /debug/modules/gpa_cache`
///
/// # Query parameters
///
/// All parameters are optional.
///
/// | Name              | Type    | Default   | Description |
/// |-------------------|---------|-----------|-------------|
/// | `detail`          | enum    | `summary` | Controls verbosity. One of `summary`, `queries`, `full`. |
/// | `program`         | base58  | unset     | Filters the per-query listing to only queries cached for this program. Ignored when `detail=summary`. |
/// | `with_pubkeys`    | bool    | `false`   | When `true`, each query in the listing also includes the cached account pubkeys. Requires `program` to be set, otherwise the response could be huge. |
/// | `min_size_bytes`  | u64     | unset     | Filters the per-query listing to queries whose cached `size_bytes` is `>= min_size_bytes`. Ignored when `detail=summary`. |
/// | `max_size_bytes`  | u64     | unset     | Filters the per-query listing to queries whose cached `size_bytes` is `<= max_size_bytes`. Ignored when `detail=summary`. |
/// | `min_cache_hit_percent` | f64 | unset   | Filters the per-query listing to queries whose `cache_hit_percent` is `>= min_cache_hit_percent` (0–100). Ignored when `detail=summary`. |
/// | `max_cache_hit_percent` | f64 | unset   | Filters the per-query listing to queries whose `cache_hit_percent` is `<= max_cache_hit_percent` (0–100). Ignored when `detail=summary`. |
/// | `limit`           | usize   | unset     | Caps the per-query listing to at most this many entries (applied after all filters and the default newest-slot-first sort). Ignored when `detail=summary`. |
///
/// ### `detail` values
///
/// - `summary` — only cache-level statistics and configuration; no per-query
///   data.
/// - `queries` — `summary` + per-cached-query metadata
///   (`program`, `slot`, `size_bytes`, `account_count`, `cache_hits`,
///   `cache_hit_percent`, `encoding`, `data_slice`). No filter bodies, no
///   account pubkeys.
/// - `full` — `queries` + the raw `filters` (`memcmp`, `data_size`,
///   `token_account_state`) for each query. Adds account pubkeys only when
///   `with_pubkeys=true` and `program` is set.
///
/// ### Cache hit fields
///
/// `cache_hits` is the number of accounts that were served from cache during
/// the request that last produced this cached query. `cache_hit_percent` is
/// `cache_hits / account_count * 100`, with `0.0` reported when
/// `account_count == 0`. A fresh query that populated the cache for the first
/// time reads as `0%`; subsequent refreshes for an identical query trend
/// toward `100%` as the underlying program state stabilizes.
///
/// # Response
///
/// Always returns `Content-Type: application/json`.
///
/// ## Status codes
///
/// - `200 OK` — successful response.
/// - `400 Bad Request` — invalid query parameter (bad `detail` value,
///   un-parseable `program`, `with_pubkeys=true` without `program`,
///   `min_size_bytes > max_size_bytes`, a cache-hit percent outside `0..=100`,
///   or `min_cache_hit_percent > max_cache_hit_percent`).
/// - `503 Service Unavailable` — cache `RwLock` is poisoned (a prior request
///   panicked while holding the write lock).
///
/// ## Body shape
///
/// When the cache module is disabled (`GpaProcessor::Standard`):
///
/// ```json
/// { "enabled": false }
/// ```
///
/// When the cache module is enabled (shape depends on `detail`):
///
/// ```json
/// {
///   "enabled": true,
///   "config": {
///     "max_total_bytes": 1073741824,
///     "min_bytes_per_query": 65536,
///     "max_bytes_query_cleanup": 104857600,
///     "max_pinned_bytes_ratio": 0.5
///   },
///   "stats": {
///     "size_bytes": 234567890,
///     "utilization_percent": 21.85,
///     "pinned_size_bytes": 314572800,
///     "pinned_threshold_bytes": 536870912,
///     "pinned_utilization_percent": 58.59,
///     "num_queries": 42,
///     "num_distinct_slots": 5,
///     "oldest_slot": 300100123,
///     "newest_slot": 300100456,
///     "queries_per_slot": [
///       { "slot": 300100123, "num_queries": 4 },
///       { "slot": 300100200, "num_queries": 10 }
///     ]
///   },
///   "queries": [
///     {
///       "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
///       "slot": 300100456,
///       "size_bytes": 1048576,
///       "account_count": 128,
///       "cache_hits": 120,
///       "cache_hit_percent": 93.75,
///       "encoding": "base64",
///       "data_slice": null,
///       "filters": [...],           // only when detail=full
///       "account_pubkeys": [...]    // only when with_pubkeys=true & program set
///     }
///   ]
/// }
/// ```
///
/// The `queries` field is omitted entirely when `detail=summary`.
///
/// # Examples
///
/// Quick summary:
///
/// ```text
/// curl http://localhost:8899/debug/modules/gpa_cache
/// ```
///
/// All queries with filter contents:
///
/// ```text
/// curl 'http://localhost:8899/debug/modules/gpa_cache?detail=full'
/// ```
///
/// All cached queries for the SPL Token program:
///
/// ```text
/// curl 'http://localhost:8899/debug/modules/gpa_cache?detail=queries&program=TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
/// ```
///
/// SPL Token cached queries with their cached account pubkeys:
///
/// ```text
/// curl 'http://localhost:8899/debug/modules/gpa_cache?detail=full&program=TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA&with_pubkeys=true'
/// ```
///
/// Top 20 heaviest cached queries (>= 1 MiB):
///
/// ```text
/// curl 'http://localhost:8899/debug/modules/gpa_cache?detail=queries&min_size_bytes=1048576&limit=20'
/// ```
///
/// Poorly-cached queries (per-account hit ratio below 50%):
///
/// ```text
/// curl 'http://localhost:8899/debug/modules/gpa_cache?detail=queries&max_cache_hit_percent=50'
/// ```
pub fn gpa_cache_handler(
    req: &Request<Incoming>,
    state: &CloudbreakRpcState,
) -> Result<HttpHandlerResponse, Infallible> {
    let params = match GpaCacheParams::from_query(req.uri().query()) {
        Ok(p) => p,
        Err(msg) => return Ok(json_error(StatusCode::BAD_REQUEST, &msg)),
    };

    let body = match &state.gpa_processor {
        GpaProcessor::Standard => serde_json::to_vec(&GpaCacheResponse {
            enabled: false,
            config: None,
            stats: None,
            queries: None,
        })
        .unwrap_or_default(),

        GpaProcessor::Cached { cache, .. } => {
            let guard = match cache.read() {
                Ok(g) => g,
                Err(_) => {
                    return Ok(json_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "gpa cache rwlock poisoned",
                    ));
                }
            };
            let response = build_gpa_cache_response(&guard, &params);
            serde_json::to_vec(&response).unwrap_or_default()
        }
    };

    Ok(HttpHandlerResponse {
        status: StatusCode::OK,
        body: ResponseBody::Buffered(body),
    })
}

/// Parsed query parameters for the GPA cache debug endpoint.
struct GpaCacheParams {
    detail: GpaCacheDetail,
    program: Option<Pubkey>,
    with_pubkeys: bool,
    min_size_bytes: Option<u64>,
    max_size_bytes: Option<u64>,
    min_cache_hit_percent: Option<f64>,
    max_cache_hit_percent: Option<f64>,
    limit: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GpaCacheDetail {
    Summary,
    Queries,
    Full,
}

impl GpaCacheParams {
    fn from_query(query: Option<&str>) -> Result<Self, String> {
        let mut detail = GpaCacheDetail::Summary;
        let mut program: Option<Pubkey> = None;
        let mut with_pubkeys = false;
        let mut min_size_bytes: Option<u64> = None;
        let mut max_size_bytes: Option<u64> = None;
        let mut min_cache_hit_percent: Option<f64> = None;
        let mut max_cache_hit_percent: Option<f64> = None;
        let mut limit: Option<usize> = None;

        if let Some(q) = query {
            for pair in q.split('&').filter(|s| !s.is_empty()) {
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                let v = urlencoding::decode(v).map_err(|e| format!("invalid url encoding: {e}"))?;
                match k {
                    "detail" => {
                        detail = match v.as_ref() {
                            "summary" => GpaCacheDetail::Summary,
                            "queries" => GpaCacheDetail::Queries,
                            "full" => GpaCacheDetail::Full,
                            other => {
                                return Err(format!(
                                    "invalid `detail` value '{other}'; expected one of: summary, queries, full"
                                ));
                            }
                        };
                    }
                    "program" => {
                        program = Some(
                            Pubkey::from_str(v.as_ref())
                                .map_err(|e| format!("invalid `program` pubkey: {e}"))?,
                        );
                    }
                    "with_pubkeys" => {
                        with_pubkeys = match v.as_ref() {
                            "true" | "1" => true,
                            "false" | "0" | "" => false,
                            other => {
                                return Err(format!(
                                    "invalid `with_pubkeys` value '{other}'; expected true or false"
                                ));
                            }
                        };
                    }
                    "min_size_bytes" => {
                        min_size_bytes =
                            Some(v.parse::<u64>().map_err(|e| {
                                format!("invalid `min_size_bytes` value '{v}': {e}")
                            })?);
                    }
                    "max_size_bytes" => {
                        max_size_bytes =
                            Some(v.parse::<u64>().map_err(|e| {
                                format!("invalid `max_size_bytes` value '{v}': {e}")
                            })?);
                    }
                    "min_cache_hit_percent" => {
                        min_cache_hit_percent =
                            Some(parse_percent("min_cache_hit_percent", v.as_ref())?);
                    }
                    "max_cache_hit_percent" => {
                        max_cache_hit_percent =
                            Some(parse_percent("max_cache_hit_percent", v.as_ref())?);
                    }
                    "limit" => {
                        limit = Some(
                            v.parse::<usize>()
                                .map_err(|e| format!("invalid `limit` value '{v}': {e}"))?,
                        );
                    }
                    _ => {
                        // Unknown params ignored, to be friendly with future expansion.
                    }
                }
            }
        }

        if with_pubkeys && program.is_none() {
            return Err(
                "`with_pubkeys=true` requires `program=<pubkey>` to avoid dumping pubkeys for the entire cache"
                    .to_string(),
            );
        }

        if let (Some(min), Some(max)) = (min_size_bytes, max_size_bytes)
            && min > max
        {
            return Err(format!(
                "`min_size_bytes` ({min}) must be <= `max_size_bytes` ({max})"
            ));
        }

        if let (Some(min), Some(max)) = (min_cache_hit_percent, max_cache_hit_percent)
            && min > max
        {
            return Err(format!(
                "`min_cache_hit_percent` ({min}) must be <= `max_cache_hit_percent` ({max})"
            ));
        }

        Ok(Self {
            detail,
            program,
            with_pubkeys,
            min_size_bytes,
            max_size_bytes,
            min_cache_hit_percent,
            max_cache_hit_percent,
            limit,
        })
    }
}

/// Per-account cache-hit ratio of a cached query, as a percentage in `[0, 100]`.
/// Returns `0.0` for an empty query to avoid dividing by zero.
fn compute_cache_hit_percent(cache_hits: u64, account_count: usize) -> f64 {
    if account_count == 0 {
        0.0
    } else {
        (cache_hits as f64) / (account_count as f64) * 100.0
    }
}

/// Parses a percentage query-param value, requiring a finite number in `[0, 100]`.
fn parse_percent(key: &str, value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|e| format!("invalid `{key}` value '{value}': {e}"))?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return Err(format!(
            "`{key}` value '{value}' must be a number between 0 and 100"
        ));
    }
    Ok(parsed)
}

// ----- Response structs ------------------------------------------------------

#[derive(Serialize)]
struct GpaCacheResponse {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<GpaCacheConfigInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<GpaCacheStatsInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queries: Option<Vec<GpaCacheQueryInfo>>,
}

#[derive(Serialize)]
struct GpaCacheConfigInfo {
    max_total_bytes: usize,
    min_bytes_per_query: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes_query_cleanup: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_pinned_bytes_ratio: Option<f64>,
}

#[derive(Serialize)]
struct GpaCacheStatsInfo {
    size_bytes: u64,
    utilization_percent: f64,
    /// Bytes currently held by pinned (non-evictable) queries.
    pinned_size_bytes: u64,
    /// Max bytes pinned queries are allowed to collectively hold. Omitted when
    /// no pinned cap is configured (`max_pinned_bytes_ratio` unset).
    #[serde(skip_serializing_if = "Option::is_none")]
    pinned_threshold_bytes: Option<u64>,
    /// Pinned usage as a percentage of `pinned_threshold_bytes`. Omitted when no
    /// pinned cap is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pinned_utilization_percent: Option<f64>,
    num_queries: usize,
    num_distinct_slots: usize,
    oldest_slot: Option<u64>,
    newest_slot: Option<u64>,
    queries_per_slot: Vec<GpaCacheSlotInfo>,
}

#[derive(Serialize)]
struct GpaCacheSlotInfo {
    slot: u64,
    num_queries: usize,
}

#[derive(Serialize)]
struct GpaCacheQueryInfo {
    program: String,
    slot: u64,
    size_bytes: u64,
    account_count: usize,
    cache_hits: u64,
    cache_hit_percent: f64,
    encoding: String,
    data_slice: Option<solana_account_decoder::UiDataSliceConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<Vec<RpcFilterType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_pubkeys: Option<Vec<String>>,
}

// ----- Response builder ------------------------------------------------------

fn build_gpa_cache_response(
    cache: &crate::modules::cache::GpaCache,
    params: &GpaCacheParams,
) -> GpaCacheResponse {
    let max_total_bytes = cache.config.max_total_bytes;
    let utilization_percent = if max_total_bytes == 0 {
        0.0
    } else {
        (cache.size as f64) / (max_total_bytes as f64) * 100.0
    };

    let queries_per_slot: Vec<GpaCacheSlotInfo> = cache
        .queries_for_slot
        .iter()
        .map(|(slot, queries)| GpaCacheSlotInfo {
            slot: *slot,
            num_queries: queries.len(),
        })
        .collect();

    let oldest_slot = cache.queries_for_slot.keys().next().copied();
    let newest_slot = cache.queries_for_slot.keys().next_back().copied();

    // Only report a pinned cap when one is actually configured.
    let (pinned_threshold_bytes, pinned_utilization_percent) =
        if cache.config.max_pinned_bytes_ratio.is_some() {
            let threshold = cache.pinned_threshold();
            let utilization = if threshold == 0 {
                0.0
            } else {
                (cache.pinned_size as f64) / (threshold as f64) * 100.0
            };
            (Some(threshold), Some(utilization))
        } else {
            (None, None)
        };

    let stats = GpaCacheStatsInfo {
        size_bytes: cache.size,
        utilization_percent,
        pinned_size_bytes: cache.pinned_size,
        pinned_threshold_bytes,
        pinned_utilization_percent,
        num_queries: cache.queries.len(),
        num_distinct_slots: cache.queries_for_slot.len(),
        oldest_slot,
        newest_slot,
        queries_per_slot,
    };

    let config = GpaCacheConfigInfo {
        max_total_bytes,
        min_bytes_per_query: cache.config.min_bytes_per_query,
        max_bytes_query_cleanup: cache.config.max_bytes_query_cleanup,
        max_pinned_bytes_ratio: cache.config.max_pinned_bytes_ratio,
    };

    let queries = match params.detail {
        GpaCacheDetail::Summary => None,
        GpaCacheDetail::Queries | GpaCacheDetail::Full => {
            let include_filters = matches!(params.detail, GpaCacheDetail::Full);
            let mut out: Vec<GpaCacheQueryInfo> = cache
                .queries
                .iter()
                .filter(|(nq, _)| {
                    params
                        .program
                        .map(|filter_program| nq.program == filter_program)
                        .unwrap_or(true)
                })
                .filter(|(_, cq)| {
                    params.min_size_bytes.map(|m| cq.size >= m).unwrap_or(true)
                        && params.max_size_bytes.map(|m| cq.size <= m).unwrap_or(true)
                })
                .filter(|(_, cq)| {
                    if params.min_cache_hit_percent.is_none()
                        && params.max_cache_hit_percent.is_none()
                    {
                        return true;
                    }
                    let hit_percent = compute_cache_hit_percent(cq.cache_hits, cq.accounts.len());
                    params
                        .min_cache_hit_percent
                        .map(|m| hit_percent >= m)
                        .unwrap_or(true)
                        && params
                            .max_cache_hit_percent
                            .map(|m| hit_percent <= m)
                            .unwrap_or(true)
                })
                .map(|(nq, cq)| {
                    let account_pubkeys = if params.with_pubkeys {
                        Some(cq.accounts.keys().map(|pk| pk.to_string()).collect())
                    } else {
                        None
                    };

                    let account_count = cq.accounts.len();
                    let cache_hit_percent = compute_cache_hit_percent(cq.cache_hits, account_count);

                    GpaCacheQueryInfo {
                        program: nq.program.to_string(),
                        slot: cq.slot,
                        size_bytes: cq.size,
                        account_count,
                        cache_hits: cq.cache_hits,
                        cache_hit_percent,
                        encoding: encoding_to_string(nq.encoding),
                        data_slice: nq.data_slice,
                        filters: include_filters.then(|| nq.filters.clone()),
                        account_pubkeys,
                    }
                })
                .collect();

            // Newest-first by slot is more useful for debugging than HashMap order.
            out.sort_by(|a, b| b.slot.cmp(&a.slot));
            if let Some(limit) = params.limit {
                out.truncate(limit);
            }
            Some(out)
        }
    };

    GpaCacheResponse {
        enabled: true,
        config: Some(config),
        stats: Some(stats),
        queries,
    }
}

fn encoding_to_string(encoding: solana_account_decoder::UiAccountEncoding) -> String {
    match encoding {
        solana_account_decoder::UiAccountEncoding::Binary => "binary",
        solana_account_decoder::UiAccountEncoding::Base58 => "base58",
        solana_account_decoder::UiAccountEncoding::Base64 => "base64",
        solana_account_decoder::UiAccountEncoding::JsonParsed => "jsonParsed",
        solana_account_decoder::UiAccountEncoding::Base64Zstd => "base64+zstd",
    }
    .to_string()
}

fn json_error(status: StatusCode, message: &str) -> HttpHandlerResponse {
    let body = serde_json::json!({ "error": message });
    HttpHandlerResponse {
        status,
        body: ResponseBody::Buffered(serde_json::to_vec(&body).unwrap_or_default()),
    }
}
