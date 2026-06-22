// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use hyper::StatusCode;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex, Once},
};
use tracing::error;
use cloudbreak_core::ApiConfig;

use crate::http::server::HttpHandlerResponse;

lazy_static::lazy_static! {
    static ref METRICS_REGISTRY: Registry = Registry::new();

    pub static ref CLOUDBREAK_API_REQUESTS_TOTAL:IntCounterVec = IntCounterVec::new(
        Opts::new("cloudbreak_api_requests_total", "Total number of Cloudbreak API calls, labelled by method and status"),
        &["method", "status"]
    ).unwrap();

    /// Total API RPC request latency in milliseconds
    pub static ref CLOUDBREAK_API_REQUEST_DURATION_MS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "cloudbreak_api_request_duration_ms",
            "Total API request latency in milliseconds, labeled by method."
        )
        .buckets(vec![
            1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0, 200.0, 300.0, 400.0, 500.0, 650.0, 800.0,
            1000.0, 1500.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0, 7000.0, 8000.0, 9000.0, 10000.0, 12000.0,
            14000.0, 16000.0, 18000.0, 20000.0, 25000.0, 30000.0, 40000.0, 50000.0, 80000.0, 100000.0,
            150000.0, 200000.0, 300000.0
        ]),
        &["method", "bytes"],
    )
    .unwrap();

    /// Per-request GPA cache hit percentage (0-100), labelled by method and
    /// response size bucket. Reports `0` when the cache is inactive or the
    /// response has no accounts. Buckets are fine-grained near both ends so
    /// fully-cached (100) and fully-fresh (0) requests stand out.
    pub static ref CLOUDBREAK_API_CACHE_HIT_PERCENT: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "cloudbreak_api_cache_hit_percent",
            "Per-request GPA cache hit percentage (0-100), labelled by method and response size bucket."
        )
        .buckets(vec![
            0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0,
            60.0, 70.0, 80.0, 90.0, 95.0, 97.0, 98.0, 99.0, 99.5, 100.0,
        ]),
        &["method", "bytes"],
    )
    .unwrap();

    /// Response bytes served, labelled by method and `kind`: `total` counts all
    /// response bytes, `cached` counts the subset served from the GPA cache
    /// (`0` for paths that don't use the cache). Effective cache utilization is
    /// `kind="cached" / kind="total"` in Grafana.
    pub static ref CLOUDBREAK_API_RESPONSE_BYTES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "cloudbreak_api_response_bytes_total",
            "Response bytes served, labelled by method and kind (total/cached)."
        ),
        &["method", "kind"],
    )
    .unwrap();

    /// Count of requests by subscription ID
    pub static ref CLOUDBREAK_API_REQUESTS_BY_SUBSCRIPTION_ID: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "cloudbreak_api_requests_by_subscription_id",
            "Total number of Cloudbreak API calls, labelled by subscription ID."
        ),
        &["subscription_id_key"],
    )
    .unwrap();

    /// Amount of data fetched by subscription ID in bytes
    pub static ref CLOUDBREAK_API_DATA_FETCHED_BY_SUBSCRIPTION_ID: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "cloudbreak_api_data_fetched_by_subscription_id",
            "Amount of data fetched by subscription ID in bytes."
        ),
        &["subscription_id_key"]
    )
    .unwrap();

    /// Current number of in-flight API requests by method
    pub static ref CLOUDBREAK_API_INFLIGHT_REQUESTS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("cloudbreak_api_inflight_requests", "Current number of in-flight API requests"),
        &["method"],
    )
    .unwrap();

    pub static ref CLOUDBREAK_API_BATCH_REQUESTS: IntCounterVec = IntCounterVec::new(
        Opts::new("cloudbreak_api_batch_requests_total", "Total number of batched requests by batch size"),
        &["batch_size"]
    ).unwrap();

    /// Current size of the GPA cache in bytes.
    pub static ref CLOUDBREAK_GPA_CACHE_SIZE_BYTES: IntGauge = IntGauge::new(
        "cloudbreak_gpa_cache_size_bytes",
        "Current size of the GPA cache in bytes"
    ).unwrap();

    /// Configured maximum size of the GPA cache in bytes. Exposed so utilization
    /// (`size / max`) can be computed at query time in Grafana.
    pub static ref CLOUDBREAK_GPA_CACHE_MAX_BYTES: IntGauge = IntGauge::new(
        "cloudbreak_gpa_cache_max_bytes",
        "Configured maximum size of the GPA cache in bytes"
    ).unwrap();

    /// Total number of GPA cache entries evicted by cleanup to make room for a
    /// different query. Labelled by `used` ("used"/"unused") indicating whether
    /// the evicted entry had ever served a cache hit. A high rate of `unused`
    /// evictions indicates cache churn (e.g. `min-bytes-per-query` set too low).
    pub static ref CLOUDBREAK_GPA_CACHE_EVICTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "cloudbreak_gpa_cache_evictions_total",
            "Total GPA cache entries evicted by cleanup, labelled by whether the entry was ever a cache hit"
        ),
        &["used"],
    ).unwrap();

    /// Total number of bytes evicted from the GPA cache by cleanup. Same `used`
    /// labelling as `CLOUDBREAK_GPA_CACHE_EVICTIONS_TOTAL`.
    pub static ref CLOUDBREAK_GPA_CACHE_EVICTED_BYTES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "cloudbreak_gpa_cache_evicted_bytes_total",
            "Total bytes evicted from the GPA cache by cleanup, labelled by whether the entry was ever a cache hit"
        ),
        &["used"],
    ).unwrap();
}

/// We use a guard to increment the in-flight requests metric when a request starts and
/// decrement it when the request completes. This way the counter is decremented
/// even in the case of panics or early returns.
pub struct InFlightRequestGuard {
    method: &'static str,
}

impl InFlightRequestGuard {
    pub fn new(method: &'static str) -> Self {
        CLOUDBREAK_API_INFLIGHT_REQUESTS
            .with_label_values(&[method])
            .inc();
        Self { method }
    }
}

impl Drop for InFlightRequestGuard {
    fn drop(&mut self) {
        CLOUDBREAK_API_INFLIGHT_REQUESTS
            .with_label_values(&[self.method])
            .dec();
    }
}

pub fn metrics_handler() -> Result<HttpHandlerResponse, Infallible> {
    let metrics = TextEncoder::new()
        .encode_to_string(&METRICS_REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {error}");
            String::new()
        });

    Ok(HttpHandlerResponse {
        status: StatusCode::OK,
        body: crate::http::server::ResponseBody::Buffered(metrics.into_bytes()),
    })
}

pub fn setup_metrics(config: &ApiConfig) -> anyhow::Result<()> {
    static REGISTER: Once = Once::new();

    REGISTER.call_once(|| {
        macro_rules! register {
            ($collector:ident) => {
                METRICS_REGISTRY
                    .register(Box::new($collector.clone()))
                    .expect("collector can't be registered");
            };
        }
        register!(CLOUDBREAK_API_REQUESTS_TOTAL);
        register!(CLOUDBREAK_API_REQUEST_DURATION_MS);
        register!(CLOUDBREAK_API_CACHE_HIT_PERCENT);
        register!(CLOUDBREAK_API_RESPONSE_BYTES_TOTAL);
        register!(CLOUDBREAK_API_DATA_FETCHED_BY_SUBSCRIPTION_ID);
        register!(CLOUDBREAK_API_REQUESTS_BY_SUBSCRIPTION_ID);
        register!(CLOUDBREAK_API_INFLIGHT_REQUESTS);
        register!(CLOUDBREAK_API_BATCH_REQUESTS);
        register!(CLOUDBREAK_GPA_CACHE_SIZE_BYTES);
        register!(CLOUDBREAK_GPA_CACHE_MAX_BYTES);
        register!(CLOUDBREAK_GPA_CACHE_EVICTIONS_TOTAL);
        register!(CLOUDBREAK_GPA_CACHE_EVICTED_BYTES_TOTAL);
    });

    // Set the max connections as a reference metric at startup
    CLOUDBREAK_API_INFLIGHT_REQUESTS
        .with_label_values(&["max"])
        .set(config.server.max_connections as i64);

    Ok(())
}

#[derive(Debug, Clone)]
pub struct GpaMetricsData {
    pub label: String,
    pub db_time: Arc<Mutex<f64>>,
    pub db_first_row_time: Arc<Mutex<f64>>,
    pub encode_time: Arc<Mutex<f64>>,
}

impl GpaMetricsData {
    pub fn new(label: String) -> Self {
        Self {
            label,
            db_time: Arc::new(Mutex::new(0.0)),
            db_first_row_time: Arc::new(Mutex::new(0.0)),
            encode_time: Arc::new(Mutex::new(0.0)),
        }
    }

    pub fn set_db_metrics(&self, db_time: f64, db_first_row_time: f64) {
        *self.db_time.lock().unwrap() = db_time;
        *self.db_first_row_time.lock().unwrap() = db_first_row_time;
    }

    pub fn set_encode_metrics(&self, encode_time: f64) {
        *self.encode_time.lock().unwrap() = encode_time;
    }

    pub fn record_metrics(
        &self,
        json_enconde_time: f64,
        total_time: f64,
        json_bytes: u64,
        cache_bytes: u64,
        cache_hit_percent: f64,
        subscription_id: String,
    ) {
        let bytes_bucket = bytes_bucket(json_bytes);
        let label = &self.label;

        CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&[format!("{label}_db").as_str(), bytes_bucket])
            .observe(*self.db_time.lock().unwrap());

        CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&[format!("{label}_db_first_row_time").as_str(), bytes_bucket])
            .observe(*self.db_first_row_time.lock().unwrap());

        CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&[format!("{label}_encode").as_str(), bytes_bucket])
            .observe(*self.encode_time.lock().unwrap());

        CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&[format!("{label}_json").as_str(), bytes_bucket])
            .observe(json_enconde_time);

        CLOUDBREAK_API_REQUEST_DURATION_MS
            .with_label_values(&[label.as_str(), bytes_bucket])
            .observe(total_time);

        CLOUDBREAK_API_CACHE_HIT_PERCENT
            .with_label_values(&[label.as_str(), bytes_bucket])
            .observe(cache_hit_percent);

        CLOUDBREAK_API_RESPONSE_BYTES_TOTAL
            .with_label_values(&[label.as_str(), "total"])
            .inc_by(json_bytes);

        CLOUDBREAK_API_RESPONSE_BYTES_TOTAL
            .with_label_values(&[label.as_str(), "cached"])
            .inc_by(cache_bytes);

        CLOUDBREAK_API_REQUESTS_BY_SUBSCRIPTION_ID
            .with_label_values(&[&subscription_id])
            .inc();

        CLOUDBREAK_API_DATA_FETCHED_BY_SUBSCRIPTION_ID
            .with_label_values(&[&subscription_id])
            .inc_by(json_bytes);
    }
}

pub fn batch_size_bucket(size: usize) -> &'static str {
    match size {
        1..=5 => "1-5",
        6..=10 => "6-10",
        11..=20 => "11-20",
        21..=50 => "21-50",
        51..=100 => "51-100",
        _ => "100+",
    }
}

pub fn bytes_bucket(bytes: u64) -> &'static str {
    match bytes {
        0..=1_000 => "0-1KB",
        1_001..=10_000 => "1-10KB",
        10_001..=100_000 => "10-100KB",
        100_001..=1_000_000 => "100KB-1MB",
        1_000_001..=10_000_000 => "1MB-10MB",
        10_000_001..=50_000_000 => "10MB-50MB",
        50_000_001..=100_000_000 => "50MB-100MB",
        100_000_001..=200_000_000 => "100MB-200MB",
        200_000_001..=500_000_000 => "200MB-500MB",
        _ => "500MB+",
    }
}
