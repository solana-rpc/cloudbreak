// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::time::Sleep;

use http_body_util::BodyExt;
use http_body_util::StreamBody;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Body;
use hyper::body::Bytes;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::net::TcpListener;
use tracing::Instrument;
use tracing::{error, info};

use crate::http::operational_endpoints;
use crate::http::rpc;
use crate::http::{CloudbreakRpcState, HeaderKeys, RequestContext};
use crate::metrics;
use crate::modules::bandwidth;

pub struct HttpServer {
    state: Arc<CloudbreakRpcState>,
    header_keys: Arc<HeaderKeys>,
}

impl HttpServer {
    pub fn new(state: CloudbreakRpcState, header_keys: HeaderKeys) -> Self {
        Self {
            state: Arc::new(state),
            header_keys: Arc::new(header_keys),
        }
    }

    pub async fn run(self, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("HTTP server listening on http://{}", addr);

        let state = self.state;
        let header_keys = self.header_keys;

        loop {
            let (stream, _remote_addr) = listener.accept().await?;

            let io = TokioIo::new(stream);
            let state = state.clone();
            let header_keys = header_keys.clone();

            tokio::spawn(async move {
                let start_time = Instant::now();
                let requests_in_connection = Arc::new(Mutex::new(0));
                let requests_in_connection_clone = requests_in_connection.clone();
                let _guard = metrics::InFlightRequestGuard::new("http_connection");

                let service = service_fn(move |req: Request<Incoming>| {
                    let state = state.clone();
                    let header_keys = header_keys.clone();

                    *requests_in_connection.lock().unwrap() += 1;

                    async move { handle_request(req, state, header_keys).await }
                });

                let connection_span = tracing::info_span!(
                    "http_connection",
                    requests_in_connection = 0,
                    wall_time = tracing::field::Empty
                );

                if let Err(err) = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .instrument(connection_span.clone())
                    .await
                {
                    error!("Error serving connection: {:?}", err);
                }

                connection_span.record(
                    "requests_in_connection",
                    *requests_in_connection_clone.lock().unwrap(),
                );
                connection_span.record("wall_time", start_time.elapsed().as_millis() as i64);

                metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                    .with_label_values(&["http_connection", "0"])
                    .observe(start_time.elapsed().as_millis() as f64);
            });
        }
    }
}

#[tracing::instrument(name = "http_request", parent = None, skip_all, fields(json_bytes = tracing::field::Empty))]
async fn handle_request(
    req: Request<Incoming>,
    state: Arc<CloudbreakRpcState>,
    header_keys: Arc<HeaderKeys>,
) -> Result<Response<UnsyncBoxBody<Bytes, Infallible>>, Infallible> {
    let request_start = Instant::now();
    let inflight_guard = metrics::InFlightRequestGuard::new("http");
    let request_timeout = state.request_timeout;

    let ctx = Arc::new(RequestContext::from_headers(req.headers(), &header_keys));

    // Resolve the per-client-IP bandwidth handle once per request; the transport
    // path is then a lock-free atomic add per frame. `None` when the module is
    // disabled.
    let bw = if bandwidth::is_enabled() {
        bandwidth::handle_for(&ctx.client_ip)
    } else {
        None
    };

    let handler_response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => rpc::handle_rpc_request(req, state, &ctx).await,
        (&Method::GET, "/metrics") => metrics::metrics_handler(&state.database)?,
        (&Method::GET, "/debug/log_filter") => operational_endpoints::log_filter_handler(&req)?,
        (&Method::GET, "/debug/modules/gpa_cache") => {
            operational_endpoints::gpa_cache_handler(&req, &state)?
        }
        _ => HttpHandlerResponse {
            status: StatusCode::NOT_FOUND,
            body: ResponseBody::Buffered(b"Not Found".to_vec()),
        },
    };

    let inner_body: UnsyncBoxBody<Bytes, Infallible> = match handler_response.body {
        ResponseBody::Buffered(bytes) => {
            let stream = futures::stream::once(async move { Ok(Frame::data(Bytes::from(bytes))) });
            BodyExt::boxed_unsync(StreamBody::new(stream))
        }
        ResponseBody::Streaming(body) => body,
    };

    // Child span of `http_request`. `wall_time` (full request + transport)
    // is filled in by `TrackedBody` when the body finishes streaming.
    let body_span = tracing::info_span!("http_body_transport", wall_time = tracing::field::Empty);

    let tracked = TrackedBody {
        inner: inner_body,
        start: request_start,
        bytes_count: 0,
        span: body_span,
        _inflight_guard: inflight_guard,
        deadline: Box::pin(tokio::time::sleep(request_timeout)),
        timed_out: false,
        bw,
    };

    let body = BodyExt::boxed_unsync(tracked);

    let response = Response::builder()
        .status(handler_response.status)
        .header("Content-Type", "application/json")
        .body(body)
        .unwrap();

    Ok(response)
}

pub struct HttpHandlerResponse {
    pub status: StatusCode,
    pub body: ResponseBody,
}

/// The body part of an `HttpHandlerResponse`.
///
/// `Buffered` is the conventional shape: a fully materialized `Vec<u8>` ready
/// to be wrapped in a single body frame.
///
/// `Streaming` is for endpoints that emit their response body incrementally —
/// the body is already a hyper-compatible `BoxBody` produced by the handler
/// (see `crate::http::streaming`).
pub enum ResponseBody {
    Buffered(Vec<u8>),
    Streaming(UnsyncBoxBody<Bytes, Infallible>),
}

/// Wraps the outgoing response body so we can observe the full
/// request-with-transport duration once the body has been fully transmitted
/// to the client.
///
/// - Holds the `InFlightRequestGuard` so the in-flight gauge stays incremented
///   through transport, not just the handler's await chain.
///
/// - If the client disconnects mid-body, `poll_frame` never returns
///   `Ready(None)`, so no metric is recorded
///
/// - When the per-request `deadline` fires, the body is truncated by
///   short-circuiting to `Ready(None)`. The client sees a chunked
///   transfer ending early (HTTP/1) or `END_STREAM` (HTTP/2). Same
///   user-visible semantics as the other mid-stream error paths in
///   `streaming.rs`.
struct TrackedBody<B> {
    inner: B,
    start: Instant,
    bytes_count: u64,
    span: tracing::Span,
    _inflight_guard: metrics::InFlightRequestGuard,
    /// This adds a defensive timeout that will trigger even if the client
    /// does not continue polling the body.
    deadline: Pin<Box<Sleep>>,
    timed_out: bool,
    /// Per-client-IP bandwidth accumulator handle, resolved once per request.
    /// `None` when the bandwidth module is disabled.
    bw: Option<bandwidth::BandwidthHandle>,
}

impl<B> Body for TrackedBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = unsafe { self.get_unchecked_mut() };

        // Always check the request deadline first. Polling here both
        // detects an already-fired deadline and registers the timer waker
        // so we get re-polled when it fires later (important when `inner`
        // is producing frames steadily and never returns Pending).
        if !this.timed_out && this.deadline.as_mut().poll(cx).is_ready() {
            this.timed_out = true;
            tracing::warn!(
                elapsed_ms = this.start.elapsed().as_millis() as i64,
                bytes_sent = this.bytes_count,
                "request timeout exceeded; truncating response body"
            );
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["http", "timeout"])
                .inc();

            return Poll::Ready(None);
        }

        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };

        match inner.poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let n = data.len() as u64;
                    this.bytes_count += n;
                    // Attribute bytes to the current 1s window (lock-free add).
                    if let Some(bw) = &this.bw {
                        bw.add(n);
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                let elapsed_ms = this.start.elapsed().as_millis() as i64;
                let bucket = metrics::bytes_bucket(this.bytes_count);
                metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                    .with_label_values(&["http_with_transport", bucket])
                    .observe(elapsed_ms as f64);
                this.span.record("wall_time", elapsed_ms);
                Poll::Ready(None)
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}
