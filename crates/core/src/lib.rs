// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::sync::OnceLock;

use anyhow::Result as AnyhowResult;

mod config;
pub mod metrics;
pub mod modules;

pub use config::*;
use tracing_subscriber::{EnvFilter, Registry};

pub type Result<T> = AnyhowResult<T>;

/// Used to reload the log filter
pub static LOG_FILTER_HANDLE: OnceLock<tracing_subscriber::reload::Handle<EnvFilter, Registry>> =
    OnceLock::new();
