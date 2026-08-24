// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Error types for the query tracker, plus their mapping to HTTP status codes.

use hyper::StatusCode;
use sea_orm::DbErr;

#[derive(thiserror::Error, Debug)]
pub enum QueryTrackerError {
    #[error("database error: {0}")]
    Database(#[from] DbErr),
    #[error("invalid request: {0}")]
    BadRequest(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl QueryTrackerError {
    /// HTTP status this error maps to on the wire.
    pub fn status(&self) -> StatusCode {
        match self {
            QueryTrackerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            QueryTrackerError::Database(_) | QueryTrackerError::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

pub type QueryTrackerResult<T> = Result<T, QueryTrackerError>;
