// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::DbErr;

#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("Internal error")]
    DatabaseError(#[from] DbErr),
    #[error("Invalid params")]
    InvalidParams,
    #[error("Invalid request")]
    InvalidRequest,
    #[error("Internal error")]
    InternalError,
    /// Holds the `Debug` form of the parse error, as Agave's `verify_pubkey` does.
    #[error("Invalid param: {0}")]
    PubkeyValidationError(String),
    #[error("Parse error")]
    ParseError,
    #[error("RPC slot ({rpc_slot}) is behind the min context slot provided")]
    RpcSlotBehindMinContextSlot { rpc_slot: u64 },
    #[error("{0}")]
    InvalidParamsWithMessage(String),
    #[error("{key} excluded from account secondary indexes; this RPC method unavailable for key")]
    KeyExcludedFromSecondaryIndex { key: String },
    #[error("Processed commitment level is not supported")]
    ProcessedCommitmentNotSupported,
    #[error("Node is unhealthy")]
    NodeUnhealthy {
        /// When `true`, this is surfaced as an HTTP `503 Service Unavailable`
        /// response instead of a `200 OK` JSON-RPC error (see the
        /// `unhealthy-response` config option). Decided at the error site from
        /// [`CloudbreakRpcState::unhealthy_response`].
        service_unavailable: bool,
    },
    #[error(
        "Account {pubkey} is owned by {owner}, which is excluded from this indexer's program filter; cannot serve this account"
    )]
    AccountOwnerExcluded { pubkey: String, owner: String },
    /// Matches Agave's response for missing accounts in token-account RPCs
    #[error("Invalid param: could not find account ({pubkey})")]
    AccountNotFound { pubkey: String },
    /// Matches Agave's response when the account exists but is not owned by
    /// SPL Token / Token-2022 in token-account RPCs.
    #[error("Invalid param: not a Token account ({pubkey})")]
    NotATokenAccount { pubkey: String },
    /// Matches Agave's response when the mint account exists but is not owned
    /// by SPL Token / Token-2022 in mint-oriented RPCs.
    #[error("Invalid param: not a Token mint ({mint})")]
    NotATokenMint { mint: String },
    #[error("Invalid param: could not find mint ({mint})")]
    MintDataNotFound { mint: String },
    /// The requested RPC method is not enabled in this node's API config.
    #[error("Method not found")]
    MethodNotFound,
}

impl RpcError {
    /// JSON-RPC error `data` member. `None` omits the field from the response.
    pub fn to_error_data(&self) -> Option<serde_json::Value> {
        None
    }

    pub fn to_numeric_code(&self) -> i32 {
        match self {
            RpcError::DatabaseError(_) => -32603,
            RpcError::InvalidRequest => -32600,
            RpcError::InvalidParams => -32602,
            RpcError::InternalError => -32603,
            RpcError::PubkeyValidationError(_) => -32602,
            RpcError::ParseError => -32700,
            RpcError::RpcSlotBehindMinContextSlot { .. } => -32000,
            RpcError::InvalidParamsWithMessage(_) => -32602,
            RpcError::KeyExcludedFromSecondaryIndex { .. } => -32010,
            RpcError::ProcessedCommitmentNotSupported => -32003,
            RpcError::NodeUnhealthy { .. } => -32005,
            RpcError::AccountOwnerExcluded { .. } => -32010,
            RpcError::AccountNotFound { .. } => -32602,
            RpcError::NotATokenAccount { .. } => -32602,
            RpcError::NotATokenMint { .. } => -32602,
            RpcError::MintDataNotFound { .. } => -32602,
            RpcError::MethodNotFound => -32601,
        }
    }
}
