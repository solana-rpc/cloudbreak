// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::DbErr;
use solana_account_decoder::MAX_BASE58_BYTES;
use solana_rpc_client_api::custom_error::{MinContextSlotNotReachedErrorData, NodeUnhealthyErrorData};

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
    #[error("Minimum context slot has not been reached")]
    MinContextSlotNotReached { context_slot: u64 },
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
    // Token errors carry the key for logs only; the messages are Agave's exact text.
    #[error("Invalid param: could not find account")]
    AccountNotFound { pubkey: String },
    #[error("Invalid param: not a Token account")]
    NotATokenAccount { pubkey: String },
    #[error("Invalid param: not a Token mint")]
    NotATokenMint { mint: String },
    #[error("Invalid param: could not find mint")]
    MintDataNotFound { mint: String },
    #[error("Invalid param: unrecognized Token program id")]
    UnrecognizedTokenProgramId { program_id: String },
    /// `getTokenSupply`: the mint account data does not unpack as a mint.
    #[error("Invalid param: mint could not be unpacked")]
    MintCouldNotBeUnpacked { mint: String },
    /// `getTokenAccountBalance` / `getTokenLargestAccounts`: the mint data does not unpack.
    #[error("Invalid param: Token mint could not be unpacked")]
    TokenMintCouldNotBeUnpacked { mint: String },
    /// Matches Agave's `encode_account` limit for `binary` / `base58` encodings.
    #[error(
        "Encoded binary (base 58) data should be less than {} bytes, please use Base64 encoding.",
        MAX_BASE58_BYTES
    )]
    Base58DataTooLarge,
    /// The requested RPC method is not enabled in this node's API config.
    #[error("Method not found")]
    MethodNotFound,
}

impl RpcError {
    /// JSON-RPC error `data` member. `None` omits the field from the response.
    pub fn to_error_data(&self) -> Option<serde_json::Value> {
        match self {
            RpcError::MinContextSlotNotReached { context_slot } => {
                serde_json::to_value(MinContextSlotNotReachedErrorData {
                    context_slot: *context_slot,
                })
                .ok()
            }
            // The indexer health flag has no slot distance, so it is always unknown.
            RpcError::NodeUnhealthy { .. } => serde_json::to_value(NodeUnhealthyErrorData {
                num_slots_behind: None,
            })
            .ok(),
            _ => None,
        }
    }

    pub fn to_numeric_code(&self) -> i32 {
        match self {
            RpcError::DatabaseError(_) => -32603,
            RpcError::InvalidRequest => -32600,
            RpcError::InvalidParams => -32602,
            RpcError::InternalError => -32603,
            RpcError::PubkeyValidationError(_) => -32602,
            RpcError::ParseError => -32700,
            RpcError::MinContextSlotNotReached { .. } => -32016,
            RpcError::InvalidParamsWithMessage(_) => -32602,
            RpcError::KeyExcludedFromSecondaryIndex { .. } => -32010,
            RpcError::ProcessedCommitmentNotSupported => -32602,
            RpcError::NodeUnhealthy { .. } => -32005,
            RpcError::AccountOwnerExcluded { .. } => -32010,
            RpcError::AccountNotFound { .. } => -32602,
            RpcError::NotATokenAccount { .. } => -32602,
            RpcError::NotATokenMint { .. } => -32602,
            RpcError::MintDataNotFound { .. } => -32602,
            RpcError::UnrecognizedTokenProgramId { .. } => -32602,
            RpcError::MintCouldNotBeUnpacked { .. } => -32602,
            RpcError::TokenMintCouldNotBeUnpacked { .. } => -32602,
            RpcError::Base58DataTooLarge => -32600,
            RpcError::MethodNotFound => -32601,
        }
    }
}
