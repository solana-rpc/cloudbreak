// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::DbErr;

#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] DbErr),
    #[error("Invalid parameters")]
    InvalidParams,
    #[error("Invalid request")]
    InvalidRequest,
    #[error("Internal error")]
    InternalError,
    #[error("Pubkey validation error")]
    PubkeyValidationError(String),
    #[error("Parse error")]
    ParseError,
    #[error("RPC slot ({rpc_slot}) is behind the min context slot provided")]
    RpcSlotBehindMinContextSlot { rpc_slot: u64 },
    #[error("Subscription ID not found in extensions")]
    SubscriptionIdNotFound,
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
    #[error("Invalid param: could not find mint ({mint})")]
    MintDataNotFound { mint: String },
}

impl RpcError {
    pub const fn to_error_code(&self) -> &'static str {
        match self {
            RpcError::DatabaseError(db_err) => match db_err {
                DbErr::ConnectionAcquire(_) => "DB_POOL_EXHAUSTED",
                DbErr::TryIntoErr { .. } => "DB_TRY_INTO_ERROR",
                DbErr::Conn(_) => "DB_CONNECTION_ERROR",
                DbErr::Exec(_) => "DB_EXECUTION_ERROR",
                DbErr::Query(_) => "DB_QUERY_ERROR",
                DbErr::ConvertFromU64(_) => "DB_U64_CONVERSION_ERROR",
                DbErr::UnpackInsertId => "DB_UNPACK_INSERT_ID",
                DbErr::UpdateGetPrimaryKey => "DB_UPDATE_PK_ERROR",
                DbErr::RecordNotFound(_) => "DB_RECORD_NOT_FOUND",
                DbErr::AttrNotSet(_) => "DB_ATTRIBUTE_NOT_SET",
                DbErr::Custom(_) => "DB_CUSTOM_ERROR",
                DbErr::Type(_) => "DB_TYPE_ERROR",
                DbErr::Json(_) => "DB_JSON_ERROR",
                DbErr::Migration(_) => "DB_MIGRATION_ERROR",
                DbErr::RecordNotInserted => "DB_NOT_INSERTED",
                DbErr::RecordNotUpdated => "DB_NOT_UPDATED",
            },
            RpcError::InvalidRequest => "INVALID_REQUEST",
            RpcError::InvalidParams => "INVALID_PARAMS",
            RpcError::InternalError => "INTERNAL_ERROR",
            RpcError::PubkeyValidationError(_) => "PUBKEY_VALIDATION_ERROR",
            RpcError::ParseError => "PARSE_ERROR",
            RpcError::RpcSlotBehindMinContextSlot { rpc_slot: _ } => {
                "RPC_SLOT_BEHIND_MIN_CONTEXT_SLOT"
            }
            RpcError::SubscriptionIdNotFound => "SUBSCRIPTION_ID_NOT_FOUND",
            RpcError::InvalidParamsWithMessage(_) => "INVALID_PARAMS_WITH_MESSAGE",
            RpcError::KeyExcludedFromSecondaryIndex { .. } => "KEY_EXCLUDED_FROM_SECONDARY_INDEX",
            RpcError::ProcessedCommitmentNotSupported => "PROCESSED_COMMITMENT_NOT_SUPPORTED",
            RpcError::NodeUnhealthy { .. } => "NODE_UNHEALTHY",
            RpcError::AccountOwnerExcluded { .. } => "ACCOUNT_OWNER_EXCLUDED",
            RpcError::AccountNotFound { .. } => "Invalid param: could not find account",
            RpcError::NotATokenAccount { .. } => "Invalid param: not a Token account",
            RpcError::MintDataNotFound { .. } => "Invalid param: could not find mint",
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
            RpcError::RpcSlotBehindMinContextSlot { .. } => -32000,
            RpcError::SubscriptionIdNotFound => -32001,
            RpcError::InvalidParamsWithMessage(_) => -32602,
            RpcError::KeyExcludedFromSecondaryIndex { .. } => -32010,
            RpcError::ProcessedCommitmentNotSupported => -32003,
            RpcError::NodeUnhealthy { .. } => -32005,
            RpcError::AccountOwnerExcluded { .. } => -32010,
            RpcError::AccountNotFound { .. } => -32602,
            RpcError::NotATokenAccount { .. } => -32602,
            RpcError::MintDataNotFound { .. } => -32602,
        }
    }
}
