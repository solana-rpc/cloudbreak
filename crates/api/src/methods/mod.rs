// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::ProcessedCommitmentBehavior;
use solana_commitment_config::CommitmentLevel;

use crate::error::RpcError;

pub mod genesis;
pub mod get_account_info;
pub mod get_balance;
pub mod get_largest_accounts;
pub mod get_multiple_accounts;
pub mod get_program_accounts;
pub mod get_supply;
pub mod get_token_account_balance;
pub mod get_token_accounts_by_delegate;
pub mod get_token_accounts_by_mint;
pub mod get_token_accounts_by_owner;
pub mod get_token_largest_accounts;
pub mod get_token_supply;
pub(crate) mod processed;
pub mod simulate_transaction;
pub mod slot;
pub mod version;
pub mod vote_accounts;

/// Resolves the requested commitment level according to the API readed config
/// on `processed-commitment`. `Confirmed` and `Finalized` pass through
/// unchanged. `Processed` is either rejected with an error (default) or
/// converted to `Confirmed`, depending on the configured behavior.
pub fn resolve_commitment(
    commitment: CommitmentLevel,
    processed_behavior: ProcessedCommitmentBehavior,
) -> Result<CommitmentLevel, RpcError> {
    match commitment {
        CommitmentLevel::Processed => match processed_behavior {
            ProcessedCommitmentBehavior::Reject => Err(RpcError::ProcessedCommitmentNotSupported),
            ProcessedCommitmentBehavior::UseConfirmed => Ok(CommitmentLevel::Confirmed),
        },
        other => Ok(other),
    }
}

pub type CloudbreakDbResult<T> = Result<T, RpcError>;
