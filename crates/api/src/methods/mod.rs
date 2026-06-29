// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use cloudbreak_core::ProcessedCommitmentBehavior;
use cloudbreak_core::modules::rpc_filter_type::RpcFilterType;

use crate::error::RpcError;

pub mod genesis;
pub mod get_account_info;
pub mod get_balance;
pub mod get_multiple_accounts;
pub mod get_token_account_balance;
pub mod get_token_largest_accounts;
pub mod mint;
pub mod mint_accounts;
pub mod program;
pub mod slot;
pub mod token;
pub mod version;

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

pub const LEGACY_TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const TOKEN_2022_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

pub fn is_token_program(program: &Pubkey) -> bool {
    program == &LEGACY_TOKEN_PROGRAM_ID || program == &TOKEN_2022_PROGRAM_ID
}

pub type CloudbreakDbResult<T> = Result<T, RpcError>;

pub struct SqlDataSliceFilter<'a> {
    pub filter_type: &'a RpcFilterType,
    pub table: &'a str,
    pub is_token_program: bool,
}

impl<'a> SqlDataSliceFilter<'a> {
    /// The token program flag is used to later transform the memcmp filter into a token owner or mint filter that can be used to index the DB
    pub fn new(filter_type: &'a RpcFilterType, table: &'a str, is_token_program: bool) -> Self {
        Self {
            filter_type,
            table,
            is_token_program,
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self.filter_type {
            RpcFilterType::DataSize(size) => {
                Some(format!("length({}.data) = {}", self.table, size))
            }
            RpcFilterType::Memcmp(memcmp) => {
                let bytes = memcmp.bytes()?.to_vec();
                let offset = memcmp.offset();

                if self.is_token_program {
                    let is_token_filter = offset == 32 && bytes.len() == 32;
                    let is_mint_filter = offset == 0 && bytes.len() == 32;

                    if is_token_filter {
                        return Some(format!(
                            "{}.token_owner = '\\x{}'::bytea",
                            self.table,
                            hex::encode(bytes)
                        ));
                    } else if is_mint_filter {
                        return Some(format!(
                            "{}.token_mint = '\\x{}'::bytea",
                            self.table,
                            hex::encode(bytes)
                        ));
                    }
                }

                Some(format!(
                    "SUBSTRING({}.data FROM {} FOR {}) = E'\\\\x{}'::bytea",
                    self.table,
                    memcmp.offset() + 1,
                    bytes.len(),
                    hex::encode(bytes)
                ))
            }
            RpcFilterType::TokenAccountState => Some(format!(
                "(length({0}.data) = 165 OR (length({0}.data) > 165 AND SUBSTRING({0}.data FROM 166 FOR 1) = E'\\\\x02'::bytea))",
                self.table
            )),
            RpcFilterType::ValueCmp(_) => None,
        }
    }
}
