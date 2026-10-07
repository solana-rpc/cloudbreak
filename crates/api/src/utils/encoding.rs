// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Account encoding checks.

use solana_account_decoder::{MAX_BASE58_BYTES, UiAccountEncoding, UiDataSliceConfig};
use solana_pubkey::Pubkey;

use crate::error::RpcError;

/// Agave's `encode_account` size check (`rpc/src/rpc.rs`): `binary` and `base58` reject data,
/// after `dataSlice`, larger than `MAX_BASE58_BYTES` (128) with `Base58DataTooLarge`.
pub fn check_account_data_len_for_encoding(
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    account_data_length: usize,
    pubkey: &Pubkey,
) -> Result<(), RpcError> {
    if (encoding == UiAccountEncoding::Binary || encoding == UiAccountEncoding::Base58)
        && data_slice
            .map(|s| core::cmp::min(s.length, account_data_length.saturating_sub(s.offset)))
            .unwrap_or(account_data_length)
            > MAX_BASE58_BYTES
    {
        tracing::debug!("Account {pubkey} data is too large for base58 encoding");

        return Err(RpcError::Base58DataTooLarge);
    }

    Ok(())
}
