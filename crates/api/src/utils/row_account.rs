// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! An account read from a database row without copying its data.

use solana_account::ReadableAccount;
use solana_pubkey::Pubkey;

/// An account whose data borrows the row buffer, so encoding needs no copy of the data. The row
/// must come from a bound query: only binary results can be read as `&[u8]`.
pub struct RowAccount<'a> {
    pub lamports: u64,
    pub data: &'a [u8],
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
}

impl ReadableAccount for RowAccount<'_> {
    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn data(&self) -> &[u8] {
        self.data
    }

    fn owner(&self) -> &Pubkey {
        &self.owner
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account::AccountSharedData;
    use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig, encode_ui_account};
    use std::sync::Arc;

    #[test]
    fn row_account_encodes_like_an_owned_account() {
        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let data: Vec<u8> = (0..=255).cycle().take(1000).collect();
        let owned = AccountSharedData::create_from_existing_shared_data(
            42,
            Arc::new(data.clone()),
            owner,
            true,
            7,
        );
        let borrowed = RowAccount {
            lamports: 42,
            data: &data,
            owner,
            executable: true,
            rent_epoch: 7,
        };
        let slice = Some(UiDataSliceConfig {
            offset: 10,
            length: 100,
        });

        for encoding in [
            UiAccountEncoding::Base58,
            UiAccountEncoding::Base64,
            UiAccountEncoding::Base64Zstd,
            UiAccountEncoding::JsonParsed,
        ] {
            for data_slice in [None, slice] {
                assert_eq!(
                    encode_ui_account(&pubkey, &borrowed, encoding, None, data_slice),
                    encode_ui_account(&pubkey, &owned, encoding, None, data_slice),
                );
            }
        }
    }
}
