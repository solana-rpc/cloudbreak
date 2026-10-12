// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Token program ids, token account layout offsets, and mint data for `jsonParsed`.

use solana_account_decoder::parse_account_data::{
    AccountAdditionalDataV3, SplTokenAdditionalDataV2,
};
use solana_pubkey::Pubkey;
use spl_token_2022::extension::{BaseStateWithExtensions, StateWithExtensions};
use spl_token_2022::state::Mint;
use spl_token_2022_interface::extension::interest_bearing_mint::InterestBearingConfig;
use spl_token_2022_interface::extension::scaled_ui_amount::ScaledUiAmountConfig;

pub const LEGACY_TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const TOKEN_2022_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

pub fn is_token_program(program: &Pubkey) -> bool {
    program == &LEGACY_TOKEN_PROGRAM_ID || program == &TOKEN_2022_PROGRAM_ID
}

/// Token account layout offsets the token filters and indexes are built on.
pub const TOKEN_ACCOUNT_MINT_OFFSET: usize = 0;
pub const TOKEN_ACCOUNT_OWNER_OFFSET: usize = 32;
pub const TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET: usize = 72;
pub const TOKEN_ACCOUNT_DELEGATE_OFFSET: usize = 76;

pub fn parse_additional_mint_data(
    mint_pubkey: &Pubkey,
    mint_data: &[u8],
    block_time: i64,
) -> Option<AccountAdditionalDataV3> {
    if mint_pubkey == &spl_token_interface::native_mint::id() {
        return Some(AccountAdditionalDataV3 {
            spl_token_additional_data: Some(SplTokenAdditionalDataV2::with_decimals(
                spl_token_interface::native_mint::DECIMALS,
            )),
        });
    }

    let mint = StateWithExtensions::<Mint>::unpack(mint_data).ok()?;
    let decimals = mint.base.decimals;
    let interest_bearing_config = mint.get_extension::<InterestBearingConfig>().cloned().ok();
    let scaled_ui_amount_config = mint.get_extension::<ScaledUiAmountConfig>().cloned().ok();

    Some(AccountAdditionalDataV3 {
        spl_token_additional_data: Some(SplTokenAdditionalDataV2 {
            decimals,
            interest_bearing_config: interest_bearing_config.map(|i| (i, block_time)),
            scaled_ui_amount_config: scaled_ui_amount_config.map(|s| (s, block_time)),
        }),
    })
}
