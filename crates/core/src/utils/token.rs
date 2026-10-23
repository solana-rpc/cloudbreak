// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Token program ids, token account layout offsets, and [`MintState`], the mint data that
//! `jsonParsed` needs.

use solana_account_decoder::parse_account_data::{
    AccountAdditionalDataV3, SplTokenAdditionalDataV2,
};
use solana_pubkey::Pubkey;
use spl_token_2022_interface::extension::interest_bearing_mint::InterestBearingConfig;
use spl_token_2022_interface::extension::scaled_ui_amount::ScaledUiAmountConfig;
use spl_token_2022_interface::extension::{
    BaseStateWithExtensions, ExtensionType, StateWithExtensions,
};
use spl_token_2022_interface::state::Mint;

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

/// The mint data `jsonParsed` needs to encode a token account: the decimals and the
/// interest-bearing and scaled-UI configs, plus whether they can ever change. It is computed once
/// from the mint account bytes and keeps nothing else, so it is cheap to cache and to copy.
/// [`MintState::additional_data`] turns it into the `AccountAdditionalDataV3` the Agave encoder takes.
#[derive(Debug, Clone, PartialEq)]
pub struct MintState {
    pub decimals: u8,
    /// `None` for the ~99.99% of mints without a config.
    pub configs: Option<Box<MintConfigs>>,
    /// An inmutable mint means nothing that `jsonParsed` reads can change.
    /// [`MintState::from_mint`] sets it from the data: `false` with `MintCloseAuthority`,
    /// `InterestBearingConfig` or `ScaledUiAmount`, else `true`.
    /// A `true` is only confirmed by [`MintState::classify`] with the mint's program owner.
    pub immutable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MintConfigs {
    pub interest_bearing: Option<InterestBearingConfig>,
    pub scaled_ui_amount: Option<ScaledUiAmountConfig>,
}

impl MintState {
    /// The state of the mint at `pubkey`, as Agave reads it: the native mint has fixed decimals,
    /// any other mint must unpack. `None` when the data is not an initialized mint.
    pub fn from_mint(pubkey: &Pubkey, data: &[u8]) -> Option<Self> {
        if pubkey == &spl_token_interface::native_mint::id() {
            return Some(Self {
                decimals: spl_token_interface::native_mint::DECIMALS,
                configs: None,
                immutable: true,
            });
        }
        let mint = StateWithExtensions::<Mint>::unpack(data).ok()?;
        let interest_bearing = mint.get_extension::<InterestBearingConfig>().ok().copied();
        let scaled_ui_amount = mint.get_extension::<ScaledUiAmountConfig>().ok().copied();
        let configs = (interest_bearing.is_some() || scaled_ui_amount.is_some()).then(|| {
            Box::new(MintConfigs {
                interest_bearing,
                scaled_ui_amount,
            })
        });
        let immutable = mint.get_extension_types().is_ok_and(|types| {
            !types.iter().any(|t| {
                matches!(
                    t,
                    ExtensionType::MintCloseAuthority
                        | ExtensionType::InterestBearingConfig
                        | ExtensionType::ScaledUiAmount
                )
            })
        });
        Some(Self {
            decimals: mint.base.decimals,
            configs,
            immutable,
        })
    }

    /// Confirms `immutable` with the mint's owner. Only a token program enforces the mint
    /// layout; any other owner can rewrite the data, so its mint is never immutable.
    pub fn classify(mut self, owner: &Pubkey) -> Self {
        self.immutable &= is_token_program(owner);
        self
    }

    /// The `jsonParsed` mint data, with the configs evaluated at `block_time`.
    pub fn additional_data(&self, block_time: i64) -> AccountAdditionalDataV3 {
        let configs = self.configs.as_deref();
        AccountAdditionalDataV3 {
            spl_token_additional_data: Some(SplTokenAdditionalDataV2 {
                decimals: self.decimals,
                interest_bearing_config: configs
                    .and_then(|c| c.interest_bearing)
                    .map(|c| (c, block_time)),
                scaled_ui_amount_config: configs
                    .and_then(|c| c.scaled_ui_amount)
                    .map(|c| (c, block_time)),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::mint_cache::tests::{legacy_mint, token_2022_mint};

    #[test]
    fn the_native_mint_has_fixed_decimals_without_data() {
        let native = MintState::from_mint(&spl_token_interface::native_mint::id(), &[]).unwrap();
        assert_eq!(native.decimals, spl_token_interface::native_mint::DECIMALS);
        assert!(native.configs.is_none());
        assert!(MintState::from_mint(&Pubkey::new_unique(), &[]).is_none());
    }

    #[test]
    fn from_mint_keeps_decimals_and_configs_only() {
        let key = Pubkey::new_unique();
        let legacy = MintState::from_mint(&key, &legacy_mint(9)).unwrap();
        assert_eq!((legacy.decimals, legacy.configs.is_none()), (9, true));

        let scaled = token_2022_mint(&[ExtensionType::ScaledUiAmount], 2.0);
        let configs = MintState::from_mint(&key, &scaled)
            .unwrap()
            .configs
            .unwrap();
        assert!(configs.scaled_ui_amount.is_some() && configs.interest_bearing.is_none());
        assert!(MintState::from_mint(&key, &[0u8; 82]).is_none());
    }

    #[test]
    fn immutable_comes_from_the_extensions_and_is_confirmed_by_the_owner() {
        let key = Pubkey::new_unique();
        let parse = |data: &[u8]| MintState::from_mint(&key, data).unwrap();
        assert!(parse(&legacy_mint(6)).immutable);
        assert!(parse(&token_2022_mint(&[ExtensionType::MetadataPointer], 0.0)).immutable);
        assert!(!parse(&token_2022_mint(&[ExtensionType::MintCloseAuthority], 0.0)).immutable);
        assert!(!parse(&token_2022_mint(&[ExtensionType::ScaledUiAmount], 2.0)).immutable);

        assert!(
            parse(&legacy_mint(6))
                .classify(&LEGACY_TOKEN_PROGRAM_ID)
                .immutable
        );
        assert!(
            !parse(&legacy_mint(6))
                .classify(&Pubkey::new_unique())
                .immutable
        );
    }

    #[test]
    fn additional_data_evaluates_configs_at_the_block_time() {
        let scaled = token_2022_mint(&[ExtensionType::ScaledUiAmount], 2.0);
        let mint = MintState::from_mint(&Pubkey::new_unique(), &scaled).unwrap();
        let token = mint
            .additional_data(1_700_000_000)
            .spl_token_additional_data
            .unwrap();
        assert_eq!(token.decimals, 6);
        assert_eq!(token.scaled_ui_amount_config.unwrap().1, 1_700_000_000);
        assert!(token.interest_bearing_config.is_none());
    }
}
