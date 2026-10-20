// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The mint data `jsonParsed` needs (decimals and the interest-bearing and scaled-UI configs),
//! kept in memory. [`MintCache`] is the module's handle. Its indexer half writes the `mints`
//! table; its API half loads it and follows the confirmed slots. See the "Mint cache" section of
//! the README.

mod api;
mod db;
mod indexer;
mod operational_endpoints;
mod store;

pub use api::MintCacheApi;
pub use indexer::MintCacheIndexer;
pub use operational_endpoints::{MintCacheApiSummary, MintCacheIndexerSummary, MintEntryDebug};
pub use store::{ChangeKind, MintStore, MutableMint, StoredMint};

use solana_pubkey::Pubkey;

use crate::config::MintCacheConfig;
use crate::utils::token::{MintState, is_token_program};

/// The module's handle: an API half and an indexer half, each a null-object handle. Each binary
/// builds it with its own constructor, which enables only its half.
#[derive(Clone, Default)]
pub struct MintCache {
    api: MintCacheApi,
    indexer: MintCacheIndexer,
}

impl MintCache {
    /// The API handle: the API half is enabled when the section is present with `enabled = true`.
    pub fn from_api_config(config: Option<&MintCacheConfig>) -> Self {
        Self {
            api: MintCacheApi::from_config(config),
            indexer: MintCacheIndexer::default(),
        }
    }

    /// The indexer handle: the indexer half is enabled when the section is present with
    /// `enabled = true`. The caller checks the section's required modules first.
    pub fn from_indexer_config(config: Option<&MintCacheConfig>) -> Self {
        Self {
            api: MintCacheApi::default(),
            indexer: MintCacheIndexer::from_config(config),
        }
    }

    pub fn api(&self) -> &MintCacheApi {
        &self.api
    }

    pub fn indexer(&self) -> &MintCacheIndexer {
        &self.indexer
    }

    pub fn is_enabled(&self) -> bool {
        self.api.is_enabled() || self.indexer.is_enabled()
    }
}

/// One account write as the mint store sees it.
#[derive(Debug, Clone, PartialEq)]
pub enum MintChange {
    Live(MintState),
    /// A zero-lamport write. It closes the mint when the key is one.
    Closed,
}

impl MintChange {
    /// What one account write means to the mint store: every zero-lamport write may close a
    /// mint, and a token-program account that parses as a mint is one. `None` for anything else.
    fn parse(pubkey: &Pubkey, owner: &Pubkey, lamports: u64, data: &[u8]) -> Option<Self> {
        if lamports == 0 {
            return Some(Self::Closed);
        }
        if !is_token_program(owner) {
            return None;
        }
        let state = MintState::from_mint(pubkey, data)?.classify(owner);
        Some(Self::Live(state))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::utils::token::{LEGACY_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
    use spl_token_2022_interface::extension::scaled_ui_amount::ScaledUiAmountConfig;
    use spl_token_2022_interface::extension::{
        BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
    };
    use spl_token_2022_interface::state::Mint;

    const MINT_LEN: usize = 82;
    const TOKEN_ACCOUNT_LEN: usize = 165;

    /// An initialized 82-byte mint with `decimals`.
    pub(crate) fn legacy_mint(decimals: u8) -> Vec<u8> {
        let mut data = vec![0u8; MINT_LEN];
        data[44] = decimals;
        data[45] = 1;
        data
    }

    /// An initialized Token-2022 mint with `extensions`. A scaled-UI config gets `multiplier`.
    pub(crate) fn token_2022_mint(extensions: &[ExtensionType], multiplier: f64) -> Vec<u8> {
        let len = ExtensionType::try_calculate_account_len::<Mint>(extensions).unwrap();
        let mut data = vec![0u8; len];
        let mut mint = StateWithExtensionsMut::<Mint>::unpack_uninitialized(&mut data).unwrap();
        for extension in extensions {
            match extension {
                ExtensionType::MintCloseAuthority => {
                    mint.init_extension::<spl_token_2022_interface::extension::mint_close_authority::MintCloseAuthority>(true).unwrap();
                }
                ExtensionType::MetadataPointer => {
                    mint.init_extension::<spl_token_2022_interface::extension::metadata_pointer::MetadataPointer>(true).unwrap();
                }
                ExtensionType::ScaledUiAmount => {
                    let config = mint.init_extension::<ScaledUiAmountConfig>(true).unwrap();
                    config.multiplier = multiplier.into();
                }
                other => panic!("unsupported test extension {other:?}"),
            }
        }
        mint.base.decimals = 6;
        mint.base.is_initialized = true;
        mint.pack_base();
        mint.init_account_type().unwrap();
        data
    }

    /// The state `MintChange::parse` keeps for one live write by `owner`.
    fn live(owner: Pubkey, data: Vec<u8>) -> Option<MintState> {
        match MintChange::parse(&Pubkey::new_unique(), &owner, 1, &data) {
            Some(MintChange::Live(state)) => Some(state),
            _ => None,
        }
    }

    #[test]
    fn classifies_mints_by_owner_and_extensions() {
        let legacy = live(LEGACY_TOKEN_PROGRAM_ID, legacy_mint(9)).unwrap();
        assert_eq!(legacy.decimals, 9);
        assert!(legacy.immutable && legacy.configs.is_none());

        let plain = token_2022_mint(&[ExtensionType::MetadataPointer], 0.0);
        assert!(live(TOKEN_2022_PROGRAM_ID, plain).unwrap().immutable);

        let closable = token_2022_mint(&[ExtensionType::MintCloseAuthority], 0.0);
        assert!(!live(TOKEN_2022_PROGRAM_ID, closable).unwrap().immutable);

        let scaled = token_2022_mint(&[ExtensionType::ScaledUiAmount], 2.0);
        let mint = live(TOKEN_2022_PROGRAM_ID, scaled).unwrap();
        assert!(!mint.immutable);
        let multiplier: f64 = mint
            .configs
            .unwrap()
            .scaled_ui_amount
            .unwrap()
            .multiplier
            .into();
        assert_eq!(multiplier, 2.0);
    }

    #[test]
    fn rejects_non_mints() {
        // Wrong owner, a token account, an uninitialized mint.
        assert!(live(Pubkey::new_unique(), legacy_mint(6)).is_none());
        assert!(live(LEGACY_TOKEN_PROGRAM_ID, vec![0u8; TOKEN_ACCOUNT_LEN]).is_none());
        assert!(live(LEGACY_TOKEN_PROGRAM_ID, vec![0u8; MINT_LEN]).is_none());
    }

    #[test]
    fn every_zero_lamport_write_is_a_close() {
        let any = Pubkey::new_unique();
        assert_eq!(
            MintChange::parse(&any, &Pubkey::default(), 0, &[]),
            Some(MintChange::Closed)
        );
    }
}
