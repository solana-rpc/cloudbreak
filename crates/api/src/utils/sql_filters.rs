// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Translates gPA filters into SQL predicates on `accounts` and `snapshot_accounts`.

use cloudbreak_core::modules::rpc_filter_type::{Memcmp, RpcFilterType};
use solana_pubkey::Pubkey;

use crate::error::RpcError;
use cloudbreak_core::utils::token::{
    TOKEN_ACCOUNT_DELEGATE_OFFSET, TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET, TOKEN_ACCOUNT_MINT_OFFSET,
    TOKEN_ACCOUNT_OWNER_OFFSET, is_token_program,
};

/// The SQL filter clauses of one gPA, for both tables, and its metric label.
#[derive(Debug, PartialEq, Eq)]
pub struct SqlFilters {
    pub accounts: String,
    pub snapshot: String,
    /// Values bound to `$1..$n` in the clauses, shared by both tables.
    pub params: Vec<Vec<u8>>,
    pub label: String,
    /// True for a mint query with an owner-prefix range. The right index depends on the bound
    /// values, so the statement must be planned with them on every run.
    pub custom_plan: bool,
}

/// The indexed key a token-program gPA filters on. The first one present, in this order, names
/// the label suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenKey {
    Mint,
    Delegate,
    Owner,
}

impl SqlFilters {
    /// Verifies the filters and translates them to SQL.
    ///
    /// On a token program the query must filter on an indexed key: a 32-byte memcmp on the mint,
    /// the delegate or the owner. Those memcmps and the delegate tag become the token index
    /// expressions, and a shorter owner memcmp becomes a range on the owner column. A delegate
    /// filter without a tag gets the tag (known difference, see the README). A `gpa` label gets
    /// the key as suffix (`gpa_mint`, `gpa_delegate`, `gpa_token`); any other label is kept.
    pub fn build(
        program: &Pubkey,
        filters: &[RpcFilterType],
        label: &str,
    ) -> Result<Self, RpcError> {
        for filter in filters {
            filter.verify().map_err(|e| {
                RpcError::InvalidParamsWithMessage(format!(
                    "Invalid param: {}",
                    e.invalid_param_text()
                ))
            })?;
        }

        let is_token_program = is_token_program(program);
        let mut label = label.to_string();
        let mut add_delegate_tag = false;
        let mut custom_plan = false;
        if is_token_program {
            let key = is_valid_token_query(filters)?;
            if label == "gpa" {
                label.push_str(match key {
                    TokenKey::Mint => "_mint",
                    TokenKey::Delegate => "_delegate",
                    TokenKey::Owner => "_token",
                });
            }
            add_delegate_tag = has_memcmp(filters, TOKEN_ACCOUNT_DELEGATE_OFFSET, Some(32))
                && !has_memcmp(filters, TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET, None);
            custom_plan = key == TokenKey::Mint && has_owner_prefix(filters);
        }

        let delegate_tag = RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET,
            vec![1, 0, 0, 0],
        ));
        let mut accounts = Vec::new();
        let mut snapshot = Vec::new();
        let mut params = Vec::new();
        for filter in filters
            .iter()
            .chain(add_delegate_tag.then_some(&delegate_tag))
        {
            let param = params.len() + 1;
            let Some((clause, values)) = filter_sql(filter, "accounts", is_token_program, param)
            else {
                continue;
            };
            accounts.push(format!("AND {clause}"));
            if let Some((clause, _)) =
                filter_sql(filter, "snapshot_accounts", is_token_program, param)
            {
                snapshot.push(format!("AND {clause}"));
            }
            params.extend(values);
        }

        Ok(Self {
            accounts: accounts.join("\n                    "),
            snapshot: snapshot.join("\n                    "),
            params,
            label,
            custom_plan,
        })
    }
}

/// The indexed key of a token-program query, or `InvalidParams` without one.
fn is_valid_token_query(filters: &[RpcFilterType]) -> Result<TokenKey, RpcError> {
    let mut keys = Vec::new();
    for filter in filters {
        if let RpcFilterType::Memcmp(memcmp) = filter {
            let bytes = memcmp.bytes().ok_or(RpcError::InvalidParams)?;
            if bytes.len() != 32 {
                continue;
            }
            keys.extend(match memcmp.offset() {
                TOKEN_ACCOUNT_MINT_OFFSET => Some(TokenKey::Mint),
                TOKEN_ACCOUNT_DELEGATE_OFFSET => Some(TokenKey::Delegate),
                TOKEN_ACCOUNT_OWNER_OFFSET => Some(TokenKey::Owner),
                _ => None,
            });
        }
    }
    [TokenKey::Mint, TokenKey::Delegate, TokenKey::Owner]
        .into_iter()
        .find(|key| keys.contains(key))
        .ok_or(RpcError::InvalidParams)
}

/// True when a memcmp sits at `offset`, of `len` bytes when given.
fn has_memcmp(filters: &[RpcFilterType], offset: usize, len: Option<usize>) -> bool {
    filters.iter().any(|filter| {
        matches!(filter, RpcFilterType::Memcmp(memcmp)
            if memcmp.offset() == offset
                && memcmp.bytes().is_some_and(|b| len.is_none_or(|len| b.len() == len)))
    })
}

/// True when a memcmp at the owner offset holds only the first bytes of an owner.
fn is_owner_prefix(offset: usize, bytes: &[u8]) -> bool {
    offset == TOKEN_ACCOUNT_OWNER_OFFSET && !bytes.is_empty() && bytes.len() < 32
}

fn has_owner_prefix(filters: &[RpcFilterType]) -> bool {
    filters.iter().any(|filter| {
        matches!(filter, RpcFilterType::Memcmp(memcmp)
            if memcmp.bytes().is_some_and(|b| is_owner_prefix(memcmp.offset(), &b)))
    })
}

/// The lowest byte string above every string that starts with `prefix`: the prefix without its
/// trailing 0xff bytes, plus one. `None` for an all-0xff prefix, which has no such string.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|byte| *byte != 0xff)?;
    let mut upper = prefix[..=last].to_vec();
    upper[last] += 1;
    Some(upper)
}

/// One filter as a SQL predicate on `table`, and the values it binds from `$param` on, if any.
/// Values that select an index (dataSize, memcmp offset and length, the delegate tag) stay literal
/// SQL. For a token program, owner, mint and delegate-tag memcmps become the exact expressions
/// the token indexes are built on, and an owner prefix becomes a range on `token_owner`.
fn filter_sql(
    filter: &RpcFilterType,
    table: &str,
    token_program: bool,
    param: usize,
) -> Option<(String, Vec<Vec<u8>>)> {
    match filter {
        RpcFilterType::DataSize(size) => {
            Some((format!("length({table}.data) = {size}"), Vec::new()))
        }
        RpcFilterType::Memcmp(memcmp) => {
            let bytes = memcmp.bytes()?.to_vec();
            let offset = memcmp.offset();

            if token_program {
                if offset == TOKEN_ACCOUNT_OWNER_OFFSET && bytes.len() == 32 {
                    return Some((format!("{table}.token_owner = ${param}"), vec![bytes]));
                } else if is_owner_prefix(offset, &bytes) {
                    // bytea sorts a prefix before its extensions, so the prefix is the lower bound.
                    let lower = format!("{table}.token_owner >= ${param}");
                    return Some(match prefix_upper_bound(&bytes) {
                        Some(upper) => (
                            format!("{lower} AND {table}.token_owner < ${}", param + 1),
                            vec![bytes, upper],
                        ),
                        None => (lower, vec![bytes]),
                    });
                } else if offset == TOKEN_ACCOUNT_MINT_OFFSET && bytes.len() == 32 {
                    return Some((format!("{table}.token_mint = ${param}"), vec![bytes]));
                } else if offset == TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET && bytes == [1, 0, 0, 0] {
                    // The first byte alone is the predicate of the token delegate partial index.
                    return Some((
                        format!(
                            "SUBSTRING({table}.data FROM 73 FOR 1) = '\\x01'::bytea AND SUBSTRING({table}.data FROM 74 FOR 3) = '\\x000000'::bytea"
                        ),
                        Vec::new(),
                    ));
                }
            }

            Some((
                format!(
                    "SUBSTRING({table}.data FROM {} FOR {}) = ${param}",
                    offset + 1,
                    bytes.len()
                ),
                vec![bytes],
            ))
        }
        // Agave's `spl_token_2022_interface::state::Account::valid_account_data`: byte 108 is the
        // account state (0 = uninitialized), a 355-byte account is a multisig.
        RpcFilterType::TokenAccountState => Some((
            format!(
                "(SUBSTRING({table}.data FROM 109 FOR 1) <> '\\x00'::bytea AND (length({table}.data) = 165 OR (length({table}.data) > 165 AND length({table}.data) <> 355 AND SUBSTRING({table}.data FROM 166 FOR 1) = '\\x02'::bytea)))"
            ),
            Vec::new(),
        )),
        RpcFilterType::ValueCmp(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudbreak_core::utils::token::LEGACY_TOKEN_PROGRAM_ID;

    fn sql(filter: RpcFilterType, token_program: bool) -> String {
        filter_sql(&filter, "accounts", token_program, 1).unwrap().0
    }

    fn bound(filter: RpcFilterType, token_program: bool) -> Vec<Vec<u8>> {
        filter_sql(&filter, "accounts", token_program, 1).unwrap().1
    }

    fn memcmp(offset: usize, bytes: &[u8]) -> RpcFilterType {
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(offset, bytes.to_vec()))
    }

    #[test]
    fn token_owner_and_mint_memcmps_use_the_indexed_columns() {
        let key = [7u8; 32];
        assert_eq!(sql(memcmp(32, &key), true), "accounts.token_owner = $1");
        assert_eq!(sql(memcmp(0, &key), true), "accounts.token_mint = $1");
        assert_eq!(bound(memcmp(32, &key), true), vec![key.to_vec()]);
        assert_eq!(bound(memcmp(0, &key), true), vec![key.to_vec()]);
    }

    #[test]
    fn a_token_owner_prefix_is_a_range_on_the_indexed_column() {
        let range = "accounts.token_owner >= $1 AND accounts.token_owner < $2";
        // One byte.
        assert_eq!(sql(memcmp(32, &[0x00]), true), range);
        assert_eq!(
            bound(memcmp(32, &[0x00]), true),
            vec![vec![0x00], vec![0x01]]
        );
        // Several bytes: only the last one moves.
        assert_eq!(sql(memcmp(32, &[0x06, 0x01]), true), range);
        assert_eq!(
            bound(memcmp(32, &[0x06, 0x01]), true),
            vec![vec![0x06, 0x01], vec![0x06, 0x02]]
        );
        assert_eq!(sql(memcmp(32, &[7; 31]), true), range);
        let mut upper = [7u8; 31];
        upper[30] = 8;
        assert_eq!(
            bound(memcmp(32, &[7; 31]), true),
            vec![vec![7; 31], upper.to_vec()]
        );
        // Carry: the trailing 0xff bytes drop and the byte before them moves.
        assert_eq!(sql(memcmp(32, &[0x01, 0xff]), true), range);
        assert_eq!(
            bound(memcmp(32, &[0x01, 0xff]), true),
            vec![vec![0x01, 0xff], vec![0x02]]
        );
        assert_eq!(
            bound(memcmp(32, &[0xfe, 0xff, 0xff]), true),
            vec![vec![0xfe, 0xff, 0xff], vec![0xff]]
        );
    }

    #[test]
    fn an_all_ff_token_owner_prefix_has_no_upper_bound() {
        for prefix in [vec![0xff], vec![0xff; 3], vec![0xff; 31]] {
            assert_eq!(sql(memcmp(32, &prefix), true), "accounts.token_owner >= $1");
            assert_eq!(bound(memcmp(32, &prefix), true), vec![prefix]);
        }
    }

    #[test]
    fn only_a_token_owner_prefix_becomes_a_range() {
        // Other programs, other offsets and longer values stay plain byte comparisons.
        assert_eq!(
            sql(memcmp(32, &[1, 2]), false),
            "SUBSTRING(accounts.data FROM 33 FOR 2) = $1"
        );
        assert_eq!(
            sql(memcmp(33, &[1, 2]), true),
            "SUBSTRING(accounts.data FROM 34 FOR 2) = $1"
        );
        assert_eq!(
            sql(memcmp(32, &[7; 40]), true),
            "SUBSTRING(accounts.data FROM 33 FOR 40) = $1"
        );
        assert_eq!(bound(memcmp(32, &[1, 2]), false), vec![vec![1, 2]]);
    }

    #[test]
    fn a_range_takes_two_parameter_numbers_in_both_tables() {
        let mint = [7u8; 32];
        let filters = [
            memcmp(0, &mint),
            memcmp(32, &[0x01, 0xff]),
            memcmp(64, &[9]),
        ];
        let sql = SqlFilters::build(&LEGACY_TOKEN_PROGRAM_ID, &filters, "gpa").unwrap();
        assert_eq!(
            sql.accounts,
            "AND accounts.token_mint = $1\n                    \
             AND accounts.token_owner >= $2 AND accounts.token_owner < $3\n                    \
             AND SUBSTRING(accounts.data FROM 65 FOR 1) = $4"
        );
        assert_eq!(
            sql.snapshot,
            sql.accounts.replace("accounts.", "snapshot_accounts.")
        );
        assert_eq!(
            sql.params,
            vec![mint.to_vec(), vec![0x01, 0xff], vec![0x02], vec![9]]
        );
        assert_eq!(sql.label, "gpa_mint");

        // Without an upper bound the next filter takes the next number.
        let filters = [memcmp(0, &mint), memcmp(32, &[0xff]), memcmp(64, &[9])];
        let sql = SqlFilters::build(&LEGACY_TOKEN_PROGRAM_ID, &filters, "gpa").unwrap();
        assert_eq!(
            sql.accounts,
            "AND accounts.token_mint = $1\n                    \
             AND accounts.token_owner >= $2\n                    \
             AND SUBSTRING(accounts.data FROM 65 FOR 1) = $3"
        );
        assert_eq!(sql.params, vec![mint.to_vec(), vec![0xff], vec![9]]);
    }

    #[test]
    fn only_a_mint_query_with_an_owner_prefix_asks_for_a_custom_plan() {
        let key = [7u8; 32];
        let custom_plan = |program: &Pubkey, filters: &[RpcFilterType]| {
            SqlFilters::build(program, filters, "gpa")
                .unwrap()
                .custom_plan
        };
        let token = &LEGACY_TOKEN_PROGRAM_ID;
        assert!(custom_plan(token, &[memcmp(0, &key), memcmp(32, &[1])]));
        assert!(!custom_plan(token, &[memcmp(0, &key)]));
        assert!(!custom_plan(token, &[memcmp(0, &key), memcmp(32, &key)]));
        assert!(!custom_plan(token, &[memcmp(76, &key), memcmp(32, &[1])]));
        assert!(!custom_plan(
            &Pubkey::new_unique(),
            &[memcmp(0, &key), memcmp(32, &[1])]
        ));
    }

    #[test]
    fn delegate_tag_memcmp_matches_the_delegate_index_predicate() {
        assert_eq!(
            sql(memcmp(72, &[1, 0, 0, 0]), true),
            "SUBSTRING(accounts.data FROM 73 FOR 1) = '\\x01'::bytea AND SUBSTRING(accounts.data FROM 74 FOR 3) = '\\x000000'::bytea"
        );
        assert!(bound(memcmp(72, &[1, 0, 0, 0]), true).is_empty());
        // Any other shape at 72 stays a plain byte comparison.
        assert_eq!(
            sql(memcmp(72, &[1]), true),
            "SUBSTRING(accounts.data FROM 73 FOR 1) = $1"
        );
        assert_eq!(bound(memcmp(72, &[1]), true), vec![vec![1]]);
    }

    #[test]
    fn delegate_memcmp_uses_the_delegate_index_expression() {
        let key = [9u8; 32];
        assert_eq!(
            sql(memcmp(76, &key), true),
            "SUBSTRING(accounts.data FROM 77 FOR 32) = $1"
        );
        assert_eq!(bound(memcmp(76, &key), true), vec![key.to_vec()]);
    }

    #[test]
    fn non_token_programs_keep_plain_byte_comparisons() {
        let key = [7u8; 32];
        assert_eq!(
            sql(memcmp(32, &key), false),
            "SUBSTRING(accounts.data FROM 33 FOR 32) = $1"
        );
        assert_eq!(
            sql(memcmp(72, &[1, 0, 0, 0]), false),
            "SUBSTRING(accounts.data FROM 73 FOR 4) = $1"
        );
        assert_eq!(
            bound(memcmp(72, &[1, 0, 0, 0]), false),
            vec![vec![1, 0, 0, 0]]
        );
    }

    #[test]
    fn token_account_state_matches_agave_valid_account_data() {
        assert_eq!(
            sql(RpcFilterType::TokenAccountState, true),
            "(SUBSTRING(accounts.data FROM 109 FOR 1) <> '\\x00'::bytea AND (length(accounts.data) = 165 OR (length(accounts.data) > 165 AND length(accounts.data) <> 355 AND SUBSTRING(accounts.data FROM 166 FOR 1) = '\\x02'::bytea)))"
        );
    }

    fn label(filters: Vec<RpcFilterType>, label: &str) -> Result<String, RpcError> {
        SqlFilters::build(&LEGACY_TOKEN_PROGRAM_ID, &filters, label).map(|sql| sql.label)
    }

    #[test]
    fn a_gpa_label_gets_the_token_key_as_suffix() {
        let key = vec![7; 32];
        assert_eq!(
            label(vec![memcmp(0, &key), memcmp(32, &key)], "gpa").unwrap(),
            "gpa_mint"
        );
        assert_eq!(
            label(vec![memcmp(76, &key), memcmp(32, &key)], "gpa").unwrap(),
            "gpa_delegate"
        );
        assert_eq!(label(vec![memcmp(32, &key)], "gpa").unwrap(), "gpa_token");
        assert_eq!(label(vec![memcmp(0, &key)], "gtabm").unwrap(), "gtabm");
    }

    #[test]
    fn a_token_query_without_an_indexed_key_is_rejected() {
        for filters in [
            vec![],
            vec![memcmp(64, &[7; 32])],
            vec![memcmp(32, &[7; 8])],
            vec![RpcFilterType::TokenAccountState],
        ] {
            assert!(matches!(
                label(filters, "gpa"),
                Err(RpcError::InvalidParams)
            ));
        }
    }

    #[test]
    fn a_delegate_filter_without_tag_gets_the_tag_in_sql() {
        let delegate = memcmp(76, &[9; 32]);
        let tagged = SqlFilters::build(
            &LEGACY_TOKEN_PROGRAM_ID,
            std::slice::from_ref(&delegate),
            "gpa",
        )
        .unwrap();
        assert!(
            tagged
                .accounts
                .contains("SUBSTRING(accounts.data FROM 73 FOR 1) = '\\x01'::bytea")
        );
        assert!(
            tagged
                .snapshot
                .contains("SUBSTRING(snapshot_accounts.data FROM 73 FOR 1) = '\\x01'::bytea")
        );

        let own_tag = SqlFilters::build(
            &LEGACY_TOKEN_PROGRAM_ID,
            &[delegate, memcmp(72, &[1])],
            "gpa",
        )
        .unwrap();
        assert_eq!(own_tag.accounts.matches("FROM 73 FOR 1").count(), 1);
    }

    #[test]
    fn other_programs_keep_their_filters_and_label() {
        let sql = SqlFilters::build(&Pubkey::new_unique(), &[memcmp(32, &[7; 32])], "gpa").unwrap();
        assert_eq!(sql.label, "gpa");
        assert_eq!(
            sql.accounts,
            "AND SUBSTRING(accounts.data FROM 33 FOR 32) = $1"
        );
        assert_eq!(sql.params, vec![vec![7; 32]]);
        assert!(
            SqlFilters::build(&Pubkey::new_unique(), &[], "gpa")
                .unwrap()
                .accounts
                .is_empty()
        );
    }

    #[test]
    fn both_tables_share_the_parameter_numbers() {
        let filters = [
            RpcFilterType::DataSize(165),
            memcmp(8, &[1, 2, 3]),
            memcmp(40, &[9]),
        ];
        let sql = SqlFilters::build(&Pubkey::new_unique(), &filters, "gpa").unwrap();
        assert_eq!(
            sql.accounts,
            "AND length(accounts.data) = 165\n                    \
             AND SUBSTRING(accounts.data FROM 9 FOR 3) = $1\n                    \
             AND SUBSTRING(accounts.data FROM 41 FOR 1) = $2"
        );
        assert_eq!(
            sql.snapshot,
            sql.accounts.replace("accounts.", "snapshot_accounts.")
        );
        assert_eq!(sql.params, vec![vec![1, 2, 3], vec![9]]);
    }
}
