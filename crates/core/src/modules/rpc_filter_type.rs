// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! In-repo (vendored) `getProgramAccounts` filter types.
//!
//! These mirror `solana_rpc_client_api::filter` / `::config` on the wire, but
//! are owned here so we can support the custom [`RpcFilterType::ValueCmp`]
//! variant, which is not present in the published `solana-rpc-client-api`.

use serde::{Deserialize, Serialize};
use solana_rpc_client_api::config::RpcAccountInfoConfig;
pub use solana_rpc_client_api::filter::{Memcmp, MemcmpEncodedBytes};
use thiserror::Error;

/// Vendored equivalent of `solana_rpc_client_api::config::RpcProgramAccountsConfig`,
/// the only difference being that `filters` uses our local [`RpcFilterType`]
/// (which adds [`RpcFilterType::ValueCmp`]). The serde representation is
/// identical so it stays wire-compatible with standard clients.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcProgramAccountsConfig {
    pub filters: Option<Vec<RpcFilterType>>,
    #[serde(flatten)]
    pub account_config: RpcAccountInfoConfig,
    pub with_context: Option<bool>,
    pub sort_results: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcFilterType {
    DataSize(u64),
    Memcmp(Memcmp),
    TokenAccountState,
    ValueCmp(ValueCmp),
}

impl RpcFilterType {
    pub fn verify(&self) -> Result<(), RpcFilterError> {
        match self {
            RpcFilterType::DataSize(_) => Ok(()),
            RpcFilterType::Memcmp(memcmp) => Ok(
                solana_rpc_client_api::filter::RpcFilterType::Memcmp(memcmp.clone()).verify()?,
            ),
            RpcFilterType::TokenAccountState => Ok(()),
            RpcFilterType::ValueCmp(cmp) => cmp.verify(),
        }
    }
}

/// The largest account data size, the system program's `MAX_PERMITTED_DATA_LENGTH`.
pub const MAX_ACCOUNT_DATA_LEN: usize = 10 * 1024 * 1024;

/// True when `memcmp` ends within the largest account data size, so an account can match it.
pub fn memcmp_fits_account(memcmp: &Memcmp) -> bool {
    memcmp.bytes().is_some_and(|bytes| {
        memcmp
            .offset()
            .checked_add(bytes.len())
            .is_some_and(|end| end <= MAX_ACCOUNT_DATA_LEN)
    })
}

/// Returns `true` if any of the filters is a [`RpcFilterType::ValueCmp`]. Used
/// to cheaply skip the post-filter pass when no ValueCmp filters are present.
pub fn has_value_cmp(filters: &[RpcFilterType]) -> bool {
    filters
        .iter()
        .any(|f| matches!(f, RpcFilterType::ValueCmp(_)))
}

/// Post-filter predicate: returns `true` if `data` satisfies *every*
/// [`RpcFilterType::ValueCmp`] filter in `filters`. Non-ValueCmp filters are
/// ignored here (they are pushed down to SQL), so this is safe to call with the
/// full filter slice. A ValueCmp that errors (e.g. offset out of bounds) is
/// treated as not matching.
pub fn account_matches_value_cmps(filters: &[RpcFilterType], data: &[u8]) -> bool {
    filters.iter().all(|f| match f {
        RpcFilterType::ValueCmp(cmp) => cmp.values_match(data).unwrap_or(false),
        _ => true,
    })
}

#[derive(Error, PartialEq, Eq, Debug)]
pub enum RpcFilterError {
    #[error(transparent)]
    Memcmp(#[from] solana_rpc_client_api::filter::RpcFilterError),
    #[error("invalid ValueCmp filter")]
    InvalidValueCmp,
}

impl RpcFilterError {
    /// Text for the `Invalid param: {}` message. Agave formats the upstream error
    /// with `{:?}`, so unwrap our transparent variant to match it byte for byte.
    pub fn invalid_param_text(&self) -> String {
        match self {
            RpcFilterError::Memcmp(err) => format!("{err:?}"),
            RpcFilterError::InvalidValueCmp => self.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValueCmp {
    pub left: Operand,
    comparator: Comparator,
    pub right: Operand,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Operand {
    Mem {
        offset: usize,
        value_type: ValueType,
    },
    Constant(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    U8,
    U16,
    U32,
    U64,
    U128,
}

impl ValueType {
    fn size(&self) -> usize {
        match self {
            ValueType::U8 => 1,
            ValueType::U16 => 2,
            ValueType::U32 => 4,
            ValueType::U64 => 8,
            ValueType::U128 => 16,
        }
    }
}

enum WrappedValueType {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
}

/// The `N` bytes of `data` at `offset`, or `InvalidValueCmp` when they are out of bounds.
fn read_bytes<const N: usize>(data: &[u8], offset: usize) -> Result<[u8; N], RpcFilterError> {
    offset
        .checked_add(N)
        .and_then(|end| data.get(offset..end))
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(RpcFilterError::InvalidValueCmp)
}

impl ValueCmp {
    pub fn new(left: Operand, comparator: Comparator, right: Operand) -> Self {
        Self {
            left,
            comparator,
            right,
        }
    }

    /// Rejects a memory operand that ends past the largest account data size.
    fn verify(&self) -> Result<(), RpcFilterError> {
        for operand in [&self.left, &self.right] {
            if let Operand::Mem { offset, value_type } = operand {
                offset
                    .checked_add(value_type.size())
                    .filter(|end| *end <= MAX_ACCOUNT_DATA_LEN)
                    .ok_or(RpcFilterError::InvalidValueCmp)?;
            }
        }
        Ok(())
    }

    fn parse_mem_into_value_type(
        o: &Operand,
        data: &[u8],
    ) -> Result<WrappedValueType, RpcFilterError> {
        match o {
            Operand::Mem { offset, value_type } => Ok(match value_type {
                ValueType::U8 => {
                    WrappedValueType::U8(u8::from_le_bytes(read_bytes(data, *offset)?))
                }
                ValueType::U16 => {
                    WrappedValueType::U16(u16::from_le_bytes(read_bytes(data, *offset)?))
                }
                ValueType::U32 => {
                    WrappedValueType::U32(u32::from_le_bytes(read_bytes(data, *offset)?))
                }
                ValueType::U64 => {
                    WrappedValueType::U64(u64::from_le_bytes(read_bytes(data, *offset)?))
                }
                ValueType::U128 => {
                    WrappedValueType::U128(u128::from_le_bytes(read_bytes(data, *offset)?))
                }
            }),
            _ => Err(RpcFilterError::InvalidValueCmp),
        }
    }

    pub fn values_match(&self, data: &[u8]) -> Result<bool, RpcFilterError> {
        match (&self.left, &self.right) {
            (left @ Operand::Mem { .. }, right @ Operand::Mem { .. }) => {
                let left = Self::parse_mem_into_value_type(left, data)?;
                let right = Self::parse_mem_into_value_type(right, data)?;

                match (left, right) {
                    (WrappedValueType::U8(left), WrappedValueType::U8(right)) => {
                        Ok(self.comparator.compare(left, right))
                    }
                    (WrappedValueType::U16(left), WrappedValueType::U16(right)) => {
                        Ok(self.comparator.compare(left, right))
                    }
                    (WrappedValueType::U32(left), WrappedValueType::U32(right)) => {
                        Ok(self.comparator.compare(left, right))
                    }
                    (WrappedValueType::U64(left), WrappedValueType::U64(right)) => {
                        Ok(self.comparator.compare(left, right))
                    }
                    (WrappedValueType::U128(left), WrappedValueType::U128(right)) => {
                        Ok(self.comparator.compare(left, right))
                    }
                    _ => Err(RpcFilterError::InvalidValueCmp),
                }
            }
            (left @ Operand::Mem { .. }, Operand::Constant(constant)) => {
                match Self::parse_mem_into_value_type(left, data)? {
                    WrappedValueType::U8(left) => {
                        let right = constant
                            .parse::<u8>()
                            .map_err(|_| RpcFilterError::InvalidValueCmp)?;
                        Ok(self.comparator.compare(left, right))
                    }
                    WrappedValueType::U16(left) => {
                        let right = constant
                            .parse::<u16>()
                            .map_err(|_| RpcFilterError::InvalidValueCmp)?;
                        Ok(self.comparator.compare(left, right))
                    }
                    WrappedValueType::U32(left) => {
                        let right = constant
                            .parse::<u32>()
                            .map_err(|_| RpcFilterError::InvalidValueCmp)?;
                        Ok(self.comparator.compare(left, right))
                    }
                    WrappedValueType::U64(left) => {
                        let right = constant
                            .parse::<u64>()
                            .map_err(|_| RpcFilterError::InvalidValueCmp)?;
                        Ok(self.comparator.compare(left, right))
                    }
                    WrappedValueType::U128(left) => {
                        let right = constant
                            .parse::<u128>()
                            .map_err(|_| RpcFilterError::InvalidValueCmp)?;
                        Ok(self.comparator.compare(left, right))
                    }
                }
            }
            _ => Err(RpcFilterError::InvalidValueCmp),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Comparator {
    Eq = 0,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl Comparator {
    pub fn compare<T: PartialOrd>(&self, left: T, right: T) -> bool {
        match self {
            Comparator::Eq => left == right,
            Comparator::Ne => left != right,
            Comparator::Gt => left > right,
            Comparator::Ge => left >= right,
            Comparator::Lt => left < right,
            Comparator::Le => left <= right,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Strings captured from Agave 4.2.2 (nyc205) and 4.3.0-rc.0 (fra286).
    #[test]
    fn test_invalid_param_text_matches_agave() {
        let oversized = RpcFilterType::Memcmp(Memcmp::new(
            0,
            MemcmpEncodedBytes::Base58("1".repeat(200)),
        ));
        let err = oversized.verify().unwrap_err();
        assert_eq!(err.invalid_param_text(), "DataTooLarge");

        let bad_base58 =
            RpcFilterType::Memcmp(Memcmp::new(0, MemcmpEncodedBytes::Base58("0OIl".into())));
        assert_eq!(
            bad_base58.verify().unwrap_err().invalid_param_text(),
            "Base58DecodeError(InvalidCharacter { character: '0', index: 0 })"
        );

        let bad_base64 =
            RpcFilterType::Memcmp(Memcmp::new(0, MemcmpEncodedBytes::Base64("!!!!".into())));
        assert_eq!(
            bad_base64.verify().unwrap_err().invalid_param_text(),
            "Base64DecodeError(InvalidByte(0, 33))"
        );

        // No Agave counterpart, so this one keeps its own Display text.
        assert_eq!(
            RpcFilterError::InvalidValueCmp.invalid_param_text(),
            "invalid ValueCmp filter"
        );
    }

    #[test]
    fn test_values_match_constant() {
        let data = vec![1, 2, 3, 4, 5];

        // data[1] == 2
        assert!(
            ValueCmp::new(
                Operand::Mem {
                    offset: 1,
                    value_type: ValueType::U8,
                },
                Comparator::Eq,
                Operand::Constant("2".to_string()),
            )
            .values_match(&data)
            .unwrap()
        );

        // data[1] < 3
        assert!(
            ValueCmp::new(
                Operand::Mem {
                    offset: 1,
                    value_type: ValueType::U8,
                },
                Comparator::Lt,
                Operand::Constant("3".to_string()),
            )
            .values_match(&data)
            .unwrap()
        );

        // little-endian u32 over [1,2,3,4] == 67305985
        assert!(
            ValueCmp::new(
                Operand::Mem {
                    offset: 0,
                    value_type: ValueType::U32,
                },
                Comparator::Eq,
                Operand::Constant("67305985".to_string()),
            )
            .values_match(&data)
            .unwrap()
        );
    }

    #[test]
    fn test_values_match_out_of_bounds_is_err() {
        let data = vec![1, 2];
        let cmp = ValueCmp::new(
            Operand::Mem {
                offset: 8,
                value_type: ValueType::U64,
            },
            Comparator::Eq,
            Operand::Constant("0".to_string()),
        );
        assert!(cmp.values_match(&data).is_err());
    }

    fn value_types() -> [ValueType; 5] {
        [
            ValueType::U8,
            ValueType::U16,
            ValueType::U32,
            ValueType::U64,
            ValueType::U128,
        ]
    }

    fn mem(offset: usize, value_type: ValueType) -> Operand {
        Operand::Mem { offset, value_type }
    }

    #[test]
    fn test_values_match_overflowing_offset_is_err() {
        let data = [7u8; 64];
        for value_type in value_types() {
            for offset in (0..16).map(|k| usize::MAX - k).chain([1 << 31, 1 << 63]) {
                let constant = ValueCmp::new(
                    mem(offset, value_type.clone()),
                    Comparator::Eq,
                    Operand::Constant("0".to_string()),
                );
                assert!(constant.values_match(&data).is_err());

                let both = ValueCmp::new(
                    mem(0, value_type.clone()),
                    Comparator::Eq,
                    mem(offset, value_type.clone()),
                );
                assert!(both.values_match(&data).is_err());

                let filters = [RpcFilterType::ValueCmp(constant)];
                assert!(!account_matches_value_cmps(&filters, &data));
            }
        }
    }

    #[test]
    fn test_values_match_last_value_in_data() {
        let data = [7u8; 64];
        for value_type in value_types() {
            let last = data.len() - value_type.size();
            let cmp = |offset| {
                ValueCmp::new(
                    mem(0, value_type.clone()),
                    Comparator::Eq,
                    mem(offset, value_type.clone()),
                )
            };
            assert!(cmp(last).values_match(&data).unwrap());
            assert!(cmp(last + 1).values_match(&data).is_err());
        }
    }

    #[test]
    fn test_verify_value_cmp_offset() {
        let constant = || Operand::Constant("0".to_string());
        for value_type in value_types() {
            let last = MAX_ACCOUNT_DATA_LEN - value_type.size();
            for (offset, valid) in [
                (0, true),
                (last, true),
                (last + 1, false),
                (usize::MAX, false),
            ] {
                let left = RpcFilterType::ValueCmp(ValueCmp::new(
                    mem(offset, value_type.clone()),
                    Comparator::Eq,
                    constant(),
                ));
                let right = RpcFilterType::ValueCmp(ValueCmp::new(
                    constant(),
                    Comparator::Eq,
                    mem(offset, value_type.clone()),
                ));
                for filter in [left, right] {
                    let expected = if valid {
                        Ok(())
                    } else {
                        Err(RpcFilterError::InvalidValueCmp)
                    };
                    assert_eq!(filter.verify(), expected, "{value_type:?} at {offset}");
                }
            }
        }
    }

    #[test]
    fn test_verify_rejects_max_u64_offset_from_json() {
        let json = r#"{
            "valueCmp": {
                "left": { "Mem": { "offset": 18446744073709551615, "value_type": "U16" } },
                "comparator": "Eq",
                "right": { "Constant": "0" }
            }
        }"#;
        let filter: RpcFilterType = serde_json::from_str(json).unwrap();
        assert_eq!(filter.verify(), Err(RpcFilterError::InvalidValueCmp));
    }

    #[test]
    fn test_account_matches_value_cmps_helper() {
        let data = vec![1, 2, 3, 4, 5];

        let matching = vec![RpcFilterType::ValueCmp(ValueCmp::new(
            Operand::Mem {
                offset: 1,
                value_type: ValueType::U8,
            },
            Comparator::Eq,
            Operand::Constant("2".to_string()),
        ))];
        assert!(has_value_cmp(&matching));
        assert!(account_matches_value_cmps(&matching, &data));

        let non_matching = vec![RpcFilterType::ValueCmp(ValueCmp::new(
            Operand::Mem {
                offset: 1,
                value_type: ValueType::U8,
            },
            Comparator::Eq,
            Operand::Constant("99".to_string()),
        ))];
        assert!(!account_matches_value_cmps(&non_matching, &data));

        // Out-of-bounds ValueCmp does not match (error treated as false).
        let oob = vec![RpcFilterType::ValueCmp(ValueCmp::new(
            Operand::Mem {
                offset: 100,
                value_type: ValueType::U8,
            },
            Comparator::Eq,
            Operand::Constant("1".to_string()),
        ))];
        assert!(!account_matches_value_cmps(&oob, &data));

        // No ValueCmp filters -> helper is a no-op pass.
        let only_datasize = vec![RpcFilterType::DataSize(5)];
        assert!(!has_value_cmp(&only_datasize));
        assert!(account_matches_value_cmps(&only_datasize, &data));
    }

    #[test]
    fn test_value_cmp_wire_compat_deserialize() {
        // Ensure the ValueCmp variant round-trips through the standard
        // getProgramAccounts filter JSON shape.
        let json = r#"{
            "valueCmp": {
                "left": { "Mem": { "offset": 0, "value_type": "U64" } },
                "comparator": "Gt",
                "right": { "Constant": "100" }
            }
        }"#;
        let filter: RpcFilterType = serde_json::from_str(json).unwrap();
        assert!(matches!(filter, RpcFilterType::ValueCmp(_)));
    }
}
