// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

/// Filters the per-block listing of the finalizer debug endpoint by block origin.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockKindFilter {
    /// Both live (gRPC) and snapshot-repaired blocks.
    All,
    /// Only live blocks (those carrying chain data: a non-empty `blockhash`).
    Live,
    /// Only snapshot-repaired blocks (empty `blockhash`/`parent_blockhash`, `parent_slot == 0`).
    Repaired,
}

/// Verbosity for the finalizer / self-healing debug endpoints.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DebugDetail {
    Summary,
    Slots,
    Full,
}

/// Parsed query parameters shared by both module debug endpoints.
pub(crate) struct DebugParams {
    pub detail: DebugDetail,
    pub kind: BlockKindFilter,
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
    pub limit: Option<usize>,
    pub with_pubkeys: bool,
}

impl DebugParams {
    pub fn from_query(query: Option<&str>) -> Result<Self, String> {
        let mut detail = DebugDetail::Summary;
        let mut kind = BlockKindFilter::All;
        let mut min_slot: Option<u64> = None;
        let mut max_slot: Option<u64> = None;
        let mut limit: Option<usize> = None;
        let mut with_pubkeys = false;

        if let Some(q) = query {
            for pair in q.split('&').filter(|s| !s.is_empty()) {
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                match k {
                    "detail" => {
                        detail = match v {
                            "summary" => DebugDetail::Summary,
                            "slots" => DebugDetail::Slots,
                            "full" => DebugDetail::Full,
                            other => {
                                return Err(format!(
                                    "invalid `detail` value '{other}'; expected one of: summary, slots, full"
                                ));
                            }
                        };
                    }
                    "kind" => {
                        kind = match v {
                            "all" => BlockKindFilter::All,
                            "live" => BlockKindFilter::Live,
                            "repaired" => BlockKindFilter::Repaired,
                            other => {
                                return Err(format!(
                                    "invalid `kind` value '{other}'; expected one of: all, live, repaired"
                                ));
                            }
                        };
                    }
                    "min_slot" => {
                        min_slot = Some(
                            v.parse::<u64>()
                                .map_err(|e| format!("invalid `min_slot` value '{v}': {e}"))?,
                        );
                    }
                    "max_slot" => {
                        max_slot = Some(
                            v.parse::<u64>()
                                .map_err(|e| format!("invalid `max_slot` value '{v}': {e}"))?,
                        );
                    }
                    "limit" => {
                        limit = Some(
                            v.parse::<usize>()
                                .map_err(|e| format!("invalid `limit` value '{v}': {e}"))?,
                        );
                    }
                    "with_pubkeys" => {
                        with_pubkeys = match v {
                            "true" | "1" => true,
                            "false" | "0" | "" => false,
                            other => {
                                return Err(format!(
                                    "invalid `with_pubkeys` value '{other}'; expected true or false"
                                ));
                            }
                        };
                    }
                    _ => { /* Unknown params ignored, to be friendly with future expansion. */ }
                }
            }
        }

        if let (Some(min), Some(max)) = (min_slot, max_slot)
            && min > max
        {
            return Err(format!("`min_slot` ({min}) must be <= `max_slot` ({max})"));
        }

        if with_pubkeys
            && (detail != DebugDetail::Full
                || (limit.is_none() && !(min_slot.is_some() && max_slot.is_some())))
        {
            return Err(
                "`with_pubkeys=true` requires `detail=full` and a bounded selection (`limit`, or both `min_slot` and `max_slot`) to avoid dumping pubkeys for the entire map"
                    .to_string(),
            );
        }

        Ok(Self {
            detail,
            kind,
            min_slot,
            max_slot,
            limit,
            with_pubkeys,
        })
    }

    /// Whether `slot` falls within the optional `[min_slot, max_slot]` range (inclusive).
    pub fn in_range(&self, slot: u64) -> bool {
        self.min_slot.map(|m| slot >= m).unwrap_or(true)
            && self.max_slot.map(|m| slot <= m).unwrap_or(true)
    }
}
