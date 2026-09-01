# largest_accounts module

Self-contained feature module for getLargestAccounts (GLA) and getTokenLargestAccounts (GTLA). The indexer maintains in-memory top-N holder sets and persists a packed 20-row record per (mint, slot) into the largest_accounts table. The API serves the newest record at or below the request's commitment slot.

## Layout

| File | Contents |
|---|---|
| mod.rs | Public surface: re-exports, the sentinel mint constants, PERSISTED_TOP_N, token program ids, MintRecord and the packed bytea codec (encode_record / decode_record). |
| tracker.rs | In-memory state: per-mint tops with the eviction reservoir and soundness bookkeeping (dropped_floor / stale), the snapshot seed path, apply_block, the class-sentinel reseed, and the unit tests. |
| persist.rs | Write path: record upserts, stale/cleared-mint deletes, from_config construction, and outcome persistence. |
| read.rs | Read path shared with the API: fetch_record returns the newest decoded record for a mint at or below a slot. |
| prune.rs | Finalize-time pruner that deletes redundant record generations, off the finalization critical path. |

## Runtime model

Both features ship in every binary and are gated at runtime by config. Two independent sections enable two independent domains in one tracker. Each is on only when its section is present with enabled = true:

- [largest-accounts] (GLA) enables the three sentinel tops: native-SOL, circulating, non-circulating. It requires an empty [programs] filter, the [snapshot] section, and the accounts owner map, because the circulating filter classifies accounts by owner.
- [token-largest-accounts] (GTLA) enables one top per mint in tracked-mints. It requires the token programs (Tokenkeg / Token-2022) indexed, which means an unfiltered [programs] or an include-list containing both, because the mint-metadata lookup and the reservoir read token accounts. It also requires the [snapshot] section. It does not need the owner map, the non-circulating tracker, or the GLA sentinels.

The tracker is seeded from the startup snapshot pass and goes live when snapshot processing finishes. When neither section is enabled the tracker handle is a no-op (LargestAccountsTracker::default()), so the indexer hooks (build_block_pending / commit_block in save_block, the pruner on finalize) cost nothing.

The API enables each method from the same two sections in its own config (present with enabled = true) and routes GTLA purely on record presence. A persisted record means the mint is tracked. No record means a fast "mint not tracked" error, including during bootstrap before a tracked mint's first record persists. There is no fallback table scan and no shared enablement state in the database.
