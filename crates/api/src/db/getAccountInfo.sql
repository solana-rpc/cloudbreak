-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- getAccountInfo: the requested pubkey.
-- Closed accounts (lamports = 0) are filtered AT THE END so that a recent
-- close shadows any earlier live version (matches getProgramAccounts.sql).
--
-- $1 = requested pubkey (bytea literal injected by the caller)
-- $2 = unused: `accounts` holds one row per account, so reads take no slot bound
--
-- No `WHERE owner = ...` predicate, so partition pruning does not apply;
-- the cost is dominated by the per-partition (pubkey, slot DESC) B-tree
-- (idx_accounts_pubkey_slot / idx_snapshot_accounts_pubkey_slot), which
-- is exactly the access pattern they were built for.

WITH all_versions AS (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data
    FROM accounts
    WHERE
        accounts.pubkey = $1
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot,
        snapshot_accounts.executable,
        snapshot_accounts.rent_epoch,
        snapshot_accounts.data
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.pubkey = $1
),

latest_account AS (
    SELECT
        pubkey,
        owner,
        lamports,
        slot,
        executable,
        rent_epoch,
        data
    FROM all_versions
    ORDER BY slot DESC
    LIMIT 1
)

SELECT
    pubkey,
    owner,
    lamports,
    slot,
    executable,
    rent_epoch,
    data
FROM latest_account
WHERE lamports > 0;
