-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- getMultipleAccounts (raw / non-jsonParsed variant).
--
-- $1 = ARRAY['\x...'::bytea, ...] - input pubkeys (order doesn't matter)
-- $2 = unused: `accounts` holds one row per account, so reads take no slot bound

WITH input AS (
    SELECT DISTINCT pubkey
    FROM unnest($1) AS t (pubkey)
),

latest_account AS (
    SELECT
        input.pubkey,
        latest_unified.owner,
        latest_unified.lamports,
        latest_unified.slot,
        latest_unified.executable,
        latest_unified.rent_epoch,
        latest_unified.data
    FROM input
    LEFT JOIN LATERAL ( -- noqa: ST05
        SELECT
            unified.owner,
            unified.lamports,
            unified.slot,
            unified.executable,
            unified.rent_epoch,
            unified.data
        FROM ( -- noqa: ST05
            (
                SELECT
                    accounts.owner,
                    accounts.lamports,
                    accounts.slot,
                    accounts.executable,
                    accounts.rent_epoch,
                    accounts.data
                FROM accounts
                WHERE
                    accounts.pubkey = input.pubkey
                ORDER BY accounts.slot DESC
                LIMIT 1
            )
            UNION ALL
            (
                SELECT
                    snapshot_accounts.owner,
                    snapshot_accounts.lamports,
                    snapshot_accounts.slot,
                    snapshot_accounts.executable,
                    snapshot_accounts.rent_epoch,
                    snapshot_accounts.data
                FROM snapshot_accounts
                WHERE
                    snapshot_accounts.pubkey = input.pubkey
                ORDER BY snapshot_accounts.slot DESC
                LIMIT 1
            )
        ) AS unified
        ORDER BY unified.slot DESC
        LIMIT 1
    ) AS latest_unified ON TRUE
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
