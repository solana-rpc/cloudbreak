-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- $1 = pubkey  (bytea literal)
-- $2 = slot bound: a slot literal, or a subquery on the slots table.
--      No row when the bound is NULL, so a missing slots entry answers no rows.
WITH latest_slot AS (
    SELECT slot
    FROM (SELECT $2::bigint AS slot) AS bound
    WHERE slot IS NOT NULL
),

all_versions AS (
    SELECT
        accounts.owner,
        accounts.lamports,
        accounts.slot
    FROM accounts, latest_slot
    WHERE
        accounts.pubkey = $1
        AND accounts.slot <= latest_slot.slot
    UNION ALL
    SELECT
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot
    FROM snapshot_accounts, latest_slot
    WHERE
        snapshot_accounts.pubkey = $1
        AND snapshot_accounts.slot <= latest_slot.slot
),

latest_account AS (
    SELECT
        owner,
        lamports
    FROM all_versions
    ORDER BY slot DESC
    LIMIT 1
)

SELECT
    latest_slot.slot AS context_slot,
    latest_account.owner,
    latest_account.lamports
FROM latest_slot
LEFT JOIN latest_account ON TRUE;
