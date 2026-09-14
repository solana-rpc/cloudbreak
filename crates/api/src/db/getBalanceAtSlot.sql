-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- getBalance on a processed view miss: newest owner and lamports at slot <= $2.
-- Closed accounts (lamports = 0) are filtered at the end so a recent close
-- shadows any earlier live version (matches getAccountInfo.sql).
-- getBalance.sql keeps closed rows, so a closed account with an excluded owner returns -32010 at confirmed.
-- Here it returns 0, which matches Agave.
--
-- $1 = requested pubkey (bytea literal)
-- $2 = slot bound, the anchor of the processed view (literal)

WITH all_versions AS (
    SELECT
        accounts.owner,
        accounts.lamports,
        accounts.slot
    FROM accounts
    WHERE
        accounts.pubkey = $1
        AND accounts.slot <= $2
    UNION ALL
    SELECT
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.pubkey = $1
        AND snapshot_accounts.slot <= $2
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
    owner,
    lamports
FROM latest_account
WHERE lamports > 0;
