-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Newest version per pubkey on narrow rows. Only `accounts` holds several versions per pubkey;
-- `snapshot_accounts` holds one, and a newer version of it is always in `accounts`.
WITH
accounts_latest AS MATERIALIZED (
    SELECT DISTINCT ON (accounts.pubkey)
        accounts.pubkey,
        accounts.slot,
        accounts.lamports,
        accounts.ctid AS row_id
    FROM accounts
    WHERE
        accounts.owner = $1
        AND accounts.slot <= $2
    -- {accounts_filters}
    ORDER BY accounts.pubkey ASC, accounts.slot DESC
)

SELECT
    latest.pubkey,
    fresh.owner,
    fresh.lamports,
    latest.slot,
    fresh.executable,
    fresh.rent_epoch,
    fresh.data,
    fresh.token_mint
FROM (
    SELECT DISTINCT ON (winners.pubkey)
        winners.pubkey,
        winners.slot,
        winners.lamports,
        winners.row_id,
        winners.in_accounts
    FROM (
        SELECT
            accounts_latest.pubkey,
            accounts_latest.slot,
            accounts_latest.lamports,
            accounts_latest.row_id,
            TRUE AS in_accounts
        FROM accounts_latest
        UNION ALL
        SELECT
            snapshot_accounts.pubkey,
            snapshot_accounts.slot,
            snapshot_accounts.lamports,
            snapshot_accounts.ctid AS row_id,
            FALSE AS in_accounts
        FROM snapshot_accounts
        WHERE
            snapshot_accounts.owner = $1
            AND snapshot_accounts.slot <= $2
    -- {snapshot_filters}
    ) AS winners
    -- A pubkey in both tables keeps its `accounts` version, which is always the newer one.
    ORDER BY winners.pubkey ASC, winners.in_accounts DESC
) AS latest
-- The full row, read from the winner's own table, only for winners newer than the cache.
LEFT JOIN LATERAL (
    SELECT
        accounts.owner,
        accounts.lamports,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data,
        accounts.token_mint
    FROM accounts
    WHERE
        latest.in_accounts
        AND latest.slot > $3
        AND accounts.owner = $1
        AND accounts.ctid = latest.row_id
    UNION ALL
    SELECT
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.executable,
        snapshot_accounts.rent_epoch,
        snapshot_accounts.data,
        snapshot_accounts.token_mint
    FROM snapshot_accounts
    WHERE
        NOT latest.in_accounts
        AND latest.slot > $3
        AND snapshot_accounts.owner = $1
        AND snapshot_accounts.ctid = latest.row_id
) AS fresh ON TRUE
WHERE latest.lamports > 0
ORDER BY latest.pubkey ASC;
