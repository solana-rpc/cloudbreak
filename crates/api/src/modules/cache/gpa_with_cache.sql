-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- `accounts` holds one row per account and shares no pubkey with `snapshot_accounts`, so the
-- filtered rows of both tables are the answer. {latest_slot} is bound but not referenced.
SELECT
    pubkey,
    CASE WHEN slot <= {cache_slot} THEN NULL ELSE owner END, -- noqa: AL03
    CASE WHEN slot <= {cache_slot} THEN NULL ELSE lamports END AS lamports,
    CASE WHEN slot <= {cache_slot} THEN slot ELSE slot END AS slot,
    CASE WHEN slot <= {cache_slot} THEN NULL ELSE executable END AS executable,
    CASE WHEN slot <= {cache_slot} THEN NULL ELSE rent_epoch END AS rent_epoch,
    CASE WHEN slot <= {cache_slot} THEN NULL ELSE data END, -- noqa: AL03
    CASE WHEN slot <= {cache_slot} THEN NULL ELSE token_mint END AS token_mint
FROM (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data,
        accounts.token_mint
    FROM accounts
    WHERE
        accounts.owner = {program}
    -- {accounts_filters}
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot,
        snapshot_accounts.executable,
        snapshot_accounts.rent_epoch,
        snapshot_accounts.data,
        snapshot_accounts.token_mint
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.owner = {program}
-- {snapshot_filters}
) AS latest
ORDER BY latest.pubkey ASC;
