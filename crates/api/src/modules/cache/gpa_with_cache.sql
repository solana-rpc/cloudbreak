-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

WITH
program_accounts AS MATERIALIZED (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data,
        accounts.token_mint,
        accounts.token_owner
    FROM accounts
    WHERE
        accounts.owner = {program}
        AND accounts.slot <= {latest_slot}
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
        snapshot_accounts.token_mint,
        snapshot_accounts.token_owner
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.owner = {program}
        AND snapshot_accounts.slot <= {latest_slot}
-- {snapshot_filters}
)

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
    SELECT DISTINCT ON (program_accounts.pubkey)
        program_accounts.pubkey,
        program_accounts.owner,
        program_accounts.lamports,
        program_accounts.slot,
        program_accounts.executable,
        program_accounts.rent_epoch,
        program_accounts.data,
        program_accounts.token_mint
    FROM program_accounts
    ORDER BY program_accounts.pubkey ASC, program_accounts.slot DESC
) AS latest
WHERE
    lamports > 0
    -- A newer row of the account under the program is its state at the read slot. The filters
    -- left it out (a close row, a version they do not select), so the account is not returned.
    AND NOT EXISTS (
        SELECT 1
        FROM accounts
        WHERE
            accounts.owner = {program}
            AND accounts.pubkey = latest.pubkey
            AND accounts.slot > latest.slot
            AND accounts.slot <= {latest_slot}
    )
-- DISTINCT ON already sorts by pubkey; this makes the output order explicit.
ORDER BY latest.pubkey ASC;
