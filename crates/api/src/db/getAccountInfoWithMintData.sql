-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- getAccountInfo variant for `encoding = "jsonParsed"`. Identical to
-- getAccountInfo.sql for the requested account, plus a LEFT JOIN that
-- fetches the latest mint data when the account is owned by SPL Token /
-- Token-2022. The mint data is needed by token::parse_additional_mint_data
-- to populate UiTokenAmount.uiAmount on jsonParsed.
--
-- For non-token accounts the `needed_mint` CTE is empty and the LEFT JOIN
-- yields mint_data = NULL.
--
-- Closed accounts (lamports = 0) are filtered AT THE END (matches
-- getProgramAccounts.sql) so a recent close shadows any earlier live
-- version. We also skip the mint lookup entirely when the latest
-- requested account is closed, because we'd be throwing the result away.
--
-- $1 = requested pubkey (bytea literal)
-- $2 = unused: `accounts` holds one row per account, so reads take no slot bound

WITH all_versions AS (
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
        accounts.pubkey = $1
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
        data,
        token_mint
    FROM all_versions
    ORDER BY slot DESC
    LIMIT 1
),

-- We only JOIN the mint data if the account is owned by a token program.
needed_mint AS (
    SELECT token_mint AS mint_pubkey
    FROM latest_account
    WHERE
        lamports > 0
        AND token_mint IS NOT NULL
        AND (
            owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea -- TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA -- noqa: LT05
            OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea -- TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb -- noqa: LT05
        )
),

all_mint_versions AS NOT MATERIALIZED (
    SELECT
        accounts.pubkey,
        accounts.data,
        accounts.slot,
        accounts.lamports
    FROM accounts
    INNER JOIN needed_mint ON accounts.pubkey = needed_mint.mint_pubkey
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.data,
        snapshot_accounts.slot,
        snapshot_accounts.lamports
    FROM snapshot_accounts
    INNER JOIN needed_mint ON snapshot_accounts.pubkey = needed_mint.mint_pubkey
),

mint AS (
    -- Pick the latest version per mint pubkey first, then drop it if that
    -- latest version is closed (lamports = 0). Mirrors the
    -- filter-at-the-end pattern used for the requested account above and
    -- in getProgramAccounts.sql.
    SELECT
        pubkey,
        mint_data
    FROM (
        SELECT DISTINCT ON (pubkey)
            pubkey,
            data AS mint_data,
            lamports
        FROM all_mint_versions
        ORDER BY pubkey ASC, slot DESC
    ) AS latest_per_pubkey
    WHERE lamports > 0
)

SELECT
    latest_account.pubkey,
    latest_account.owner,
    latest_account.lamports,
    latest_account.slot,
    latest_account.executable,
    latest_account.rent_epoch,
    latest_account.data,
    mint.mint_data
FROM latest_account
LEFT JOIN mint ON latest_account.token_mint = mint.pubkey
WHERE latest_account.lamports > 0;
