-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

WITH
program_accounts AS (
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
    -- We could also directly query the `slots` table here in the subquery
    -- to get the slot for the commitment rather than using the 
    -- `slot_for_commitment` CTE(TODO!: test performance difference)
    WHERE
        accounts.owner = $1
        AND accounts.slot <= $2
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
        snapshot_accounts.owner = $1
        AND snapshot_accounts.slot <= $2
-- {snapshot_filters}
),

max_slot AS (
    SELECT
        program_accounts.pubkey,
        MAX(program_accounts.slot) AS slot
    FROM program_accounts
    GROUP BY program_accounts.pubkey
),

-- this is only handling the deduplication for same (pubkey, slot)
-- pairs, the rest is handled by the inner join
deduplicated_program_accounts AS (
    SELECT DISTINCT ON (program_accounts.pubkey)
        program_accounts.pubkey,
        program_accounts.owner,
        program_accounts.lamports,
        program_accounts.slot,
        program_accounts.executable,
        program_accounts.rent_epoch,
        program_accounts.data,
        program_accounts.token_mint,
        program_accounts.token_owner
    FROM program_accounts
    INNER JOIN max_slot
        ON
            program_accounts.slot = max_slot.slot
            AND program_accounts.pubkey = max_slot.pubkey
    WHERE program_accounts.lamports > 0
),

-- Get unique mints we need to look up
needed_mints AS (
    SELECT DISTINCT token_mint
    FROM deduplicated_program_accounts
    WHERE token_mint IS NOT NULL
),

-- Gather all mint account versions from both tables
all_mint_versions AS NOT MATERIALIZED (
    SELECT
        accounts.pubkey,
        accounts.data,
        accounts.slot
    FROM accounts
    INNER JOIN needed_mints ON accounts.pubkey = needed_mints.token_mint
    WHERE
        accounts.owner = $1
        AND accounts.slot <= $2
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.data,
        snapshot_accounts.slot
    FROM snapshot_accounts
    INNER JOIN
        needed_mints
        ON snapshot_accounts.pubkey = needed_mints.token_mint
    WHERE
        snapshot_accounts.owner = $1
        AND snapshot_accounts.slot <= $2
),

-- Get latest slot per mint
mint_max_slot AS NOT MATERIALIZED (
    SELECT
        pubkey,
        MAX(slot) AS slot
    FROM all_mint_versions
    GROUP BY pubkey
),

-- Deduplicated mints
mints AS NOT MATERIALIZED (
    SELECT DISTINCT ON (all_mint_versions.pubkey)
        all_mint_versions.pubkey,
        all_mint_versions.data AS mint_data
    FROM all_mint_versions
    INNER JOIN mint_max_slot
        ON
            all_mint_versions.pubkey = mint_max_slot.pubkey
            AND all_mint_versions.slot = mint_max_slot.slot
)

SELECT
    deduplicated_program_accounts.pubkey,
    deduplicated_program_accounts.owner,
    deduplicated_program_accounts.lamports,
    deduplicated_program_accounts.slot,
    deduplicated_program_accounts.executable,
    deduplicated_program_accounts.rent_epoch,
    deduplicated_program_accounts.data,
    deduplicated_program_accounts.token_mint,
    mints.mint_data
FROM deduplicated_program_accounts
LEFT JOIN mints ON deduplicated_program_accounts.token_mint = mints.pubkey
ORDER BY deduplicated_program_accounts.pubkey ASC;
