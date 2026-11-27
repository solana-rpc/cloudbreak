-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- For the accounts a block wrote ($1 pubkeys, $2 owners, $3 slot), writes a close row under the
-- previous owner of each one that moved to another program, as the owner map does when it is on.
WITH
    written AS (
        SELECT
            written.pubkey,
            written.owner
        FROM UNNEST($1::bytea [], $2::bytea []) AS written (pubkey, owner)
    ),
    -- The accounts that were not live under the owner before this block: they are new, or come from
    -- another program. A row in `accounts` is newer than the account's snapshot row, so the newest
    -- one there decides, and the snapshot row only when there is none. `snapshot_accounts` holds no
    -- closed account once the snapshot has loaded, which is when this statement starts to run.
    not_under_owner AS (
        SELECT
            written.pubkey,
            written.owner
        FROM written
        WHERE NOT COALESCE(
            (
                SELECT accounts.lamports > 0
                FROM accounts
                WHERE
                    accounts.owner = written.owner
                    AND accounts.pubkey = written.pubkey
                    AND accounts.slot < $3
                ORDER BY accounts.slot DESC
                LIMIT 1
            ),
            EXISTS (
                SELECT 1
                FROM snapshot_accounts
                WHERE
                    snapshot_accounts.owner = written.owner
                    AND snapshot_accounts.pubkey = written.pubkey
                    AND snapshot_accounts.slot < $3
            )
        )
    ),
    -- Each such account's newest older row under any owner. An earlier move left two rows at its
    -- slot: the close row under the owner before it and the live row, which is the account's state.
    previous AS (
        SELECT
            not_under_owner.pubkey,
            not_under_owner.owner AS new_owner,
            newest.owner,
            newest.lamports
        FROM not_under_owner
        CROSS JOIN LATERAL (
            SELECT
                accounts.owner,
                accounts.lamports,
                accounts.slot
            FROM accounts
            WHERE
                accounts.pubkey = not_under_owner.pubkey
                AND accounts.slot < $3
            UNION ALL
            SELECT
                snapshot_accounts.owner,
                snapshot_accounts.lamports,
                snapshot_accounts.slot
            FROM snapshot_accounts
            WHERE
                snapshot_accounts.pubkey = not_under_owner.pubkey
                AND snapshot_accounts.slot < $3
            ORDER BY slot DESC, lamports DESC
            LIMIT 1
        ) AS newest
    )
INSERT INTO accounts (pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version)
SELECT
    previous.pubkey,
    previous.owner,
    0,
    $3,
    FALSE,
    0,
    '\x'::bytea,
    0
FROM previous
WHERE
    previous.lamports > 0
    AND previous.owner <> previous.new_owner;
