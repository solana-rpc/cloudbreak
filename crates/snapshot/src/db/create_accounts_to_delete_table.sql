-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Reads from the temp_snapshot_account_versions table and creates a table 
-- with the accounts to delete (which are the accounts that have multiple 
-- versions in the temp_snapshot_account_versions table, and this table will
-- contain all the older versions of the account)
DROP TABLE IF EXISTS accounts_to_delete;

CREATE UNLOGGED TABLE accounts_to_delete AS
SELECT
    owner,
    pubkey,
    slot
FROM temp_snapshot_account_versions AS t1
WHERE EXISTS (
    SELECT 1 FROM temp_snapshot_account_versions AS t2
    WHERE
        t2.pubkey = t1.pubkey
        AND t2.slot > t1.slot
);
