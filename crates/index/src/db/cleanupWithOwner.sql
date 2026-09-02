-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Owner-routed variant of cleanup.sql: the owner in each (pubkey, owner) pair lets
-- Postgres prune the DELETE to a single hash partition instead of scanning all 64.
DELETE FROM accounts_table_name AS a  -- placeholder to be replaced with the actual table name
USING unnest($2::bytea[], $3::bytea[]) AS k(pubkey, owner)
WHERE a.owner = k.owner
  AND a.pubkey = k.pubkey
  AND a.slot < $1;  -- $1 is finalized_slot
