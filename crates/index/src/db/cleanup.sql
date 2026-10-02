-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Unrouted cleanup, for nodes with the owner map off and for keys the map did not know.
-- Same per-key exclusive cutoff as cleanupWithOwner.sql without the owner terms. Every read
-- path dedupes on pubkey alone, so "newest version of this pubkey regardless of owner" is the
-- visibility rule this form matches. On a partitioned table it fans out to every partition.
DELETE FROM accounts_table_name a  -- placeholder to be replaced with the actual table name
USING unnest($1::bytea[], $2::bigint[]) AS k(pubkey, cutoff)
WHERE a.pubkey = k.pubkey AND a.slot < k.cutoff
