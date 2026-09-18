-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Owner-routed cleanup. Each key carries its own exclusive cutoff, so one form expresses both
-- an open key (cutoff = the finalized slot it was written at) and a closed or owner-moved key
-- (cutoff = one past the slot of its mask). The owner lets Postgres prune the DELETE to a single
-- hash partition per key, and `slot < k.cutoff` sits inside the index condition.
DELETE FROM accounts_table_name a  -- placeholder to be replaced with the actual table name
USING unnest($1::bytea[], $2::bytea[], $3::bigint[]) AS k(pubkey, owner, cutoff)
WHERE a.owner = k.owner AND a.pubkey = k.pubkey AND a.slot < k.cutoff
