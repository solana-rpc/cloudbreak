-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Owner-routed variant of closedAccountscleanup.sql. Inclusive slot bound so the
-- closed-account mask row inserted at the finalized slot is deleted too.
DELETE FROM accounts AS a
USING unnest($2::bytea[], $3::bytea[]) AS k(pubkey, owner)
WHERE a.owner = k.owner
  AND a.pubkey = k.pubkey
  AND a.slot <= $1;  -- $1 is finalized_slot
