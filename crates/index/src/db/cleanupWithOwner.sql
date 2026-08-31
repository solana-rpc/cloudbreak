-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

DELETE FROM accounts_table_name AS a
USING unnest($2::bytea[], $3::bytea[]) AS k(pubkey, owner)
WHERE a.owner = k.owner
  AND a.pubkey = k.pubkey
  AND a.slot < $1;
