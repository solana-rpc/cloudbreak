-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

WITH all_versions AS (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.data,
        accounts.token_mint
    FROM accounts
    WHERE
        accounts.token_mint = $1
        AND accounts.slot <= $2
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot,
        snapshot_accounts.data,
        snapshot_accounts.token_mint
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.token_mint = $1
        AND snapshot_accounts.slot <= $2
),

latest_per_pubkey AS (
    SELECT DISTINCT ON (pubkey)
        pubkey,
        owner,
        lamports,
        data
    FROM all_versions
    ORDER BY pubkey, slot DESC
)

SELECT
    pubkey,
    SUBSTRING(data FROM 65 FOR 8) AS amount   -- raw LE bytes; handler does from_le_bytes
FROM latest_per_pubkey
WHERE lamports > 0
  -- amount lives at bytes 64..71; require enough bytes so the get_byte() sort below
  -- can never read past the end (real token accounts are >= 82 bytes anyway).
  AND octet_length(data) >= 72
  AND (owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea      -- Tokenkeg  -- noqa: LT05
       OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea)  -- Token-2022  -- noqa: LT05
-- Sort by balance: a little-endian u64 at bytes 64..71 reconstructed as numeric.
-- Keep it numeric, not ::bigint (signed 64-bit, overflows above 2^63).
ORDER BY (
      get_byte(data,64)::numeric
    + get_byte(data,65)::numeric * 256
    + get_byte(data,66)::numeric * 65536
    + get_byte(data,67)::numeric * 16777216
    + get_byte(data,68)::numeric * 4294967296
    + get_byte(data,69)::numeric * 1099511627776
    + get_byte(data,70)::numeric * 281474976710656
    + get_byte(data,71)::numeric * 72057594037927936
) DESC, pubkey
LIMIT 20;
