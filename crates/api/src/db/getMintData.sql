-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

WITH
all_mints AS (
    -- TokenKeg partition (accounts)
    SELECT
        data,
        slot,
        owner,
        lamports
    FROM accounts
    WHERE
        owner
        = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea -- noqa: LT05
        AND pubkey = $1
        AND slot <= $2
    UNION ALL
    -- TokenKeg partition (snapshot_accounts)
    SELECT
        data,
        slot,
        owner,
        lamports
    FROM snapshot_accounts
    WHERE
        owner
        = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea -- noqa: LT05
        AND pubkey = $1
        AND slot <= $2
    UNION ALL
    -- Token-2022 partition (accounts)
    SELECT
        data,
        slot,
        owner,
        lamports
    FROM accounts
    WHERE
        owner
        = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea -- noqa: LT05
        AND pubkey = $1
        AND slot <= $2
    UNION ALL
    -- Token-2022 partition (snapshot_accounts)
    SELECT
        data,
        slot,
        owner,
        lamports
    FROM snapshot_accounts
    WHERE
        owner
        = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea -- noqa: LT05
        AND pubkey = $1
        AND slot <= $2
)

SELECT
    data,
    owner,
    lamports
FROM all_mints
ORDER BY slot DESC LIMIT 1;
