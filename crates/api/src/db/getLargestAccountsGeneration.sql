SELECT
    pubkey,
    amount::text AS amount
FROM largest_accounts
WHERE mint = $1
  AND slot = (
      SELECT MAX(slot)
      FROM largest_accounts
      WHERE mint = $1
        AND slot <= $2
  )
ORDER BY largest_accounts.amount DESC, pubkey DESC
LIMIT 20;
