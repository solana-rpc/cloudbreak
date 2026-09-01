SELECT record
FROM largest_accounts
WHERE mint = $1
  AND slot = (
      SELECT MAX(slot)
      FROM largest_accounts
      WHERE mint = $1
        AND slot <= $2
  );
