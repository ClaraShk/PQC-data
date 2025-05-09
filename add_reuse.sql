SELECT
  address_type,
  COUNT(DISTINCT address) AS address_count,
  SUM(usage_count) AS total_usages
FROM (
  SELECT
    address,
    output.type AS address_type,
    COUNT(1) AS usage_count
  FROM
    `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    UNNEST(tx.outputs) AS output,
    UNNEST(output.addresses) AS address
  WHERE
    1=1
    and tx.block_number > 823500  -- adjust as needed
  GROUP BY
    address, address_type
)
WHERE
  usage_count > 1 -- reused addresses only
GROUP BY
  address_type
ORDER BY
  address_count DESC
