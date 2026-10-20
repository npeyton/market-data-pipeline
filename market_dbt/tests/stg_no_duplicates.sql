-- Fails if any ticker appears more than once on the same day.
SELECT
  ticker,
  trade_date,
  COUNT(*) AS copies
FROM {{ ref('stg_daily_bars') }}
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1