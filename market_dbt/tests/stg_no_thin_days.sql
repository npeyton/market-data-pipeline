-- Fails if any trading day has fewer than 10,000 stocks.
SELECT
  trade_date,
  COUNT(*) AS stocks
FROM {{ ref('stg_daily_bars') }}
GROUP BY trade_date
HAVING COUNT(*) < 10000