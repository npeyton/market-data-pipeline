SELECT
  ticker,
  trade_date,
  open,
  high,
  low,
  close,
  LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_close,
  LAG(high)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_high,
  ROUND((close / prev_close - 1) * 100, 2)                  AS pct_return,
  low > prev_high                                           AS gap_up,
  volume,
  ROUND(volume * vwap_approx)                               AS dollar_volume
FROM {{ ref('stg_daily_bars') }}