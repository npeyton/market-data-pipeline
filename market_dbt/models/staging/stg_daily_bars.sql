SELECT
  ticker,
  CONVERT_TIMEZONE('UTC', 'America/New_York',
    TO_TIMESTAMP_NTZ(window_start, 9))::DATE   AS trade_date,
  open,
  high,
  low,
  close,
  volume,
  transactions,
  ROUND((high + low + close) / 3, 4)           AS vwap_approx,
  file_name,
  loaded_at
FROM {{ source('raw', 'raw_day_aggs') }}
WHERE close > 0
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ticker, trade_date
  ORDER BY loaded_at DESC
) = 1