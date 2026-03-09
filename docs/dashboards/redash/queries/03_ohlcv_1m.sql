-- OHLCV 1m (symbol 선택)
SELECT
  window_start,
  open, high, low, close,
  volume,
  trade_count,
  vwap
FROM coinstream.mart_ohlcv_1m
WHERE symbol = '{{symbol}}'
  AND window_start >= now() - INTERVAL 24 HOUR
ORDER BY window_start;
