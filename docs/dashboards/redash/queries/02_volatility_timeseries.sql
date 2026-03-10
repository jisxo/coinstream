-- Volatility time series (symbol 선택)
-- Redash 파라미터 예: {{symbol}} (기본 BTCUSDT)
SELECT
  window_start,
  symbol,
  if(low = 0, 0, (high - low) / low) AS symbol_volatility_ratio
FROM coinstream.mart_ohlcv_1m
WHERE symbol = '{{symbol}}'
  AND window_start >= now() - INTERVAL 24 HOUR
ORDER BY window_start;
