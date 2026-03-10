-- Volatility Top-N (최근 1시간, 기본 Top 10)
SELECT
  symbol,
  avg(if(low = 0, 0, (high - low) / low)) AS avg_volatility_ratio_1h,
  max(if(low = 0, 0, (high - low) / low)) AS max_volatility_ratio_1h,
  sum(volume) AS total_volume_1h,
  sum(trade_count) AS total_trade_count_1h
FROM coinstream.mart_ohlcv_1m
WHERE window_start >= now() - INTERVAL 1 HOUR
GROUP BY symbol
ORDER BY avg_volatility_ratio_1h DESC
LIMIT 10;
