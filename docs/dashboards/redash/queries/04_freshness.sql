-- Freshness: 최신 window_end 기준 지연(초)
SELECT
  now() AS ts,
  dateDiff('second', max(window_end), now()) AS freshness_sec
FROM coinstream.mart_ohlcv_1m
WHERE window_start >= now() - INTERVAL 6 HOUR;
