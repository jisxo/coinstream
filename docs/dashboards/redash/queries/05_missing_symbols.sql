-- Missing symbols: 최근 5분 동안 데이터 없는 symbol 수(간단 버전)
-- 전체 symbol 목록 테이블이 있다면 left join으로 정교화 가능
SELECT
  countDistinct(symbol) AS active_symbols_last_5m
FROM coinstream.mart_ohlcv_1m
WHERE window_start >= now() - INTERVAL 5 MINUTE;
