-- Missing symbols: 최근 5분 동안 집계가 없는 symbol 탐지
-- expected_symbols는 운영 중인 SYMBOLS 값에 맞춰 수정
WITH expected_symbols AS (
  SELECT arrayJoin(['BTCUSDT', 'ETHUSDT', 'SOLUSDT']) AS symbol
),
recent_symbols AS (
  SELECT DISTINCT symbol
  FROM coinstream.mart_ohlcv_1m
  WHERE window_start >= now() - INTERVAL 5 MINUTE
)
SELECT
  countIf(r.symbol IS NULL) AS missing_symbols_last_5m,
  groupArrayIf(e.symbol, r.symbol IS NULL) AS missing_symbol_list
FROM expected_symbols e
LEFT JOIN recent_symbols r USING (symbol);
