-- Volatility time series (symbol 선택)
-- Redash 템플릿 변수 예: {{symbol}}
SELECT
  window_start,
  symbol_volatility_ratio
FROM coinstream.mart_kpi_symbol_1m
WHERE symbol = '{{symbol}}'
  AND window_start >= now() - INTERVAL 24 HOUR
ORDER BY window_start;
