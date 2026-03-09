-- ClickHouse Mart DDL (초안)
-- 이미 테이블이 있다면: "운영 증빙용 스키마 문서"로 사용해도 무방

CREATE DATABASE IF NOT EXISTS coinstream;

-- 1) OHLCV Mart (1m)
CREATE TABLE IF NOT EXISTS coinstream.mart_ohlcv_1m
(
  window_start DateTime,
  window_end   DateTime,
  symbol       String,
  open         Float64,
  high         Float64,
  low          Float64,
  close        Float64,
  volume       Float64,
  trade_count  UInt64,
  vwap         Float64,
  created_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start);

-- 2) Symbol KPI Mart (1m) - volatility ratio 등
CREATE TABLE IF NOT EXISTS coinstream.mart_kpi_symbol_1m
(
  window_start DateTime,
  window_end   DateTime,
  symbol       String,
  symbol_volatility_ratio Float64,
  late_event_count UInt64 DEFAULT 0,
  created_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start);

-- 3) Pipeline Health Mart (1m) - 운영 관측 지표
CREATE TABLE IF NOT EXISTS coinstream.mart_pipeline_health_1m
(
  window_start DateTime,
  window_end   DateTime,
  ingest_rate  Float64,   -- events/sec (또는 events/min)
  process_rate Float64,
  consumer_lag UInt64,
  e2e_latency_p95_ms UInt64,
  freshness_sec UInt64,
  checkpoint_duration_ms UInt64 DEFAULT 0,
  state_size_bytes UInt64 DEFAULT 0,
  created_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (window_start);
