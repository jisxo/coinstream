CREATE DATABASE IF NOT EXISTS crypto;
CREATE DATABASE IF NOT EXISTS coinstream;

-- Aggregated OHLCV (1 minute)
-- ReplacingMergeTree helps converge duplicates (idempotent-ish) by (symbol, window_start)
CREATE TABLE IF NOT EXISTS crypto.ohlc_1m
(
    window_start DateTime64(3, 'UTC'),
    window_end   DateTime64(3, 'UTC'),
    symbol       LowCardinality(String),
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64,
    vwap         Float64,
    emitted_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(emitted_at)
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start);

-- Mart tables for downstream BI + pipeline health tracking
CREATE TABLE IF NOT EXISTS coinstream.mart_ohlcv_1m
    (window_start DateTime64(3, 'UTC'),
     window_end DateTime64(3, 'UTC'),
     symbol String,
     open Float64,
     high Float64,
     low Float64,
     close Float64,
     volume Float64,
     trade_count UInt64,
     vwap Float64,
     late_event_count UInt64 DEFAULT 0,
     window_ms UInt32,
     emitted_at DateTime64(3, 'UTC'),
     result_version UInt64,
     created_at DateTime64(3, 'UTC'))
ENGINE = ReplacingMergeTree(result_version)
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start);

CREATE TABLE IF NOT EXISTS coinstream.mart_pipeline_health_1m
(window_start DateTime64(3, 'UTC'),
 throughput UInt64,
 p95_latency_ms Float64,
 consumer_lag_ms UInt64,
 freshness_seconds Float64,
 late_event_count UInt64,
 created_at DateTime64(3, 'UTC'))
ENGINE = MergeTree()
PARTITION BY toDate(window_start)
ORDER BY window_start;
