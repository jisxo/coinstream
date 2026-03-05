# Real-time Crypto Streaming Pipeline (Interview-ready, runnable)

This repo implements a runnable, local, end-to-end streaming pipeline:

**Binance WebSocket (`aggTrade`) → Kafka-compatible broker (Redpanda) → Python window processor (event-time + watermark + dedup) → ClickHouse + MinIO(Parquet) → Prometheus**

> Why Redpanda? It is Kafka-API compatible and easy to run locally without ZooKeeper/KRaft complexity.
> If you must use Apache Kafka, you can swap the broker; the rest remains similar.

## 0) Prerequisites
- Docker + Docker Compose (v2)

## 1) Quick start

### 1. Start infra
```bash
docker compose up -d --build
```

### 2. Create topics (one-time)
```bash
docker compose exec redpanda rpk topic create trades_raw -p 12 -r 1
docker compose exec redpanda rpk topic create trades_dlq -p 3 -r 1
```

### 3. Check ClickHouse tables
```bash
docker compose exec clickhouse clickhouse-client -q "SHOW TABLES FROM crypto"
```

### 4. Watch logs
```bash
docker compose logs -f ingest
# in another terminal
docker compose logs -f processor
```

### 5. Query results (OHLC 1m)
```bash
docker compose exec clickhouse clickhouse-client -q "
SELECT symbol, window_start, open, high, low, close, volume, trade_count
FROM crypto.ohlc_1m
ORDER BY window_start DESC
LIMIT 20"
```

### 6. MinIO 확인
- Console: http://localhost:9001
- Bucket: `crypto`
- Prefix: `ohlc_1m/`

## 2) Key config knobs (important for “high-volume” claims)
Edit `.env` (copy from `.env.example`):
- `SYMBOLS`: increase number of symbols to increase event rate
- `WINDOW_MS`: 60000 for 1m OHLCV
- `ALLOWED_LATENESS_MS`: watermark lateness tolerance
- `DEDUP_*`: dedup TTL and cache size

## 3) Scale out the processor (parallelism)
Increase consumer parallelism by running multiple `processor` containers:
```bash
docker compose up -d --scale processor=3
```
All processors share the same consumer group; partitions are distributed automatically.

## 4) What makes this “interview-ready”
- **Event-time + watermark**: windows finalize based on watermark = max_event_time - allowed_lateness
- **Dedup**: TTL cache on (symbol, agg_trade_id) to tolerate duplicates on reconnect
- **Idempotent-ish sink**: ClickHouse uses `ReplacingMergeTree` keyed by (symbol, window_start) so duplicates converge
- **Observability**: `ingest` and `processor` expose Prometheus metrics on `/metrics`

## 5) Prometheus
- Prometheus UI: http://localhost:9090
- Metrics endpoints:
  - ingest: http://localhost:8001/metrics
  - processor: http://localhost:8002/metrics

## 6) Redash 실행/접속
- `docker compose up -d redash-redis redash-postgres redash`으로 Redash+Redis+Postgres를 띄운 뒤 http://localhost:5000 으로 접속하면 최초 로그인 화면 또는 관리자 생성 화면이 나타납니다.
- Redash UI에서 `docs/redash_setup.md`에 기록한 순서대로 ClickHouse 데이터 소스를 추가하고 `SELECT 1 AS readiness` 등을 실행해 보세요.
- 운영 증빙용 쿼리나 대시보드는 `dashboards/redash/queries/` 경로에 저장하고, 필요한 경우 `docs/kpi_definition.md` / `observability/alert_rules.yml`도 함께 갱신합니다.

## 7) Notes / limitations
- This is a runnable demo. For strict end-to-end exactly-once, you'd typically use Flink checkpointing + transactional sinks.
- Binance WebSocket is public market data; no API key is required for these streams.
