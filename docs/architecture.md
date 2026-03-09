# Architecture & idempotency notes

## ClickHouse mart tables
- `coinstream.mart_ohlcv_1m`: 1분 OHLCV를 symbol/window_start 기준으로 저장하고 `ReplacingMergeTree(result_version)`을 이용해 `result_version`이 높은 행만 남겨 중복/재처리 상태를 자동 수렴시킵니다. `late_event_count`을 컬럼으로 추가해 워터마크 밖 이벤트도 감시할 수 있도록 했습니다.
- `coinstream.mart_pipeline_health_1m`: throughput/p95/lag/freshness 등 KPI를 1분 단위로 기록하며, 간단한 `MergeTree` 엔진에 `window_start`를 ORDER BY로 사용합니다.

## Dedup + checkpoint
- Processor는 `(symbol, agg_trade_id)` 키로 `cachetools.TTLCache`를 유지해 `DEDUP_TTL_SECONDS` 동안만 중복을 허용하지 않고, 워터마크 계산 시 마감된 window만 flush합니다.
- `processor/checkpoint.py`에서 dedup state, window state, Kafka offset을 주기적으로 JSON으로 직렬화/저장하며, 재시작 시 복원합니다. 파일 기반 구현은 atomic rename을 사용하고, 필요 시 Redis/ClickHouse로 확장 가능한 인터페이스를 제공합니다.

## MinIO Parquet sink
- Flush된 OHLCV row는 ClickHouse에 `crypto.ohlc_1m`으로 쓰이고, 동시에 `pyarrow`로 Parquet을 생성해 `MinIO`에 `s3://<bucket>/ohlc_1m/dt=YYYY-MM-DD/hour=HH/`에 업로드합니다. 이중 적재 덕분에 ClickHouse가 장기 저장/쿼리 역할을 맡고, MinIO는 cold path나 replay에 사용됩니다.
