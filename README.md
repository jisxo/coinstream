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
Copy `.env.example` to `.env` and fill secrets, then edit it:
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

### Render에서 Prometheus/Grafana 추가
- Prometheus 서비스
  - Runtime: Dockerfile
  - Root Directory: 비움
  - Dockerfile Path: `prometheus/Dockerfile`
  - 내부 포트: `9090`
  - 스크랩 타겟 설정 파일: `prometheus/prometheus.render.yml`
- Grafana 서비스
  - Runtime: Dockerfile
  - Root Directory: 비움
  - Dockerfile Path: `grafana/Dockerfile`
  - 내부 포트: `3000`
  - env: `PROMETHEUS_URL=http://coinstream-prometheus:9090`
  - env: `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD`
- Grafana는 `grafana/dashboards/coinstream-overview.json`를 자동 로드합니다.

## 6) Redash 실행/접속
- 로컬 Compose: `docker compose up -d redash-redis redash-postgres redash redash-worker`
- Render:
  - `redash-web` start command: `/app/bin/docker-entrypoint server`
  - `redash-worker` start command: `python3 /app/manage.py rq worker queries`
- 데이터 소스 연결/테스트는 `docs/redash_setup.md` 기준으로 진행합니다.
- SQL 쿼리는 `docs/dashboards/redash/queries/`에 저장하고, 대시보드 export JSON은 `docs/dashboards/redash/dashboard.json`으로 관리합니다.

## 7) Notes / limitations
- This is a runnable demo. For strict end-to-end exactly-once, you'd typically use Flink checkpointing + transactional sinks.
- Binance WebSocket is public market data; no API key is required for these streams.
- MinIO bucket(`S3_BUCKET`)은 processor가 시작 시 자동 bootstrap 합니다.

## 8) Verification checkpoints
- ClickHouse mart tables
  ```bash
  docker compose exec clickhouse clickhouse-client -q "SHOW TABLES FROM coinstream"
  ```
  `mart_ohlcv_1m`과 `mart_pipeline_health_1m`이 보여야 새 테이블이 정상 생성된 것입니다.
- Processor unit tests
  ```bash
  python3 -m pytest tests/test_processor_logic.py
  ```
  워터마크/late 처리, dedup 경계를 검증합니다.
- Checkpoint recovery run
  1. `docker compose stop processor && docker compose rm -f processor`으로 컨테이너를 내립니다.
  2. `docker compose up -d processor`로 다시 띄우고, 로그에서 `checkpoint_restores_total`이 증가하는지 확인합니다.
  3. `docker compose exec processor curl -s localhost:8000/metrics | grep state_window_count`로 상태 윈도우 수가 반영되는지 확인하세요.
  4. 위 과정을 통해 Kafka offsets+state 복구가 실제로 동작하는지 증명할 수 있습니다.

## 9) Mart / health batch
- `processor/flush_windows`에서 `coinstream.mart_ohlcv_1m`에 동일한 row를 삽입합니다 (`processor/mart.py`의 `write_mart_ohlcv`).
- `mart_pipeline_health_1m`은 주기별 throughput/p95/lag/freshness를 기록하는 테이블로, `write_mart_pipeline_health`를 적절한 시점(예: flush 시점)에서 호출하면 운영 KPI를 쌓을 수 있습니다. 현재는 helper 구조를 만들었으니, 실제 latency/lag 계산 로직이 필요할 경우 해당 helper를 time-window 루프 안에 추가하면 됩니다.

## 9) Documentation index
- `AGENTS.md`: 빠른 시작(Compose/make 등), 테스트·린트 명령, ingest/processor/sink/observability/redash 대시보드 디렉터리 설명, `.env.example` 규칙을 모아두었습니다.
- `docs/project_overview.md`: 디렉터리 맵, Kafka/processor/ClickHouse/MinIO/Prometheus 흐름, 구동/검증 명령을 정리한 전체 개요입니다.
- `docs/kafka_schema.md`: `trades_raw`/`trades_dlq` 토픽과 `event_time_ms`, `symbol`, `agg_trade_id` 등 메시지 JSON 스키마 요약입니다.
- `docs/architecture.md`: ClickHouse mart 테이블, idempotent 전략, dedup/checkpoint/MinIO 흐름 설명을 담고 있습니다.
- `docs/operational_todo.md`: 운영 루프 완성과 관찰 포인트를 위한 항목별 진행 상태와 다음 조치 체크리스트입니다.
- `docs/runbook.md` + `docs/incidents/incident_template.md`: alert별 체크리스트/조치·incidents 기록 포맷입니다.
- `docs/minio_bootstrap.md`: MinIO bucket 자동 bootstrap 동작과 Render 점검 절차입니다.
- `docs/observability_render.md`: Render에 Prometheus/Grafana를 붙이는 단계별 설정 가이드입니다.
- `alertmanager/Dockerfile`, `alertmanager/entrypoint.sh`: Render Alertmanager 이메일 알림 서비스 구성입니다.
- `docs/redash_setup.md` + `docs/dashboards/redash/queries/`: Redash 연결/데이터소스 등록과 Saved Query 템플릿입니다.
- `docs/operations_shutdown.md`: 운영 종료/재가동 순서와 백업 체크리스트입니다.
- `docs/observability/alert_rules.yml`, `prometheus/prometheus.yml`: Prometheus scraping/alert 룰과 Alertmanager 연계 기준을 정의했습니다.
- `prometheus/Dockerfile`, `prometheus/prometheus.render.yml`: Render용 Prometheus 이미지/스크랩 설정입니다.
- `grafana/Dockerfile`, `grafana/provisioning/*`, `grafana/dashboards/*`: Render용 Grafana 자동 프로비저닝 구성입니다.
- `docs/benchmarks.md` + `docs/scripts/load_test.md`: 벤치마크/로드 테스트 템플릿과 실행 절차를 기록합니다.
- `docs/benchmarks.md` 및 `docs/scripts/load_test.md`에 따라 결과를 수기 작성하거나 스크립트에 추가해도 됩니다.
