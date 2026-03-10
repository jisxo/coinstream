# Project overview & quick flow

이 저장소는 Binance aggTrade 스트림 → Kafka(Redpanda) → Python processor → ClickHouse/MinIO + Observability(Prometheus/Grafana/Redash)까지 연결된 스트리밍 데모입니다. 아래 항목은 주요 디렉터리/실행 흐름을 정리한 것입니다.

## 디렉터리/파일 맵
- `ingest/` : Binance WebSocket을 구독하고 `normalize_event`로 `event_time_ms`, `symbol`, `agg_trade_id`, `price`, `quantity`, `is_buyer_maker`, `stream` 필드를 갖는 JSON 메시지를 만들고 `trades_raw` 토픽에 전송합니다. `ingest/app.py`가 진입점입니다.
- `processor/` : Kafka consumer(`processor/app.py`)에서 워터마크/윈도우를 계산하고 `processor/window.py`의 `OhlcAgg`를 채운 뒤 ClickHouse/MinIO 및 checkpoint/metrics를 처리합니다. `processor/config.py`는 환경변수 파서를, `processor/metrics.py`는 Prometheus 계측을, `processor/checkpoint.py`는 상태 스냅샷을 담당합니다.
- `clickhouse/` : `init.sql`에 `crypto`/`coinstream` 데이터베이스와 OLHCV/마트 테이블 정의가 있습니다.
- `docs/` : `redash_setup.md`, `kpi_definition.md`, `observability/`, `dashboards/` 등 운영/모니터링/쿼리 자료가 모여 있습니다.
- `prometheus/` : `prometheus.yml`이 있고, 프로세서/ingest/Redpanda 메트릭을 수집합니다.

## 주요 실행 흐름
1. `.env.example`을 `.env`로 복사하고 비밀값을 채웁니다 (`CLICKHOUSE_PASSWORD`, `REDASH_COOKIE_SECRET` 등).
2. `docker compose up -d --build`를 실행하면 Redpanda, ClickHouse, MinIO, Redis, ingest, processor, prometheus, redash 등 전체 스택이 올라옵니다.
3. Kafka 토픽이 필요하면 `docker compose exec redpanda rpk topic create ...`로 직접 생성합니다(이미 `trades_raw`, `trades_dlq`가 빈 상태로 존재).
4. `docker compose logs -f ingest`/`processor`로 수집/처리 상태를 확인하고, `docker compose exec clickhouse clickhouse-client -q "SHOW TABLES FROM crypto"` 등으로 ClickHouse 테이블을 확인합니다.

### Render 운영 시 핵심 포인트
- `coinstream-processor`, `coinstream-ingest`는 같은 region에서 동작해야 내부 DNS 지연/해석 실패를 줄일 수 있습니다.
- Redash worker start command는 `python3 /app/manage.py rq worker queries`를 사용합니다.
- Redash worker는 on-demand로 Resume/Suspend해서 비용을 줄일 수 있습니다.

## 검증 명령 (변경 전/후 기준)
- `docker compose exec ingest python -m ingest.app` (ingest 독립 실행) 또는 `docker compose up -d ingest` (compose)을 통해 WebSocket → Kafka 흐름을 테스트합니다.
- `docker compose exec processor python -m processor.app`로 processor를 수동 실행해 Kafka → ClickHouse/MinIO/Prometheus 경로를 점검합니다.
- `docker compose exec redpanda rpk topic list` 및 `rpk topic describe trades_raw`로 토픽/파티션을 확인합니다.
- `docker compose exec redash curl -vsSf "http://clickhouse:8123/?database=crypto&query=SELECT%201"`로 ClickHouse 연결을 추가 검증합니다.

## 참고
- ingestion 설계는 `ingest/app.py`의 `normalize_event` 함수를 중심으로 하고, Kafka 메시지는 `shared/config.py`의 환경변수로 제어됩니다.
- `processor/app.py`는 dedup/워터마크/유도된 metrics/Parquet flush/Checkpoint를 한 사이클로 실행합니다.
- Redash/Prometheus/Grafana 관련 문서는 `docs/redash_setup.md`, `docs/observability`에서 계속 업데이트되어야 합니다.
