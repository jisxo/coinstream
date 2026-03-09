# 운영 루프 완성 TODO

# 다음 항목은 “Binance aggTrade → Kafka → Python processor → ClickHouse/MinIO + Observability” 작업의 완성도를 체크하기 위한 TODO 리스트입니다. 각 항목에서 현재 상태와 다음 단계가 기록되어 있습니다.

| 항목 | 현재 상태 | 다음 조치 |
|---|---|---|
| Python processor: event-time + watermark window(1s/1m) | ✅ 1분 윈도우까지 구현됨(`window_bounds`, `flush_windows`) | 1초 윈도우 추가 여부는 필요시 업그레이드
| Dedup(key=symbol+agg_trade_id) + TTL | ✅ `cachetools.TTLCache`로 `processor/app.py`에서 관리됨, checkpoint로 복원 | TTL/size 조정은 `.env`에서 가능, 상태 기반 모니터링 추가 가능
| Checkpoint(snapshot state + offsets) | ✅ `processor/checkpoint.py` 복원/저장 + config (`CHECKPOINT_STORE`, `CHECKPOINT_INTERVAL_MS`) | 재시작 시 실제 재생 테스트 문서화 (README, docs/architecture.md 참조)
| ClickHouse Hot sink (`crypto.ohlc_1m`) | ✅ window flush에서 생성된 row를 `clickhouse`에 저장 | mart 테이블로 구간별 런칭 + downstream ingest 고려
| MinIO/Parquet Cold sink | ✅ `processor/app.py`에서 `pyarrow` Parquet 생성 후 `MinIO` 업로드 | replay 모드/파일 기반 재처리 스크립트 (`scripts/replay.md`) 추가 예정
| Prometheus metrics + Grafana | ✅ ingest/processor `/metrics` + Prometheus 서비스/GUI | Grafana 대시보드 JSON + README에 지표 스크린샷 경로 추가
| Redash SQL dashboard + 알림/Runbook | ✅ Redash 연결 문서 + query folder 정리 | redash dashboard export + 청사진 Runbook `(docs/runbook.md)` 정리

추가로 필요한 문서/스크립트는 `docs/architecture.md`, `README`, `docs/runbook.md`, `scripts/` 내 파일들에서 차례로 채워 넣으면 됩니다.
