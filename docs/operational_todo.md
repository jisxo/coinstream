# 운영 루프 완성 TODO

현재 스택(`Binance aggTrade -> Redpanda -> processor -> ClickHouse/MinIO + Prometheus/Grafana/Redash`) 기준 TODO입니다.

| 항목 | 현재 상태 | 다음 조치 |
|---|---|---|
| Processor event-time/watermark + 다중 윈도우 | ✅ 구현 완료 (`processor/window.py`, `processor/app.py`) | 윈도우별 리소스 사용량 벤치마크 갱신 |
| Dedup + TTL + checkpoint 복구 | ✅ 구현 완료 (`processor/checkpoint.py`) | 체크포인트 복구 drill을 주기적으로 기록 |
| ClickHouse Hot sink + Mart | ✅ `crypto.ohlc_1m`, `coinstream.mart_*` 운영 중 | mart 기반 Redash 쿼리 저장본 확대 |
| MinIO/Parquet Cold sink | ✅ 업로드 동작 확인됨 | 운영 종료 전 백업/정리 루틴 문서화 유지 |
| Alert 룰 정합성 | ✅ 코드 메트릭명 기준으로 정렬 완료 (`docs/observability/alert_rules.yml`) | Alertmanager 연동 시 채널(슬랙 등)만 추가 |
| Redash 운영 | ✅ web/worker 분리 운영, worker on-demand 가능 | 실제 대시보드 export JSON 갱신 |
| 문서 동기화 | ✅ Render 중심 문서로 갱신 중 | 새 배포 방식 변경 시 즉시 업데이트 |

참고:
- Redash worker는 비용 최적화를 위해 필요 시에만 `Resume`해서 사용합니다.
- Prometheus를 내리면 Grafana 시계열에 공백이 생기므로 목적에 맞춰 ON/OFF를 결정하세요.
