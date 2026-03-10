# Runbook (CoinStream)

> 목표: 알림 발생 시 **확인 → 원인 후보 → 조치 → 재발 방지** 절차를 체크리스트로 표준화

## 공통 확인 경로
- Render 운영: 각 서비스(Render Dashboard -> Logs / Shell / Events)
- 로컬 운영: `docker compose ps`, `docker compose logs -f <service>`

## Processor Down
1) 서비스 상태와 로그 확인 (Render `coinstream-processor` 또는 `docker compose ps processor`)
2) `.env`/`processor/config.py` 변경 여부 점검 및 Secret 유효성 확인
3) Kafka 브로커(`redpanda`)와 `trades_raw` 접근성 확인
4) 재시작 후 checkpoint restore 로그(`checkpoint_restores_total`)와 lag 감소 확인
- 재발 방지: stack trace/metrics + `docs/incidents/incident_template.md`에 기록

## Consumer Lag High
1) Grafana/Prometheus(`processor_lag_seconds`, `consumer_lag_ms_approx`)에서 lag 상승 시점 파악
2) processor throughput 대비 ingest events/sec 비율 점검
3) ClickHouse insert 오류/slow write 확인 (`processor/app.py` metric + log)
4) 조치
   - processor 병렬도/컨슈머 그룹 확대
   - ClickHouse insert batch 조정 또는 마이크로 배치 감소
   - 필요시 실시간 인입(symbol 수) 줄이기
- 재발 방지: tuning 파라미터와 결과를 `docs/benchmarks.md`에 기록

## Freshness High
1) `coinstream.mart_ohlcv_1m`/minio 파티션에 최신 timestamp가 들어오는지 확인
2) ingest reconnect/WS latency 증가 추적(`ingest` metrics) 및 Kafka ingest rate 확인
3) processor watermark/allowed lateness 설정, `state_window_count` 감소 여부 확인
4) 조치: ingest 재접속, processor 재시작, backlog 또는 checkpoint recovery 확인

## E2E Latency High
1) Prometheus `window_emission_latency_seconds` p95 지표 확인과 lag/throughput 상관관계 점검
2) ClickHouse insert 지연 여부 및 minio upload(Parquet) 지연 체크
3) watermark/allowed lateness 설정이 적절한지 검토
- 조치: batch flush 주기/size 조정, watermark, Kafka fetch tuning

## MinIO NoSuchBucket
1) processor env의 `S3_ENDPOINT`, `S3_BUCKET`, 키 값이 올바른지 확인
2) `processor` 재배포 후 동일 에러가 반복되는지 확인
3) `processor` startup bucket bootstrap(`ensure_s3_bucket`) 로그/동작 확인
4) 필요 시 `docs/minio_bootstrap.md`의 Shell 검증 커맨드로 bucket/object 상태 확인

## Incident drill
1) alert name/measurements를 `docs/incidents/incident_template.md`에 기록
2) 로그/metrics 스냅샷(Render Logs 또는 `docker compose logs`, Prometheus query) 첨부
3) 재발 방지를 위한 test case/monitor rule 업데이트
