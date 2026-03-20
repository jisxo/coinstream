# CDC Runbook

| 증상 | 확인 위치 | 원인 후보 | 조치 방법 | 재처리 필요 여부 |
|---|---|---|---|---|
| CDC 이벤트 미유입 | Kafka topic(`cdc_coinstream.public.orders`), Kafka Connect `/connectors/{name}/status` | connector down, slot/publication 미생성, Postgres logical 설정 누락 | connector 상태 복구, postgres `wal_level=logical`/slot/publication 점검 후 connector 재등록 | 필요 시 snapshot 재수행 |
| consumer lag 증가 | `cdc_consumer_offsets`, `cdc_raw_events` offset 차이 | apply worker 중단, ClickHouse write 지연, 배치 크기 과대 | apply 프로세스 재기동, batch limit 축소, ClickHouse 성능 점검 | 보통 불필요(오프셋 기반 catch-up) |
| duplicate 급증 | duplicate rate 쿼리, raw event_id 빈도 | connector retry 폭증, 재시작 반복, event_id 규칙 불안정 | retry/backoff 점검, event_id/dedup_key 규칙 보정, 중복 이벤트 샘플 추적 | 불필요(상태 반영 시 중복 무시) |
| delete 반영 누락 | `order_current_state`의 `is_deleted`, raw `op_type='d'` | delete 이벤트 파싱 실패, source_pk 추출 실패, old-event 필터 오적용 | delete 파싱/키 추출 로직 점검, 누락 구간 재적용 | 필요(누락 구간 재적용 권장) |
| mart 갱신 지연 | `mart_order_metrics_hourly.load_ts`, refresh 실행 로그 | state apply는 정상이나 mart refresh 실패, 스케줄 지연 | mart refresh 함수 수동 실행, refresh 주기/오류 로그 점검 | 필요(해당 시간대 mart 재계산) |
