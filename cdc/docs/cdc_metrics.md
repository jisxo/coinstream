# CDC Metrics Definition

## freshness
- 정의: 소스 변경 시각(`source_updated_at`)과 CDC 적재 시각(`ingested_at`)의 시간차
- 계산 기준: `max/avg(dateDiff('millisecond', source_updated_at, ingested_at))/1000`
- 정상 범위: 평균 0~5초, 최대 30초 이내
- 이상 판단 기준: 평균 10초 초과 또는 최대 60초 초과 5분 지속
- 확인 위치: `cdc/queries/cdc_monitoring.sql`의 freshness 쿼리, Grafana/Redash CDC 패널
- 대응 방법: Kafka Connect 상태 점검, consumer lag 확인, connector/consumer 재시작

## consumer lag
- 정의: Raw 이벤트 최신 오프셋 대비 state applier가 반영 완료한 오프셋 차이
- 계산 기준: `max(cdc_raw_events.source_offset) - max(cdc_consumer_offsets.offset where consumer_name='cdc_state_applier')`
- 정상 범위: 0~200
- 이상 판단 기준: 1,000 초과 10분 지속
- 확인 위치: `cdc/queries/cdc_monitoring.sql`의 consumer lag 쿼리
- 대응 방법: apply worker 리소스 증설, 배치 크기 조정, ClickHouse insert 지연 확인

## duplicate rate
- 정의: 동일 `event_id`가 2회 이상 유입된 비율
- 계산 기준: `sum(max(cnt-1,0))/count(*)`, `cnt = count(*) by event_id`
- 정상 범위: 0~0.5%
- 이상 판단 기준: 2% 초과
- 확인 위치: `cdc/queries/cdc_monitoring.sql`의 duplicate 쿼리
- 대응 방법: connector 재시도 정책 점검, producer 재전송 패턴 확인, dedup key 규칙 재검증

## delete ratio
- 정의: 전체 CDC 이벤트 중 삭제(`op_type='d'`) 이벤트 비중
- 계산 기준: `countIf(op_type='d') / count(*)`
- 정상 범위: 도메인 특성 의존(기준선 대비 급변 여부로 판단)
- 이상 판단 기준: 기준선 대비 3배 이상 급증 또는 급감
- 확인 위치: `cdc/queries/cdc_monitoring.sql`의 delete ratio 쿼리
- 대응 방법: 운영 배치/정책 변경 여부 확인, soft delete/hard delete 로직 정합성 점검

## mart row anomaly
- 정의: 시간 버킷별 mart row의 급격한 변동(건수/합계)
- 계산 기준: `mart_order_metrics_hourly`의 `order_count`, `total_amount` 시계열 추이 비교
- 정상 범위: 전일 동시간대 대비 ±30% 내
- 이상 판단 기준: 전일 동시간대 대비 ±50% 이상
- 확인 위치: `cdc/queries/cdc_monitoring.sql`의 mart hourly trend 쿼리
- 대응 방법: state 반영 누락 여부 확인, delete 반영 여부 점검, refresh job 재실행
