# 운영 종료 루틴

목표: 1~2주 데모 운영 종료 시 데이터 유실 없이 서비스를 안전하게 정리합니다.

## 1) 종료 전 체크
1. ClickHouse 최신 데이터 확인
   ```sql
   SELECT max(window_start), count()
   FROM coinstream.mart_ohlcv_1m
   WHERE window_start >= now() - INTERVAL 1 HOUR;
   ```
2. processor 메트릭 확인 (`processor_messages_total`, `checkpoint_snapshots_total`)
3. Redash/Grafana 필요한 스크린샷 저장

## 2) 데이터 백업
1. ClickHouse에서 CSV 추출
   - `coinstream.mart_ohlcv_1m`
   - `coinstream.mart_pipeline_health_1m`
   - `crypto.ohlc_1m`
2. 백업 파일을 R2 또는 로컬 스토리지에 업로드
3. 업로드 검증: 파일 목록/사이즈/샘플 조회

## 3) 서비스 종료 순서 (Render)
1. 분석 계층 중지
   - `redash-worker` -> Suspend
   - `redash-web` -> Suspend
   - `grafana` -> Suspend (사용 중이면 유지)
   - `prometheus` -> Suspend (시계열 이력 필요하면 유지)
2. 수집 계층 중지 (완전 종료 시)
   - `coinstream-ingest` -> Suspend
   - `coinstream-processor` -> Suspend
   - `coinstream-redpanda` -> Suspend
3. 저장소 계층 중지 (필요 시)
   - `coinstream-clickhouse` -> Suspend
   - `coinstream-minio` -> Suspend

## 4) 재가동 순서 (Render)
1. `coinstream-redpanda`
2. `coinstream-clickhouse`, `coinstream-minio`
3. `coinstream-ingest`
4. `coinstream-processor`
5. `prometheus`, `grafana`, `redash-web`, `redash-worker`

## 5) 사후 검증
1. processor 로그에 ClickHouse/MinIO 연결 에러 없는지 확인
2. ClickHouse 최근 10분 데이터 count 증가 확인
3. Redash에서 `SELECT 1`과 OHLCV 쿼리 실행 확인
