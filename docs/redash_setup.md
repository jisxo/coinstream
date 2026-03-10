# Redash 운영 가이드 (Render 기준)

이 문서는 Render에 올린 Redash(web/worker)와 ClickHouse 데이터 소스를 안정적으로 운영하는 기준을 정리합니다.

## 1) 서비스 설정

### redash-web
- Service type: Web Service
- Start command: `/app/bin/docker-entrypoint server`
- 필수 env:
  - `REDASH_DATABASE_URL`
  - `REDASH_REDIS_URL`
  - `REDASH_COOKIE_SECRET`
  - `REDASH_SECRET_KEY`
- 권장 env:
  - `REDASH_WEB_WORKERS=1`
  - `REDASH_WEB_WORKER_TIMEOUT=120`

### redash-worker
- Service type: Background Worker
- Start command: `python3 /app/manage.py rq worker queries`
- 필수 env:
  - `REDASH_DATABASE_URL`
  - `REDASH_REDIS_URL`
  - `REDASH_COOKIE_SECRET`
  - `REDASH_SECRET_KEY`

## 2) ClickHouse 데이터 소스 등록

1. Redash UI -> Settings -> Data Sources -> New Data Source -> ClickHouse
2. 아래 값 입력:
   | 필드 | 값 |
   | --- | --- |
   | Name | `ClickHouse` |
   | URL | `http://coinstream-clickhouse:8123` |
   | Database Name | `coinstream` |
   | Username | `redash` |
   | Password | `<CLICKHOUSE_PASSWORD>` |
3. Save 후 `Test Connection`

## 3) 연결 확인

Redash web Shell에서 직접 확인:

```bash
python3 - <<'PY'
import urllib.request
u = "http://coinstream-clickhouse:8123/?database=coinstream&query=SELECT%201"
res = urllib.request.urlopen(u, timeout=5)
print(res.status, res.read().decode().strip())
PY
```

- 결과가 `200 1`이면 네트워크는 정상입니다.

## 4) 운영 체크

- web/worker 정상 여부:
  - web 로그에 500 반복 없음
  - worker 로그에 `Listening on queries...`, `Job OK`
- 데이터 소스 테스트 후 아래 쿼리로 데이터 확인:
  ```sql
  SELECT count() AS rows_10m
  FROM coinstream.mart_ohlcv_1m
  WHERE window_start >= now() - INTERVAL 10 MINUTE;
  ```

## 5) 대시보드 산출물 관리

- SQL 파일: `docs/dashboards/redash/queries/*.sql`
- 대시보드 템플릿: `docs/dashboards/redash/dashboard.json`
- 실제 운영본은 Redash UI에서 `Export` 후 위 파일을 덮어써서 버전 관리합니다.
