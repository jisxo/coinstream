# Redash 운영 지원

이 문서는 로컬 Compose 환경에서 Redash를 띄우고 ClickHouse를 데이터 소스로 연결하는 기본 흐름을 정리합니다.

## 1. Redash 스택 실행

1. `.env`에서 `REDASH_*` 값을 검토하고 필요하면 강력한 `COOKIE_SECRET`/`SECRET_KEY` 및 Postgres 비밀번호를 채웁니다.
2. 다음 명령으로 Redash + 의존 서비스를 백그라운드로 올립니다:
   ```bash
   docker compose up -d redash-redis redash-postgres redash
   ```
3. `docker compose logs -f redash`를 보면 `Starting server` 이후 `Running on` 로그를 확인할 수 있고, Redash Health 체크(`http://localhost:5000/health`)가 통과하면 UI 접속 준비가 끝난 것입니다.
4. 기본 화면(`http://localhost:5000`)으로 접속하면 최초 관리자 계정을 만들라는 안내가 나오며, 이메일/패스워드를 입력해서 계정을 생성합니다.

## 2. ClickHouse 데이터 소스 등록

1. Redash 좌측 상단 메뉴 → **Data Sources** → **Create** 를 클릭합니다.
2. `Type`에서 **ClickHouse**를 선택합니다.
3. 다음 값을 채웁니다:
   | 필드 | 값 |
   | --- | --- |
   | Name | `ClickHouse` (기호에 따라 변경) |
   | Host | `clickhouse` (Compose 네트워크 이름) |
   | Port | `8123` |
   | Database Name | `crypto` |
   | Username | `default` |
   | Password | (기본 ClickHouse는 빈 값) |
   | HTTP Path | `/` |
   | Use HTTPS | 꺼짐 |
4. `Save` 버튼을 누르면 Redash가 입력한 정보를 검증하고 `TEST CONNECTION` 버튼이 활성화됩니다.
5. `TEST CONNECTION`을 클릭해 성공 메시지를 확인합니다.

## 3. 연결 확인 / 테스트 쿼리

ClickHouse 데이터 소스가 추가되면 다음 쿼리를 실행해서 연결/쿼리 결과를 확인합니다:
```sql
SELECT 1 AS readiness;
```
이 결과가 반환되면 Redash가 ClickHouse로 쿼리 요청을 보내고 성공적으로 응답을 받았다는 의미입니다.

## 4. 운영 증빙

- `docs/kpi_definition.md`에 `symbol_volatility_ratio` 등 실제 지표 정의를 기록하세요.
- `dashboards/redash/queries/`에 ClickHouse 기반 쿼리를 저장해 두면 나중에 Redash 대시보드를 복원하거나 사례로 공유하기 좋습니다.
- 알림 기준을 `observability/alert_rules.yml`에 정리하고, Runbook·Incidents 문서도 함께 업데이트하면 JD 증빙 흐름이 완전히 갖춰집니다.
