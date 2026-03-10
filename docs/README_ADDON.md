# CoinStream Ops/BI Layer Add-on

이 패키지는 **이미 구현된 수집/프로세서 코드에 “덧붙이는(overlay)” 산출물 묶음**입니다.
목표는 고가용/실시간 스트리밍 파이프라인의 운영/BI 증빙을 레포에 **문서화 형태로 남기는 것**입니다.

- KPI 정의 → 마트 테이블 → BI 대시보드(Redash) → 이상징후 알림 → Runbook/Drill

## 적용 방법(권장)
1) 본 폴더 내용을 **기존 CoinStream 레포 루트에 그대로 복사**합니다.
2) 아래 3가지만 프로젝트에 맞게 값을 확정/치환합니다.
   - (A) `symbol_volatility_ratio` 산식/윈도우
   - (B) ClickHouse 테이블명/컬럼명 (이미 존재하면 DDL은 “참고”로만 사용)
   - (C) 알림 임계치(예: lag, freshness, latency)

## 최소 산출물 체크(면접/포트폴리오용)
- docs/kpi_definition.md (지표 정의서)
- sink/clickhouse/init.sql (마트 테이블 DDL)
- dashboards/redash/queries/*.sql (대시보드용 SQL)
- observability/alert_rules.yml (알림 룰)
- docs/runbook.md + incidents/incident_template.md (런북/드릴 기록)
- docs/benchmarks.md (처리량/지연 근거)
