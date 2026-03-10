# Redash Dashboard 가이드

## 목적
- CoinStream의 **시장 KPI(OHLCV/volatility)** + **운영 KPI(freshness/누락)**를 Redash에서 조회 가능하게 구성
- Grafana는 Prometheus 시계열(throughput/lag/latency), Redash는 ClickHouse SQL 기반 KPI 중심(역할 분리)

## 권장 차트 구성
1) `01_volatility_topn.sql` : 변동성 Top-N
2) `02_volatility_timeseries.sql` : symbol 변동성 시계열
3) `03_ohlcv_1m.sql` : symbol OHLCV 시계열
4) `04_freshness.sql` : 최신 적재 지연
5) `05_missing_symbols.sql` : 최근 5분 미수집 symbol 탐지
6) `06_late_event_trend.sql` : late event 추이

## 적용 순서
1. `queries/` SQL을 Redash에서 Saved Query로 각각 저장
2. Visualize 설정 후 대시보드에 위젯 추가
3. 대시보드 완성 후 `Share -> Export`로 JSON 내보내기
4. 내보낸 파일로 `docs/dashboards/redash/dashboard.json` 업데이트

## 참고
- 현재 `docs/dashboards/redash/dashboard.json`은 템플릿 형태입니다.
- 실제 운영본은 Redash UI에서 export한 JSON을 기준으로 관리하세요.
