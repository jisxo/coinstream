# Redash Dashboard (초안)

## 목적
- CoinStream의 **시장 KPI(OHLCV/volatility)** + **운영 KPI(freshness/누락)**를 Redash에서 조회 가능하게 구성
- Grafana는 Prometheus 시계열(throughput/lag/latency), Redash는 ClickHouse SQL 기반 KPI 중심(역할 분리)

## 권장 차트 구성(5개 내외)
1) symbol_volatility_ratio Top-N (최근 1h/24h)
2) 선택 symbol의 volatility ratio 시계열
3) 선택 symbol의 OHLCV 시계열(가격/거래량)
4) freshness(최신성) 지표(최근 window 갱신 지연)
5) missing/0건 탐지(최근 N분에 데이터 없는 symbol 수)

## 적용
- `queries/`의 SQL을 Redash Saved Query로 등록 후, Dashboard로 배치
- 필터 변수: {{symbol}}, {{from}}, {{to}} 형태로 Redash 템플릿에 맞게 조정
