# KPI 정의서 (CoinStream)

> 목적: **KPI 정의 → 마트 → BI → 알림 → 대응(Runbook/Drill)** 운영 루프 증빙

## 공통 규칙
- 집계 단위: 기본 `1m tumbling window` (필요 시 1s/5m 추가)
- 파티션/세그먼트: `symbol` 기준
- 갱신 주기: 실시간(스트림) + 대시보드는 최근 N분/시간 범위 조회

---

## KPI-01: OHLCV (1m, per symbol)
- 목적: 심볼별 가격/거래량 변동 관측(기본 시장 KPI)
- 산식:
  - open: window 내 첫 trade price
  - high/low: window 내 max/min price
  - close: window 내 마지막 trade price
  - volume: sum(quantity)
  - trade_count: count(*)
  - vwap: sum(price*qty) / sum(qty)
- 윈도우/갱신: 1m tumbling, event-time 기준
- 저장: ClickHouse `mart_ohlcv_1m` (PK: window_start, symbol)

## KPI-02: symbol_volatility_ratio (per symbol)
- 목적: 변동성 급변 심볼 탐지(대시보드 Top-N, 알림 후보)
- 윈도우/갱신: 기본 5m 또는 1h rolling (선택)

### Option A (MVP 권장, 구현 단순)
- 정의: (window_high - window_low) / window_open
- 입력: KPI-01 OHLCV
- 장점: 빠르고 직관적, 대시보드 Top-N에 적합
- 한계: 분포 기반 변동성(표준편차) 의미와는 다름

### Option B (고도화)
- 정의: log return의 표준편차 / (기준 기간의 평균) 등
- 입력: tick 또는 더 촘촘한 window
- 장점: 통계적 변동성에 가까움
- 한계: 구현/상태 관리 난이도 상승

## KPI-03: Throughput (events/sec or events/min)
- 목적: “대용량 처리” 근거(숫자로 말하기)
- 정의: 단위시간당 수집/처리 이벤트 수
- 저장: `mart_pipeline_health_1m`

## KPI-04: Consumer Lag (Kafka)
- 목적: 지연/정체 감지(알림 핵심)
- 정의: consumer group lag (topic/partition 합)
- 수집: Prometheus exporter 또는 프로세서 자체 metric
- 시각화: Grafana(시계열) + 필요 시 Redash에도 요약 표시

## KPI-05: Freshness / E2E latency (p95)
- 목적: end-to-end 지연 관측 + SLA 기반 알림
- 정의:
  - freshness: now - max(window_end) OR now - max(updated_at)
  - e2e latency: ingest_time 대비 window_end/created_at 차이(가능한 정의 선택)
- 저장: `mart_pipeline_health_1m` 또는 Prometheus metric

## KPI-06: Late Event Count (Watermark 초과)
- 목적: watermark 정책 영향/데이터 품질 관측
- 정의: allowed lateness(예: 5s) 초과 이벤트 카운트
- 저장: `mart_pipeline_health_1m` 또는 `mart_kpi_symbol_1m`
