요구사항:
- 이 레포는 “Binance aggTrade → Kafka → Python processor → ClickHouse/MinIO + Prometheus/Grafana/Redash” 스트리밍 데모입니다.
- 작업 원칙:
  1) 변경 전/후로 실행 가능한 검증 명령을 반드시 제시하고, 가능하면 실제로 실행해 주세요.
  2) 큰 작업은 작은 PR 단위로 쪼개 주세요.
  3) 기존 ingestion 코드(이미 구현된 WS→Kafka)는 최대한 건드리지 말고, 필요한 경우에만 최소 변경으로 해 주세요.
  4) Python 3.11 기준, 코드 스타일은 ruff/black(없으면 추가) + 타입 힌트 선호.
  5) 새 의존성 추가 시, 꼭 필요한 것만. 추가 이유를 PR 설명에 남겨 주세요.
