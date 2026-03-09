# Load Test Guide

## 목적
- 멀티 심볼 구독 수를 늘려 이벤트 레이트를 상승시키고
- Kafka 파티션/컨슈머 그룹/프로세서 병렬도를 조절하며
- throughput/lag/latency 변화를 측정

## 절차(예시)
1) symbols=5로 시작 → 20 → 50로 증가
2) partitions=3 → 6 → 12로 변경
3) consumers=1 → 2 → 4로 변경
4) 각 케이스마다 5~10분 관측 후 최대값/분위수 기록

## 기록
- docs/benchmarks.md에 결과 표 업데이트
- docs/screenshots/에 Grafana/Redash 캡처 저장
