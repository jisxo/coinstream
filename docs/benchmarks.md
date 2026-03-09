# Benchmarks (Throughput / Lag / Latency)

> 목적: “대용량” 근거를 숫자로 제시하기 위한 기록

## Test Setup
- Symbols subscribed:
- Kafka partitions:
- Consumer group size:
- Processor parallelism:
- ClickHouse batch size / flush interval:
- Watermark allowed lateness:

## Results
| Case | symbols | partitions | consumers | max events/sec | p95 e2e latency(ms) | max lag | notes |
|------|---------|------------|----------:|---------------:|--------------------:|--------:|------|
| A (baseline) | | | | | | | |
| B (tuned)    | | | | | | | |

## Evidence
- Grafana screenshot(s): docs/screenshots/
- Redash screenshot(s): docs/screenshots/
