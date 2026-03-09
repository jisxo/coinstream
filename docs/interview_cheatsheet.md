# Interview cheat sheet (talk track)

## What did you build?
- A real-time streaming pipeline that ingests high-frequency market data via WebSocket,
  buffers it in Kafka, performs event-time window aggregation with watermarking and dedup,
  and stores hot (ClickHouse) + cold (S3/Parquet via MinIO) outputs.
- Added observability using Prometheus metrics for throughput/lag and emission latency.

## Why is it “real-time”?
- Long-lived WebSocket stream; processing starts immediately as events arrive.
- Uses event-time semantics (trade time) + watermark to handle out-of-order arrivals.

## Why is it “high-volume”?
- Increase symbols; scale partitions; scale processors in a consumer group.
- Show metrics: records/sec, lag, p95 emission latency.

## Dedup / correctness
- Dedup key: (symbol, agg_trade_id) with TTL to handle reconnect duplicates.
- Watermark: max_event_time - allowed_lateness; windows finalize when window_end <= watermark.

## Exactly-once?
- This demo uses practical reliability: idempotent-ish producer and sink dedup (ReplacingMergeTree).
- For strict end-to-end exactly-once, Flink checkpointing + transactional sinks would be next.
