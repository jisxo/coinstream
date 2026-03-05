# Stream Processor Checkpointing Strategy

## Goals
- Periodically capture the processor state (`state[symbol][window]`) together with the latest Kafka offsets so the job can resume without reprocessing the same window data or losing late arrivals.
- Provide a narrative explaining how this mirrors Flink checkpointing for interview discussions.

## Implementation Notes
1. Every `checkpoint_interval_ms` (suggested 10-30s) dump the following to a durable store (ClickHouse metadata table, Redis, or simple local file):
   * The Kafka offset for each partition that the processor has successfully consumed.
   * The current in-memory window state, keyed by `(symbol, window_start)` plus any dedup cache entries.
2. On restart, read the latest checkpoint snapshot, seek the Kafka consumer back to the recorded offsets, rebuild the `state` dictionary and TTL cache from the persisted values, then continue processing only new records.
3. Track the durability source (file, ClickHouse helper table, or Redis key) so the checkpoint store can be rotated/trimmed without affecting the running job.

## Interview Talking Points
- "Flink has built-in checkpointing, but in this repo we mimic the same guarantees by snapshotting `state[symbol][window]` and Kafka offsets to durable storage, then replaying or resuming from that snapshot if the processor restarts."
- "Dedup key `(symbol, agg_trade_id)` and idempotent ClickHouse inserts complement the checkpoint snapshot, letting us avoid double counting after recovery."
